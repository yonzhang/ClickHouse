#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <IO/WriteHelpers.h>
#include <ext/map.h>
#include <ext/range.h>

#include "formatString.h"
#include <common/logger_useful.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include "boost/functional/hash.hpp"
#include <Columns/ColumnsNumber.h>
#include <Core/Types.h>
#include <common/types.h>
#include <Columns/ColumnConst.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/ComplexKeyHashedDictionary.h>
#include <Interpreters/Context.h>
#include <Resharding/ReshardingUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}    

/**
 * @monstorconsistenthash-support
 * 
 * monstor consistent hashing algorithm to choose shards based on data hash rings and sharding keys
 * 
 * Sharding expression is MonstorConsistentHash('<keyspace_name>', sharding_key_1, ...)
 **/        
class MonstorConsistentHash : public IFunction
{
public:
    static constexpr auto name = "MonstorConsistentHash";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<MonstorConsistentHash>(context.getExternalDictionariesLoader());
    }

    MonstorConsistentHash(const ExternalDictionariesLoader & dictionaries_loader_) : dictionaries_loader(dictionaries_loader_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override 
    {
        return std::make_shared<DataTypeNumber<UInt32>>();
    }

    
    /**
     * The expected arguments must be (keyspace_name, shard_key_1, shard_key_2, ...)
     **/
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t ) override
    {
        LOG_DEBUG(log, "checking all arguments for MonstorConsistentHash");
        // argument 'keyspace_name'
        auto& keyspace_arg = arguments[0];
        const IDataType * keyspace_arg_type = block.getByPosition(keyspace_arg).type.get();
        // assert type for argument 'table' is String
        if(keyspace_arg_type->getTypeId() != TypeIndex::String){
            LOG_ERROR(log, "MonstorConsistentHash function's first argument must be 'keyspace' with 'String' type");
            throw Exception("MonstorConsistentHash function's first argument 'keyspace' is not 'String' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * keyspace_col = block.getByPosition(keyspace_arg).column.get();
        const ColumnString * keyspace_col_string = checkAndGetColumn<ColumnString>(keyspace_col);
        const ColumnConst * keyspace_col_const_string = checkAndGetColumnConst<ColumnString>(keyspace_col);
        std::string keyspace = keyspace_col_string ? keyspace_col_string->getDataAt(0).toString() : keyspace_col_const_string->getDataAt(0).toString();
        
        LOG_DEBUG(log, "column 0: name=" << keyspace_col->getName() << ", type=String" << ", value=" << keyspace);

        // encode all sharding keys to a string
        std::string encoded = encodeShardingKeys(block, arguments);

        UInt32 shard = lookupShard(keyspace, encoded);
        
        LOG_DEBUG(log, "Selected shard: " << shard << ", for keyspace: " << keyspace << ", encoded:" << encoded);

        auto c_res = ColumnUInt32::create();
        auto & data = c_res->getData();
        data.push_back(shard);
        block.getByPosition(result).column = std::move(c_res);
    }
private:
    // TODO support all possible data types
    // order preserving encoding
    std::string encodeShardingKeys(Block & block, const ColumnNumbers & arguments){
        std::string encoded = "";
        for(std::vector<size_t>::size_type i=1; i<arguments.size(); i++){
            const IColumn * c = block.getByPosition(arguments[i]).column.get();
            // check type for column to do hashing
            const IDataType * type = block.getByPosition(arguments[i]).type.get();
            WhichDataType which(type);
            switch (which.idx)
            {
                case TypeIndex::UInt8:
                {
                    const UInt8& v_uint8 = checkAndGetColumn<ColumnUInt8>(c)->getElement(0);
                    // TODO encode a uint8
                    encoded += std::to_string(v_uint8);
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_uint8 << ", encoded=" << encoded);
                    break;
                }
                case TypeIndex::Int8:
                {
                    const Int8& v_int8 = checkAndGetColumn<ColumnInt8>(c)->getElement(0);
                    // TODO encode a int8
                    encoded += std::to_string(v_int8);
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_int8 << ", encoded=" << encoded);
                    break;
                }
                case TypeIndex::Int64:
                {
                    const Int64& v_int64 = checkAndGetColumn<ColumnInt64>(c)->getElement(0);
                    // TODO encode a int64
                    encoded += std::to_string(v_int64);
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_int64 << ", encoded=" << encoded);
                    break;
                }
                case TypeIndex::String:
                {
                    const ColumnString* val = checkAndGetColumn<ColumnString>(c);
                    std::string v_str = val->getDataAt(0).toString();
                    // TODO encode a string
                    encoded += v_str;
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_str << ", encoded=" << encoded);
                    break;
                }
                default:
                {
                    LOG_DEBUG(log, "Skipping column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx));
                    break;
                }
            }
        }
        return encoded;
    }

    UInt32 lookupShard(const std::string& keyspace, const std::string& encoded){
        std::optional<UInt32> shard = ReshardingUtils::findShardByMonstorConsistentHash(dictionaries_loader, keyspace, encoded);

         if(!shard){
            std::ostringstream err;
            err << "shard not found for MonstorConsistentHash for {keyspace: " << keyspace << ", encoded: " << encoded << "}";
            throw Exception(err.str(), ErrorCodes::ILLEGAL_COLUMN);
        }

        // shard index starting from 0, used for mod by number of shards
        return (*shard - 1);
    }

private:
    const ExternalDictionariesLoader & dictionaries_loader;
    Logger * log = &Logger::get("MonstorConsistentHash");
};


void registerFunctionMonstorConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<MonstorConsistentHash>();
}
}
