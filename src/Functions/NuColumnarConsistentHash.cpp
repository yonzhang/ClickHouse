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
#include <DataTypes/DataTypeArray.h>
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
 * @resharding-support
 * 
 * find shard from dictionary by complete keys
 *  
   CREATE DICTIONARY sharding_version_dict
    (
        table String,
        date String,
        bucket UInt32,
        active_ver String,
        A UInt32,
        B UInt32,
        S String,
        T String
    )
    PRIMARY KEY table, date, bucket
    SOURCE(FILE(path './user_files/sharding_version_dict.csv' format 'CSV'))
    LAYOUT(COMPLEX_KEY_HASHED())
    LIFETIME(10)
    ;
 * 
 * Algorithm to find shard
 * 1) read current active version column from the entry {$table_name, '00000000', 0}
 * 2) read shard id from active column from the entry {$table_name, toYYYYMMDD($date_field), $bucket}
 **/        
class GetShardFromDict : public IFunction
{
public:
    static constexpr auto name = "getShardFromDict";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<GetShardFromDict>(context.getExternalDictionariesLoader());
    }

    explicit GetShardFromDict(const ExternalDictionariesLoader & dictionaries_loader_) : dictionaries_loader(dictionaries_loader_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override 
    {
        if (arguments.size() != 3)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        return std::make_shared<DataTypeNumber<UInt32>>();
    }

    
    /**
     * The expected arguments must be (table_name, f_date, hash_range_id)
     * hash_range_id is from 1 to 16
     **/
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count ) const override
    {
        LOG_DEBUG(log, "checking all arguments for NuColumnConsistentHash");
        // argument 'table'
        const auto& table_arg = arguments[0];
        const IDataType * table_arg_type = block.getByPosition(table_arg).type.get();
        // assert type for argument 'table' is String
        if(table_arg_type->getTypeId() != TypeIndex::String){
            LOG_ERROR(log, "NuColumnarConsistentHash function's first argument must be 'table' with 'String' type");
            throw Exception("NuColumnarConsistentHash function's first argument 'table' is not 'String' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * table_col = block.getByPosition(table_arg).column.get();
        const ColumnString * table_col_string = checkAndGetColumn<ColumnString>(table_col);
        const ColumnConst * table_col_const_string = checkAndGetColumnConst<ColumnString>(table_col);
        std::string table = table_col_string ? table_col_string->getDataAt(0).toString() : table_col_const_string->getDataAt(0).toString();
        
        LOG_DEBUG(log, "column 0: name={}, type=String, value={}", table_col->getName(), table);

        // argument 'date'
        const auto& date_arg = arguments[1];
        const IDataType * date_arg_type = block.getByPosition(date_arg).type.get();        
        // assert type for argument 'date' is Date
        if (date_arg_type->getTypeId() != TypeIndex::UInt32){
            LOG_WARNING(log, "GetShardFromDict function's second argument must be 'date' with 'UInt32' type");
            throw Exception("GetShardFromDict function's second argument 'date' is not 'UInt32' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * date_col = block.getByPosition(date_arg).column.get();
        const auto * date_col_val = checkAndGetColumn<ColumnUInt32>(date_col); 
        LOG_DEBUG(log, "column 1: name={}, type=Date, value={}", date_col->getName(), date_col_val->getElement(0));

        // argument "range_id"
        const auto& rangeid_arg = arguments[2];
        const IDataType * rangeid_arg_type = block.getByPosition(rangeid_arg).type.get();        
        // assert type for argument 'range_id' is UInt32
        if (rangeid_arg_type->getTypeId() != TypeIndex::UInt32){
            LOG_WARNING(log, "GetShardFromDict function's third argument must be 'range_id' with 'UInt32' type");
            throw Exception("GetShardFromDict function's sethirdcond argument 'range_id' is not 'UInt32' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * rangeid_col = block.getByPosition(rangeid_arg).column.get();
        const auto * rangeid_col_val = checkAndGetColumn<ColumnUInt32>(rangeid_col); 
        LOG_DEBUG(log, "column 2: name={}, type=UInt32, value={}", rangeid_col->getName(), rangeid_col_val->getElement(0));

        // pushing an array for testing only
        if(input_rows_count > 0){
            UInt32 shard = lookupShard(table, date_col_val->getElement(0), rangeid_col_val->getElement(0));
            LOG_DEBUG(log, "found shard: shard={}, table={}, date={} rangeid={}", shard, table, date_col_val->getElement(0), rangeid_col_val->getElement(0));
            auto c_res = ColumnUInt32::create();
            auto & data = c_res->getData();
            //shard number starts from 1, but shard index in cluster definition starts from 0, so shard-1 is the index
            data.push_back(shard-1);
            block.getByPosition(result).column = std::move(c_res);
            LOG_DEBUG(log, "GetShardFromDict run with input_rows_count={}, result={}", input_rows_count, result);
        }else{
            auto c_res = ColumnUInt32::create();
            block.getByPosition(result).column = std::move(c_res);
            LOG_DEBUG(log, "GetShardFromDict dry-run with input_rows_count=0, result={}", result);
        }
    }
private:
    UInt32 lookupShard(const std::string& table, UInt32 date, UInt32 rangeId) const {
        std::optional<std::string> activeVerColumn = ReshardingUtils::findActiveShardingVersionIfExists(dictionaries_loader, table, "00000000");

        if(!activeVerColumn){
            std::ostringstream err;
            err << "active version column not found for NuColumnarConsistentHash for {table: " << table << ", date: " << date << 
                ", range_id: " << rangeId;
            throw Exception(err.str(), ErrorCodes::ILLEGAL_COLUMN);
        }

        std::optional<UInt32> shard = ReshardingUtils::findShardIfExists(dictionaries_loader, table, date, rangeId, *activeVerColumn);

         if(!shard){
            std::ostringstream err;
            err << "shard not found for NuColumnarConsistentHash for table: " << table << ", date: " << date <<
                ", range_id: " << rangeId << ", activeVerColumn: " << *activeVerColumn;
            throw Exception(err.str(), ErrorCodes::ILLEGAL_COLUMN);
        }

        // put activeVerColumn to query context for later use in executeQuery
        CurrentThread::setActiveVerColumn(*activeVerColumn);

        return (*shard);
    }

    const ExternalDictionariesLoader & dictionaries_loader;
    Poco::Logger * log = &Poco::Logger::get("GetShardFromDict");
};




/**
 *  GetShardsFromDictByBucket("default.ads", modulo(sellerId,10))
 **
 **/ 
class GetShardsFromDictByBucket : public IFunction
{
public:
    static constexpr auto name = "getShardsFromDictByBucket";

    static FunctionPtr create(const Context & context)
    {
         return std::make_shared<GetShardsFromDictByBucket>(context.getExternalDictionariesLoader());
    }

    explicit GetShardsFromDictByBucket(const ExternalDictionariesLoader & dictionaries_loader_) : dictionaries_loader(dictionaries_loader_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & ) const override 
    {
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNumber<UInt32>>());
    }

     /**
     * test to return multiple values
     * 
     **/
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        LOG_DEBUG(log, "checking all arguments for GetShardsByBucket");

        if(input_rows_count == 0){
            auto nested = ColumnUInt32::create();
            auto c_res = ColumnArray::create(std::move(nested));
            block.getByPosition(result).column = std::move(c_res);
            LOG_DEBUG(log, "dry-run with input_rows_count=0, result={}", result);
            return;
        }

        // argument 'table'
        const auto& table_arg = arguments[0];
        const IDataType * table_arg_type = block.getByPosition(table_arg).type.get();
        // assert type for argument 'table' is String
        if(table_arg_type->getTypeId() != TypeIndex::String){
            LOG_ERROR(log, "GetShardsByBucket function's first argument must be 'table' with 'String' type");
            throw Exception("GetShardsByBucket function's first argument 'table' is not 'String' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * table_col = block.getByPosition(table_arg).column.get();
        const ColumnString * table_col_string = checkAndGetColumn<ColumnString>(table_col);
        const ColumnConst * table_col_const_string = checkAndGetColumnConst<ColumnString>(table_col);
        std::string table = table_col_string ? table_col_string->getDataAt(0).toString() : table_col_const_string->getDataAt(0).toString();
        LOG_DEBUG(log, "column 0: name={}, type=String, value={}", table_col->getName(), table);

         // argument 'bucket'
        const auto& bucket_arg = arguments[1];
        const IDataType * bucket_arg_type = block.getByPosition(bucket_arg).type.get();        
        // assert type for argument 'bucket' is UInt32
        if (bucket_arg_type->getTypeId() != TypeIndex::UInt32){
            LOG_WARNING(log, "GetShardsByBucket function's second argument must be 'bucket' with 'UInt32' type");
            throw Exception("GetShardsByBucket function's second argument 'bucket' is not 'UInt32' type", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * bucket_col = block.getByPosition(bucket_arg).column.get();
        const auto * bucket_col_val = checkAndGetColumn<ColumnUInt32>(bucket_col); 
        UInt32 bucket = bucket_col_val->getElement(0);
        LOG_DEBUG(log, "column 1: name={}, type=UInt32, value={}", bucket_col->getName(), bucket);

        std::optional<std::string> activeVerColumn = ReshardingUtils::findActiveShardingVersionIfExists(dictionaries_loader, table, "99999999");

        if(!activeVerColumn){
            std::ostringstream err;
            err << "active version column not found for GetShardsByBucket for {table: " << table << ", date: 99999999" << 
                ", bucket: " << bucket_col_val->getElement(0);
            throw Exception(err.str(), ErrorCodes::ILLEGAL_COLUMN);
        }

        std::optional<std::string> shardList = ReshardingUtils::findShardListIfExists(dictionaries_loader, table, "99999999", bucket, *activeVerColumn);

         if(!shardList){
            std::ostringstream err;
            err << "shard not found for GetShardsFromDictByBucket for table: " << table << ", date: 99999999" << 
                ", bucket: " << bucket << ", activeVerColumn: " << *activeVerColumn;
            throw Exception(err.str(), ErrorCodes::ILLEGAL_COLUMN);
        }

        // split shard list by seperator comma
        std::string::size_type prev_pos = 0, pos = 0;
        char seperator = ',';

        auto nested = ColumnUInt32::create();
        int count = 0;
        while((pos = (*shardList).find(seperator, pos)) != std::string::npos)
        {
            std::string shard((*shardList).substr(prev_pos, pos-prev_pos));
            int shardId = std::stoi(shard); 
            nested->getData().push_back(shardId-1);
            prev_pos = ++pos;
            count++;
        }
        std::string lastShard((*shardList).substr(prev_pos, pos-prev_pos));
        nested->getData().push_back(std::stoi(lastShard)-1);
        count++;
        LOG_DEBUG(log, "shard count={}, shard list={}", count, *shardList);
        auto off = ColumnUInt64::create();
        auto & off_data = off->getData();
        off_data.push_back(count);
        auto c_res = ColumnArray::create(std::move(nested), std::move(off));
        block.getByPosition(result).column = std::move(c_res);
    }

private:
    const ExternalDictionariesLoader & dictionaries_loader;
    Poco::Logger * log = &Poco::Logger::get("GetShardsFromDictByBucket");
};

void registerFunctionNuColumnarConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<GetShardFromDict>();
    factory.registerFunction<GetShardsFromDictByBucket>();
}
}
