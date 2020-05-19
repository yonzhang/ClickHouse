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
#include <common/Types.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}    

/**
 * This consistent hashing algorithm has 2 benefits in case of resharding
 *  - moving less keys
 *  - copy Clickhouse partition without affecting live traffic
 * Sharding expression is (f_date, f1, f2, ...) where f_date a Date type column, f1, f2, ... are columns with any type
 * Partition expression is same as sharding expression
 * 
 * Algorithm to choose shard
 * 1) Build in-memory map from (f_date, hash_range) to shard id, where hash_range is hash of concatenated of columns f1, f2, ...
 *     This map could be rooted in a dictionary or in a file system which is periodically populated
 * 2) In query time, calculate f_date and hash_range and lookup the map to get target shard
 **/        
class NuColumnarConsistentHash : public IFunction
{
public:
    static constexpr auto name = "nuColumnarConsistentHash";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<NuColumnarConsistentHash>();
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & ) const override 
    {
        return std::make_shared<DataTypeNumber<UInt32>>();
    }

    
    /**
     * The expected arguments must be (f_date, f1, f2, ...)
     **/
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t ) override
    {
        const IDataType * type0 = block.getByPosition(arguments[0]).type.get();
        WhichDataType which(type0);
        LOG_DEBUG(log, "column 0" << ": type=" << getTypeName(which.idx));
        
        // assert type for argument 0 is Date
        if (!which.isUInt32()){ // first argument must be UInt32 type
            LOG_WARNING(log, "NuColumnarConsistentHash function's first argument must be UInt32");
            throw Exception("NuColumnarConsistentHash function's first argument is not UInt32", ErrorCodes::ILLEGAL_COLUMN);
        }
        const IColumn * c0 = block.getByPosition(arguments[0]).column.get(); // Date type column
        const auto * fdate = checkAndGetColumn<ColumnUInt32>(c0); 
        LOG_DEBUG(log, "column 0" << ": name=" << c0->getName() << ", type=Date" << ", value=" << fdate->getElement(0));

        UInt32 shard = lookupShard(fdate->getElement(0), block, arguments);
        
        auto c_res = ColumnUInt32::create();
        auto & data = c_res->getData();
        data.push_back(shard);
        block.getByPosition(result).column = std::move(c_res);
    }
private:
    // return map of f_date to map of bucket ending hash value to shard id
    // f_date is the days from epoch time, and assume there are 2 partitions
    // boost::hash_combine outputs hash value with type std::size_t, which is unsigned integer.
    // min_hash_value=0, max_hash_value=-1, middle_hash_value=max_hash_value/2
    std::size_t max = -1;
    std::size_t middle = max/2;
    std::unordered_map<UInt16, std::map<std::size_t, UInt32>> getShardingMap(){
        LOG_WARNING(log, "max: " << max << ", middle: " << middle);
        return 
        {
            {20200508,    // daysSinceEpoch, 2020-05-08
               {
                   {middle, 0},  // shard 0 for hash value less than 0 and middle
                   {max, 1}      // shard 1 for hash value between middle+1 and max
               }
            },
            {20200509,    // daysSinceEpoch, , 2020-05-09
               {
                   {middle, 0}, // shard 0 for hash value less than 0 and middle
                   {max, 1}     // shard 1 for hash value between middle+1 and max
               }
            },
            {20200510,    // daysSinceEpoch, , 2020-05-10
               {
                   {middle, 0}, // shard 0 for hash value less than 0 and middle
                   {max, 1}     // shard 1 for hash value between middle+1 and max
               }
            },
            {20200511,    // daysSinceEpoch, , 2020-05-11
               {
                   {middle, 0}, // shard 0 for hash value less than 0 and middle
                   {max, 1}     // shard 1 for hash value between middle+1 and max
               }
            },
        };
    }

    UInt32 lookupShard(UInt16 daysSinceEpoch, Block & block, const ColumnNumbers & arguments){
        auto map = getShardingMap();
        auto foundDays = map.find(daysSinceEpoch);
        if(foundDays == map.end()){
            LOG_WARNING(log, "Date not found: " << daysSinceEpoch);
            throw Exception("NuColumnarConsistentHash Date not found in map: " + std::to_string(daysSinceEpoch), ErrorCodes::ILLEGAL_COLUMN);
        }
        std::size_t hash = concatenatedHash(block, arguments);
        std::map<std::size_t, UInt32>& hashRange2ShardMap = foundDays->second;
        auto foundShard = hashRange2ShardMap.lower_bound(hash);
        if(foundShard == hashRange2ShardMap.end()){
            LOG_WARNING(log, "Shard not found for hash: " << hash);
            throw Exception("NuColumnarConsistentHash shard not found for hash: " + std::to_string(hash), ErrorCodes::ILLEGAL_COLUMN);
        }
        return foundShard->second;
    }

    // iterate from the 2nd argument and doing hash
    std::size_t concatenatedHash(Block & block, const ColumnNumbers & arguments){
        std::size_t seed = 0;
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
                    boost::hash_combine(seed, static_cast<unsigned char>(v_uint8));
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_uint8 << ", hash=" << seed);
                    break;
                }
                case TypeIndex::Int64:
                {
                    const Int64& v_int64 = checkAndGetColumn<ColumnInt64>(c)->getElement(0);
                    boost::hash_combine(seed, v_int64);
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_int64 << ", hash=" << seed);
                    break;
                }
                case TypeIndex::String:
                {
                    const ColumnString* val = checkAndGetColumn<ColumnString>(c);
                    std::string v_str = val->getDataAt(1).toString();
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_str << ", hash=" << seed);
                    boost::hash_combine(seed, v_str);
                    break;
                }
                default:
                {
                    LOG_DEBUG(log, "Skipping column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx));
                    break;
                }
            }
        }
        return seed;
    }

private:
    Logger * log = &Logger::get("NuColumnarConsistentHash");
};



/**
 *  hashCombine input arguments and return an integer to indicate
 *  This function is used as below
 *   nuColumnarHashRange(f1, f2, ...)
 * it uses boost hashCombine to compute a hash value with std::size_t type,
 *   then lookup bucket id by the hash value. The lookup is on a fixed number of
 *   array whose element is sorted hash range.
 * Assume max value for std::size_t is N (2^64-1), obviously 0 is min value, and we need 16 buckets,
 * so each bucket has (N+1)/16 numbers, so the first bucket is [0, (N+1)/16-1], i.e. [0, (N-15)/16] 
 *  Example: [(N-15)/16, (N-15)/16*2+1, (N-15)/16*3+2, ... , (N-15)/16*15+14, N]
 **/ 
class NuColumnarHashRange : public IFunction
{
public:
    static constexpr auto name = "nuColumnarHashRange";

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<NuColumnarHashRange>();
    }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block &) override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & ) const override 
    {
        return std::make_shared<DataTypeNumber<UInt32>>();
    }

     /**
     * The expected arguments must be (f_date, f1, f2, ...)
     * 
     * return bucket from 1 to 16
     **/
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t ) override
    {
        std::size_t combinedHash = concatenatedHash(block, arguments);
        // print hash ranges
        for(auto it = hash_ranges.begin(); it != hash_ranges.end(); ++it){
            LOG_DEBUG(log, "hash range: " << *it);
        }
        auto found = std::lower_bound(hash_ranges.begin(), hash_ranges.end(), combinedHash);
        std::size_t bucketIdx = found-hash_ranges.begin();
        LOG_DEBUG(log, "Combined hash= " << combinedHash  << ", bucket index=" << bucketIdx);

        auto c_res = ColumnUInt32::create();
        auto & data = c_res->getData();
        data.push_back(static_cast<UInt32>(bucketIdx));
        block.getByPosition(result).column = std::move(c_res);
    }

    // iterate from the 2nd argument and doing hash
    std::size_t concatenatedHash(Block & block, const ColumnNumbers & arguments){
        std::size_t seed = 0;
        for(std::vector<size_t>::size_type i=0; i<arguments.size(); i++){
            const IColumn * c = block.getByPosition(arguments[i]).column.get();
            // check type for column to do hashing
            const IDataType * type = block.getByPosition(arguments[i]).type.get();
            WhichDataType which(type);
            switch (which.idx)
            {
                case TypeIndex::UInt8:
                {
                    const UInt8& v_uint8 = checkAndGetColumn<ColumnUInt8>(c)->getElement(0);
                    boost::hash_combine(seed, static_cast<unsigned char>(v_uint8));
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_uint8 << ", hash=" << seed);
                    break;
                }
                case TypeIndex::Int64:
                {
                    const Int64& v_int64 = checkAndGetColumn<ColumnInt64>(c)->getElement(0);
                    boost::hash_combine(seed, v_int64);
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_int64 << ", hash=" << seed);
                    break;
                }
                case TypeIndex::String:
                {
                    const ColumnString* val = checkAndGetColumn<ColumnString>(c);
                    std::string v_str = val->getDataAt(1).toString();
                    LOG_DEBUG(log, "Column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx) << ", value=" << v_str << ", hash=" << seed);
                    boost::hash_combine(seed, v_str);
                    break;
                }
                default:
                {
                    LOG_DEBUG(log, "Skipping column " << i  << ": name=" << c->getName() << ", type=" << getTypeName(which.idx));
                    break;
                }
            }
        }
        return seed;
    }

private:
    Logger * log = &Logger::get("NuColumnarHashRange");
    static std::size_t unit_range;
    // 16 buckets for search
    static std::vector<std::size_t> hash_ranges;
};

std::size_t NuColumnarHashRange::unit_range = (static_cast<std::size_t>(-1)-15)/16;

std::vector<std::size_t> NuColumnarHashRange::hash_ranges = 
{
    NuColumnarHashRange::unit_range, 
    NuColumnarHashRange::unit_range*2+1, 
    NuColumnarHashRange::unit_range*3+2, 
    NuColumnarHashRange::unit_range*4+3, 
    NuColumnarHashRange::unit_range*5+4, 
    NuColumnarHashRange::unit_range*6+5,
    NuColumnarHashRange::unit_range*7+6,
    NuColumnarHashRange::unit_range*8+7, 
    NuColumnarHashRange::unit_range*9+8, 
    NuColumnarHashRange::unit_range*10+9, 
    NuColumnarHashRange::unit_range*11+10, 
    NuColumnarHashRange::unit_range*12+11, 
    NuColumnarHashRange::unit_range*13+12, 
    NuColumnarHashRange::unit_range*14+13, 
    NuColumnarHashRange::unit_range*15+14, 
    static_cast<std::size_t>(-1)
};

void registerFunctionNuColumnarConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<NuColumnarConsistentHash>();
    factory.registerFunction<NuColumnarHashRange>();
}
}
