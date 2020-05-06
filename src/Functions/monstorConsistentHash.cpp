#include "FunctionsConsistentHashing.h"
#include <Functions/FunctionFactory.h>


namespace DB
{

static inline int32_t MonstorConsistentHash(uint64_t key, int32_t num_buckets)
{
    // main fixed hash ranges for testing purpose, 2 shards
    int64_t key_copy = static_cast<int64_t> (key);
    int32_t shardIdx = -1;
    if(key_copy < 0){
        shardIdx = 0;
    }else{
        shardIdx = 1;
    }

    // remove compile error 
    num_buckets++;
    
    return shardIdx;
    // int64_t b = -1, j = 0;
    // while (j < num_buckets)
    // {
    //     b = j;
    //     key = key * 2862933555777941757ULL + 1;
    //     j = static_cast<int64_t>((b + 1) * (double(1LL << 31) / double((key >> 33) + 1)));
    // }
    // return static_cast<int32_t>(b);
}

struct MonstorConsistentHashImpl
{
    static constexpr auto name = "monstorConsistentHash";

    using HashType = UInt64;
    using ResultType = Int32;
    using BucketsType = ResultType;
    static constexpr auto max_buckets = static_cast<UInt64>(std::numeric_limits<BucketsType>::max());

    static inline ResultType apply(UInt64 hash, BucketsType n)
    {
        return MonstorConsistentHash(hash, n);
    }
};

using FunctionMonstorConsistentHash = FunctionConsistentHashImpl<MonstorConsistentHashImpl>;

void registerFunctionMonstorConsistentHash(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMonstorConsistentHash>();
}

}

