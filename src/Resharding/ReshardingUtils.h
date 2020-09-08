#pragma once

#include <Interpreters/Context.h>

namespace DB
{
/**
 *  Some utility methods to find sharding information 
 **/ 
class ReshardingUtils{
public:
    static const std::string _SHARDING_VERSION_DICTIONARY;

    static const std::string _MONSTOR_CONSISTENT_HASH_DICTIONARY;

    /**
     * find active sharding version from sharding_version_dict dictionary, where a special entry is used for current
     *  active sharding version.
     *  table: $table, date: 00000000, rangeId: 0
     * 
     * @parameters db_table_name: full qualified table name
     * @return optional of active sharding version
     **/ 
    static std::optional<std::string> findActiveShardingVersionIfExists(const ExternalDictionariesLoader & dictionaries_loader, const std::string& db_table_name);

    /**
     * find shard from provided column activeVerColumn in sharding_version_dict dictionary, where entry matches provided table, date and rangeId
     **/ 
    static std::optional<UInt32> findShardIfExists(const ExternalDictionariesLoader & dictionaries_loader, const std::string& table, UInt32 date, UInt32 rangeId, const std::string& activeVerColumn);


    /**
     * find shard by keyspace and encoded key.
     * TODO building hash ring expensive is expensive, we might need cache the hash ring when dictionary is loaded
     * */
    static std::optional<UInt32> findShardByMonstorConsistentHash(const ExternalDictionariesLoader & dictionaries_loader, const std::string& keyspace, const std::string& encoded);
};

}
