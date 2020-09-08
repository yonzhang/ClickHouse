#include <Resharding/ReshardingUtils.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Dictionaries/ComplexKeyHashedDictionary.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
#include <common/logger_useful.h>

namespace DB
{

const std::string ReshardingUtils::_SHARDING_VERSION_DICTIONARY = "default.sharding_version_dict";

const std::string ReshardingUtils::_MONSTOR_CONSISTENT_HASH_DICTIONARY = "default.monstor_consistent_hash_dict";

std::optional<std::string> ReshardingUtils::findActiveShardingVersionIfExists(const ExternalDictionariesLoader & dictionaries_loader, const std::string& db_table_name){
    std::shared_ptr<const IDictionaryBase> partition_ver_dict;
    try{
        partition_ver_dict = dictionaries_loader.getDictionary(_SHARDING_VERSION_DICTIONARY);
    }catch(const DB::Exception& ex){
        LOG_DEBUG(&Logger::get("ReshardingUtils"), ex.what());
        return std::nullopt;
    }

    const IDictionaryBase * dict_ptr = partition_ver_dict.get();
    const auto dict = typeid_cast<const ComplexKeyHashedDictionary *>(dict_ptr);
    if (!dict)
        return std::nullopt;

    Columns key_columns;
    DataTypes key_types;

    // column 'table'
    auto key_tablename = ColumnString::create();
    key_tablename->insert(db_table_name);
    ColumnString::Ptr immutable_ptr_key_tablename = std::move(key_tablename);
    key_columns.push_back(immutable_ptr_key_tablename);
    key_types.push_back(std::make_shared<DataTypeString>());

    // column 'date'
    auto key_date = ColumnString::create();
    key_date->insert("00000000");
    ColumnString::Ptr immutable_ptr_key_date = std::move(key_date);
    key_columns.push_back(immutable_ptr_key_date);
    key_types.push_back(std::make_shared<DataTypeString>());

    // column 'range_id'
    auto key_rangeid = ColumnUInt32::create();
    key_rangeid->insert(0);
    ColumnUInt32::Ptr immutable_ptr_key_rangeid = std::move(key_rangeid);
    key_columns.push_back(immutable_ptr_key_rangeid);
    key_types.push_back(std::make_shared<DataTypeUInt32>());

    // column 'active_ver'
    auto out = ColumnString::create();
    String attr_name = "active_ver";    
    dict->getString(attr_name, key_columns, key_types, out.get());
    std::string active_ver = out->getDataAt(0).toString();

    if(active_ver.empty()){
        LOG_WARNING(&Logger::get("ReshardingUtils"), "active _sharding_ver not found for {table: " << db_table_name << ", date: 00000000, range_id: 0}");
        return std::nullopt;
    }

    return std::optional<std::string>(active_ver);
}

std::optional<UInt32> ReshardingUtils::findShardIfExists(const ExternalDictionariesLoader & dictionaries_loader, const std::string& table, UInt32 date, UInt32 rangeId, const std::string& activeVerColumn){
    auto getDebugContext = [&](){
        std::ostringstream oss;
        oss << "from column: " << activeVerColumn <<  ", for {table: " << table << ", date: " << date << ", rangeId: " << rangeId << "}";
        return oss.str();
    };

    std::shared_ptr<const IDictionaryBase> partition_ver_dict;
    try{
        partition_ver_dict = dictionaries_loader.getDictionary(_SHARDING_VERSION_DICTIONARY);
    }catch(const DB::Exception& ex){
        LOG_DEBUG(&Logger::get("ReshardingUtils"), ex.what() << ", " << getDebugContext());
        return std::nullopt;
    }

    const IDictionaryBase * dict_ptr = partition_ver_dict.get();
    const auto dict = typeid_cast<const ComplexKeyHashedDictionary *>(dict_ptr);
    if (!dict){
        LOG_DEBUG(&Logger::get("ReshardingUtils"), "ComplexKeyHashedDictionary not found: " << getDebugContext());
        return std::nullopt;
    }

    Columns key_columns;
    DataTypes key_types;

    // column 'table'
    auto key_tablename = ColumnString::create();
    key_tablename->insert(table);
    ColumnString::Ptr immutable_ptr_key_tablename = std::move(key_tablename);
    key_columns.push_back(immutable_ptr_key_tablename);
    key_types.push_back(std::make_shared<DataTypeString>());

    // column 'date'
    auto key_date = ColumnString::create();
    key_date->insert(std::to_string(date));
    ColumnString::Ptr immutable_ptr_key_date = std::move(key_date);
    key_columns.push_back(immutable_ptr_key_date);
    key_types.push_back(std::make_shared<DataTypeString>());

    // column 'range_id'
    auto key_rangeid = ColumnUInt32::create();
    key_rangeid->insert(rangeId);
    ColumnUInt32::Ptr immutable_ptr_key_rangeid = std::move(key_rangeid);
    key_columns.push_back(immutable_ptr_key_rangeid);
    key_types.push_back(std::make_shared<DataTypeUInt32>());

    // column 'A' - 'F' to get shard id
    PaddedPODArray<UInt32> out(1);
    dict->getUInt32(activeVerColumn, key_columns, key_types, out);
    UInt32 shardId = out.front();

    // shardId is number starting from 1 and 0 is used for non-existence of the specified entry.
    return shardId == 0 ? std::nullopt : std::optional<UInt32> {shardId};
}

std::optional<UInt32> ReshardingUtils::findShardByMonstorConsistentHash(const ExternalDictionariesLoader & dictionaries_loader, const std::string& keyspace, const std::string& encoded){
    std::shared_ptr<const IDictionaryBase> monstor_consistent_hash_dict;
    try{
        monstor_consistent_hash_dict = dictionaries_loader.getDictionary(_MONSTOR_CONSISTENT_HASH_DICTIONARY);
    }catch(const DB::Exception& ex){
        LOG_DEBUG(&Logger::get("ReshardingUtils"), "Fail opening monstor_consistent_hash_dictionary: " << ex.what() << " for keyspace: " << keyspace);
        return std::nullopt;
    }

    const IDictionaryBase * dict_ptr = monstor_consistent_hash_dict.get();
    const auto dict = typeid_cast<const ComplexKeyHashedDictionary *>(dict_ptr);
    if (!dict){
        LOG_DEBUG(&Logger::get("ReshardingUtils"), "monstor_consistent_hash_dictionary is not ComplexKeyHashedDictionary for keyspace: " << keyspace);
        return std::nullopt;
    }

    Columns key_columns;
    DataTypes key_types;

    // column 'keyspace'
    auto key_keyspace = ColumnString::create();
    key_keyspace->insert(keyspace);
    ColumnString::Ptr immutable_ptr_key_keyspace = std::move(key_keyspace);
    key_columns.push_back(immutable_ptr_key_keyspace);
    key_types.push_back(std::make_shared<DataTypeString>());

    // column data_hash_rings
    auto out = ColumnString::create();
    dict->getString("data_hash_rings", key_columns, key_types, out.get());
    std::string data_hash_rings = out->getDataAt(0).toString();
    
    // TODO calculate shardId based on encoded and data_hash_rings.
    // here it is a just an illustration of how to create hash ring data structure and then binary search it
    // eventually it should be a cached data structure in DT so don't need create hash ring data structure each time
    // when query comes
    // the format for data hash rings is like:
    // -9223372036854775809:0:0,0:9223372036854775808:1"
    char value = *(encoded.c_str());
    if(value % 2 == 0)
        return std::optional<UInt32> {1};
    else
        return std::optional<UInt32> {2};
}

}
