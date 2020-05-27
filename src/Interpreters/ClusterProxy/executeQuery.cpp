#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/IInterpreter.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Parsers/queryToString.h>
#include <Interpreters/ProcessList.h>
#include <Processors/Pipe.h>
#include <Dictionaries/ComplexKeyHashedDictionary.h>
#include <Storages/VirtualColumnUtils.h>
#include <Resharding/ReshardingUtils.h>

namespace DB
{

namespace ClusterProxy
{

Context removeUserRestrictionsFromSettings(const Context & context, const Settings & settings)
{
    Settings new_settings = settings;
    new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.max_execution_time);

    /// Does not matter on remote servers, because queries are sent under different user.
    new_settings.max_concurrent_queries_for_user = 0;
    new_settings.max_memory_usage_for_user = 0;

    /// Set as unchanged to avoid sending to remote server.
    new_settings.max_concurrent_queries_for_user.changed = false;
    new_settings.max_memory_usage_for_user.changed = false;

    if (settings.force_optimize_skip_unused_shards_no_nested)
    {
        new_settings.force_optimize_skip_unused_shards = 0;
        new_settings.force_optimize_skip_unused_shards.changed = false;
    }

    Context new_context(context);
    new_context.setSettings(new_settings);

    return new_context;
}

//@resharding-support
static std::optional<std::string> findActiveShardingMapVersionIfExists(const Context & context){
    /// check if activeVerCol exists in query_context
    if(context.hasQueryContext()){
        std::string activeVerCol = context.getQueryContext().getActiveVerColumn();
        return activeVerCol.empty() ? std::nullopt : std::optional<std::string>(activeVerCol);
    }else
        return std::nullopt;
}

Pipes executeQuery(
    IStreamFactory & stream_factory, const ClusterPtr & cluster,
    const ASTPtr & query_ast, const Context & context, const Settings & settings, const SelectQueryInfo & query_info)
{
    Pipes res;
    Context new_context = removeUserRestrictionsFromSettings(context, settings);

    ThrottlerPtr user_level_throttler;
    if (auto * process_list_element = context.getProcessListElement())
        user_level_throttler = process_list_element->getUserNetworkThrottler();

    /// Network bandwidth limit, if needed.
    ThrottlerPtr throttler;
    if (settings.max_network_bandwidth || settings.max_network_bytes)
    {
        throttler = std::make_shared<Throttler>(
                settings.max_network_bandwidth,
                settings.max_network_bytes,
                "Limit for bytes to send or receive over network exceeded.",
                user_level_throttler);
    }
    else
        throttler = user_level_throttler;

    //@resharding-support: rewrite query with active version for snapshot query if needed
    auto foundVer = findActiveShardingMapVersionIfExists(context);
    if(foundVer){
        LOG_DEBUG(&Logger::get("ClusterProxy::executeQuery"), "found active sharding version: " << *foundVer);
        VirtualColumnUtils::rewriteEntityInAst(query_ast, "_sharding_ver", *foundVer);
    }else{
        LOG_DEBUG(&Logger::get("ClusterProxy::executeQuery"), "not found active sharding version");
    }

    const std::string query = queryToString(query_ast);

    for (const auto & shard_info : cluster->getShardsInfo())
        stream_factory.createForShard(shard_info, query, query_ast, new_context, throttler, query_info, res);

    return res;
}

}

}
