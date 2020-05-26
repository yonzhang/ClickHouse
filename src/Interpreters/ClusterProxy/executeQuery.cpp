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
static std::optional<std::string> findActiveShardingMapVersionIfExists(const Context & context, const ASTPtr & query_ast){
    const ASTSelectQuery & select = query_ast->as<ASTSelectQuery &>();
    if (!select.tables())
        return std::nullopt;

    const auto & tables_in_select_query = select.tables()->as<ASTTablesInSelectQuery &>();
    if (tables_in_select_query.children.empty())
        return std::nullopt;

    const auto & tables_element = tables_in_select_query.children[0]->as<ASTTablesInSelectQueryElement &>();
    if (!tables_element.table_expression)
        return std::nullopt;

    const auto& table_expr = tables_element.table_expression->as<ASTTableExpression>();
    if(!table_expr->database_and_table_name)
        return std::nullopt;

    const auto& db_table = table_expr->database_and_table_name->as<ASTIdentifier>();

    return ReshardingUtils::findActiveShardingVersionIfExists(context, db_table->name );
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
    auto modified_query_ast = query_ast->clone();
    auto foundVer = findActiveShardingMapVersionIfExists(context, query_ast);
    if(foundVer){
        LOG_DEBUG(&Logger::get("ClusterProxy::executeQuery"), "found active sharding version: " << *foundVer);
        VirtualColumnUtils::rewriteEntityInAst(modified_query_ast, "_sharding_ver", *foundVer);
    }else{
        LOG_DEBUG(&Logger::get("ClusterProxy::executeQuery"), "not found active sharding version");
    }

    const std::string query = queryToString(modified_query_ast);

    for (const auto & shard_info : cluster->getShardsInfo())
        stream_factory.createForShard(shard_info, query, modified_query_ast, new_context, throttler, query_info, res);

    return res;
}

}

}
