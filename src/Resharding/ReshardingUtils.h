#pragma once

#include <Interpreters/Context.h>

namespace DB
{
/**
 *  Some utility methods to find sharding information 
 **/ 
class ReshardingUtils{
public:
    static std::optional<std::string> findActiveShardingMapVersionIfExists(const Context & context, const ASTPtr & query_ast);
};

}