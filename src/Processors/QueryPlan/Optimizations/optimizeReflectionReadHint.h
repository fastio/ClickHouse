#pragma once

#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/ReflectionReadHint.h>
#include <Processors/QueryPlan/QueryPlan.h>

namespace DB
{
class ActionsDAG;
}

namespace DB::QueryPlanOptimizations
{

/// Generic Reflection `ReadHint` rewriter. Detects the TopK vector-search plan
/// shape, dispatches every `IReflectionMatcher` candidate registered on the
/// source through `matchReadHint`/`prepareReadHint`, applies the winner's
/// structural rewrite during optimization and defers its engine search to
/// pipeline-build time. Body lives in `optimizeReflectionReadHint.cpp`.
size_t tryUseReflectionReadHint(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// Returns true when an `ExpressionStep` output other than the `ORDER BY`
/// distance result still depends on the search column and the rewrite must
/// keep reading that physical column alongside the virtual `_distance` column.
bool materializedIndexExpressionNeedsSearchColumn(
    const ActionsDAG & expression,
    const String & sort_column,
    const String & search_column);

}
