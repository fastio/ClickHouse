#pragma once

#include <Core/UUID.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/ReflectionReadHint.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB
{
class ActionsDAG;
}

namespace DB::QueryPlanOptimizations
{

/// Generic Reflection `ReadHint` rewriter. Detects the TopK vector-search plan
/// shape, dispatches every `IReflectionMatcher` candidate registered on the
/// source through `matchReadHint`/`realizeReadHint`, then attaches the winner's
/// per-part hits to the covered `ReadFromMergeTree`. Body lives in
/// `optimizeReflectionReadHint.cpp`.
size_t tryUseReflectionReadHint(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// If `hints` says `part_uuid` is covered, write its hits (or an empty result
/// for zero-hit covered parts) into `read_hints.ann_index_search_results`.
/// `chassert`-asserts the destination is empty before the write so that a
/// second-pass optimizer attempting to re-attach hints fails fast in debug
/// builds.
void attachANNIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const ANNIndexHints & hints);

/// Apply `attachANNIndexHintForPart` across every part in `parts`.
void applyANNIndexHints(RangesInDataParts & parts, const ANNIndexHints & hints);

/// Returns true when an `ExpressionStep` output other than the `ORDER BY`
/// distance result still depends on the search column and the rewrite must
/// keep reading that physical column alongside the virtual `_distance` column.
bool materializedIndexExpressionNeedsSearchColumn(
    const ActionsDAG & expression,
    const String & sort_column,
    const String & search_column);

}
