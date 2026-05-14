#pragma once

#include <Core/UUID.h>
#include <Common/Logger.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB
{
class ActionsDAG;
}

namespace DB::QueryPlanOptimizations
{

/// First-pass MaterializedIndex optimization. Detects the TopK vector search plan shape, rewrites
/// it into a Union of covered / uncovered branches, and attaches per-part hints to the covered
/// ReadFromMergeTree. Body is implemented in optimizeMaterializedIndex.cpp.
size_t tryUseMaterializedIndex(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// If `hints` says `part_uuid` is covered, write its hits (or an empty result
/// for zero-hit covered parts) into `read_hints.materialized_index_search_results`. Asserts
/// (chassert) that the destination is empty before the write so that a
/// second-pass optimizer attempting to re-attach hints fails fast in debug builds.
void attachMaterializedIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const MaterializedIndexHints & hints);

/// Apply attachMaterializedIndexHintForPart across every part in `parts`.
void applyMaterializedIndexHints(RangesInDataParts & parts, const MaterializedIndexHints & hints);

/// Cost helpers — exposed for unit tests.

/// Sum the algorithm-reported search cost with the framework-side verify cost
/// (PREWHERE re-evaluation over `candidate_limit` rows), coverage fallback
/// cost, and fixed MI-part/Union overheads. Result is in equivalent scanned
/// rows so it can be compared against full-scan cost.
size_t computeMaterializedIndexTotalCost(
    const AlgorithmCostEstimate & est,
    size_t candidate_limit,
    const CoverageSnapshot & coverage = {});

/// Translate `top_k` and `materialized_index_overfetch_factor` into the search
/// candidate count. Returns nullopt for settings that intentionally disable
/// the fast path or for overflow.
std::optional<size_t> computeMaterializedIndexCandidateLimit(size_t top_k, UInt64 overfetch_factor);

/// Choose a winner from already-scored candidates. `scored_by_name` must be
/// sorted ascending by name. `force_name` non-empty bypasses the
/// `fallback_cost` comparison; missing force entries log a warning and fall
/// through to the cost path. Returns nullopt when no candidate beats fallback.
std::optional<size_t> pickMaterializedIndexWinner(
    const std::vector<std::pair<String, size_t>> & scored_by_name,
    const String & force_name,
    size_t fallback_cost,
    LoggerPtr log);

/// Returns true when an ExpressionStep output other than the ORDER BY distance
/// result still depends on the search column and the MI rewrite must keep
/// reading that physical column alongside virtual `_distance`.
bool materializedIndexExpressionNeedsSearchColumn(
    const ActionsDAG & expression,
    const String & sort_column,
    const String & search_column);

}
