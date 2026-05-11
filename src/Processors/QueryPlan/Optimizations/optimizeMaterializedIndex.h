#pragma once

#include <Core/UUID.h>
#include <Common/Logger.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB::QueryPlanOptimizations
{

/// First-pass MaterializedIndex optimization. Detects the TopK vector search plan shape, rewrites
/// it into a Union of covered / uncovered branches, and attaches per-part hints to the covered
/// ReadFromMergeTree. Body is implemented in optimizeMaterializedIndex.cpp.
size_t tryUseMaterializedIndex(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings);

/// If `hints` contains an entry for `part_uuid`, write it into `read_hints.mi_search_results`.
/// Asserts (chassert) that the destination is empty before the write so that a second-pass
/// optimizer attempting to re-attach hints fails fast in debug builds.
void attachMaterializedIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const MaterializedIndexHints & hints);

/// Apply attachMaterializedIndexHintForPart across every part in `parts`.
void applyMaterializedIndexHints(RangesInDataParts & parts, const MaterializedIndexHints & hints);

/// Cost helpers — exposed for unit tests.

/// Sum the algorithm-reported search cost with the framework-side verify cost
/// (PREWHERE re-evaluation over `candidate_limit` rows). Result is in
/// equivalent scanned rows so it can be compared against full-scan cost.
size_t computeMaterializedIndexTotalCost(const AlgorithmCostEstimate & est, size_t candidate_limit);

/// Choose a winner from already-scored candidates. `scored_by_name` must be
/// sorted ascending by name. `force_name` non-empty bypasses the
/// `fallback_cost` comparison; missing force entries log a warning and fall
/// through to the cost path. Returns nullopt when no candidate beats fallback.
std::optional<size_t> pickMaterializedIndexWinner(
    const std::vector<std::pair<String, size_t>> & scored_by_name,
    const String & force_name,
    size_t fallback_cost,
    LoggerPtr log);

}
