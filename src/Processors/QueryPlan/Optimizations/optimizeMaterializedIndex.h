#pragma once

#include <Core/UUID.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
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

}
