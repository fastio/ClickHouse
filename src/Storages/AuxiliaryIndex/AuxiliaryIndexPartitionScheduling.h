#pragma once

#include <Core/UUID.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartName.h>

#include <chrono>
#include <optional>
#include <unordered_set>
#include <vector>


namespace DB
{

/// Lightweight view of one source-part candidate considered for a Build batch.
/// Used by `pickContiguousBatchInOldestPartition` so the partition + contiguity
/// decision is a pure-data computation, decoupled from `IMergeTreeDataPart`.
struct BuildBatchCandidateView
{
    UUID source_part_uuid;
    String source_partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt64 rows = 0;
    UInt64 bytes = 0;
    std::chrono::steady_clock::time_point first_seen{};
};

struct BuildBatchSelection
{
    /// UUIDs of selected candidates, ordered by ascending `(min_block, max_block, uuid)`.
    std::vector<UUID> picked_uuids;
    UInt64 rows = 0;
    UInt64 bytes = 0;
    std::optional<std::chrono::steady_clock::time_point> oldest_first_seen;
};

/// Selection rules:
///   1. Pick the source partition containing the oldest `first_seen` candidate.
///   2. Drop candidates from any other source partition.
///   3. Sort remaining candidates by `(min_block, max_block, uuid)`.
///   4. Stop at the first block-range gap (`min_block != prev.max_block + 1`).
///
/// Returns an empty selection if `candidates` is empty.
///
/// Threshold checks (`min_rows`, `min_bytes`, `min_parts`, `max_delay`) and
/// `initial_build` / `force_build` overrides remain in the caller because
/// they depend on `MergeTreeSetting`.
BuildBatchSelection pickContiguousBatchInOldestPartition(
    std::vector<BuildBatchCandidateView> candidates);


/// Identity slice of a MergeTree part for partition-scheduling decisions.
struct PartIdentityView
{
    UUID uuid;
    String partition_id;
};

/// Returns true iff:
///   (a) every materialized-index part shares a single physical partition id;
///   (b) every source part is in `covered_source_uuids`;
///   (c) every source part's `partition_id` maps (via
///       `getAuxiliaryIndexPhysicalPartitionId`) to that same id.
///
/// Pure helper for `StorageANN::shouldScheduleCompactRebuild`;
/// the size threshold (`auxiliary_index_compact_min_parts`) and empty-source
/// guard remain in the caller because they depend on settings.
bool compactRebuildPartitionConditionMet(
    const std::vector<PartIdentityView> & source_parts,
    const std::vector<PartIdentityView> & auxiliary_index_parts,
    const std::unordered_set<UUID> & covered_source_uuids);

}
