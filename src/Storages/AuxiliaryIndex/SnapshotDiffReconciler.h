#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/AuxiliaryIndex/CoverageMap.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexRemapKind.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

enum class ReconcileCandidateKind
{
    Nothing,
    BuildBatch,
    RemapLineage,
    RebuildSourcePart,
    ObsoleteCoverage,
    CompactCandidate,
    CompactMerge,
    CompactRebuild,
};

struct BuildBatchCandidate
{
    MergeTreeData::DataPartsVector source_parts;
    std::vector<UUID> source_part_uuids;
};

struct ReconcileSourcePart
{
    UUID uuid;
    String partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt32 level = 0;
    Int64 mutation = 0;
    UInt64 rows = 0;
    bool has_part_info = false;
    bool indexed_columns_unchanged_by_mutation = false;
    MergeTreeData::DataPartPtr part;
};

struct CoverageCommitValue
{
    UInt64 total_rows = 0;
    UInt64 active_rows = 0;
    UInt64 remappable_stale_rows = 0;

    UInt64 valuableRows() const { return active_rows + remappable_stale_rows; }
    UInt64 valuableRatioPercent() const { return total_rows == 0 ? 0 : valuableRows() * 100 / total_rows; }
};

struct RemapLineageCandidate
{
    MergeTreeData::DataPartsVector old_auxiliary_index_parts;
    std::vector<UUID> old_auxiliary_index_part_uuids;
    std::vector<UUID> old_source_part_uuids;
    MergeTreeData::DataPartPtr new_source_part;
    UUID new_source_part_uuid;
    AuxiliaryIndexRemapKind remap_kind = AuxiliaryIndexRemapKind::None;
};

struct RebuildSourcePartCandidate
{
    MergeTreeData::DataPartsVector affected_auxiliary_index_parts;
    std::vector<UUID> affected_auxiliary_index_part_uuids;
    MergeTreeData::DataPartPtr source_part;
    UUID source_part_uuid;
    String reason;
};

struct CompactCandidate
{
    MergeTreeData::DataPartsVector input_auxiliary_index_parts;
    std::vector<UUID> input_auxiliary_index_part_uuids;
    String mode_hint;
};

struct ObsoleteCoverageCandidate
{
    MergeTreeData::DataPartsVector affected_auxiliary_index_parts;
    std::vector<UUID> affected_auxiliary_index_part_uuids;
    std::vector<UUID> obsolete_source_part_uuids;
    UInt64 obsolete_rows = 0;
};

/// Result of diffing the source-table snapshot against the
/// AuxiliaryIndex's current snapshot for one cycle.
///   * `delta_in`  - source parts whose UUID is not yet covered by any
///                   currently-Active materialized-index-part.
///   * `delta_out` - UUIDs that previous materialized-index-parts cover but no longer exist
///                   in the source snapshot (source part was dropped or
///                   replaced).
///   * `candidate_kind` carries the explicit scheduler candidate. The legacy
///     boolean fields are kept as compatibility aliases for existing tests and
///     callers while the scheduler migrates to candidates.
struct ReconcileResult
{
    MergeTreeData::DataPartsVector delta_in;
    std::vector<UUID> delta_out;
    ReconcileCandidateKind candidate_kind = ReconcileCandidateKind::Nothing;
    BuildBatchCandidate build_batch;
    RemapLineageCandidate remap_lineage;
    RebuildSourcePartCandidate rebuild_source_part;
    CompactCandidate compact_candidate;
    ObsoleteCoverageCandidate obsolete_coverage;
    AuxiliaryIndexRemapKind remap_kind = AuxiliaryIndexRemapKind::None;
    bool has_build_candidate = false;
    bool has_remap_target = false;
};

/// Stateless cycle-head helper. The cycle pulls the source / auxiliary_index snapshots
/// once (I-BG-14) and the aggregated coverage UUID set, and runs this
/// function to decide whether to schedule a Build or a Remap for this tick.
class SnapshotDiffReconciler
{
public:
    /// Time complexity is O(|source| + |auxiliary_index|).
    static ReconcileResult run(
        const MergeTreeData::DataPartsVector & source_snapshot,
        const MergeTreeData::DataPartsVector & auxiliary_index_snapshot,
        const std::unordered_set<UUID> & coverage);

    static ReconcileResult run(
        const MergeTreeData::DataPartsVector & source_snapshot,
        const MergeTreeData::DataPartsVector & auxiliary_index_snapshot,
        const std::unordered_map<UUID, CoverageEntry> & coverage_by_source_uuid);

    static ReconcileResult run(
        const MergeTreeData::DataPartsVector & source_snapshot,
        const MergeTreeData::DataPartsVector & auxiliary_index_snapshot,
        const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_auxiliary_index_part_uuid);

    static ReconcileResult runOnPartViews(
        const std::vector<ReconcileSourcePart> & source_snapshot,
        bool auxiliary_index_snapshot_non_empty,
        const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_auxiliary_index_part_uuid);

    /// UUID-only overload that powers the Storage-driven path above. Useful
    /// in unit tests where building full IMergeTreeDataPart instances is
    /// prohibitively heavy. The `delta_in` field of the returned result is
    /// left empty (full DataPart references are only meaningful with the
    /// pointer-bearing overload).
    static ReconcileResult runOnUuids(
        const std::vector<UUID> & source_uuid_list,
        bool auxiliary_index_snapshot_non_empty,
        const std::unordered_set<UUID> & coverage);

    static CoverageCommitValue evaluateCoverageCommitValue(
        const std::vector<ReconcileSourcePart> & active_source_snapshot,
        const std::vector<CoverageEntry> & output_coverage);
};

}
