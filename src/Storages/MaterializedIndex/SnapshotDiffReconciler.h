#pragma once

#include <Core/UUID.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <unordered_set>
#include <vector>


namespace DB
{

/// Result of diffing the source-table snapshot against the
/// MaterializedIndex's current snapshot for one cycle.
///   * `delta_in`  - source parts whose UUID is not yet covered by any
///                   currently-Active mi-part.
///   * `delta_out` - UUIDs that previous mi-parts cover but no longer exist
///                   in the source snapshot (source part was dropped or
///                   replaced).
///   * `has_build_candidate` - the index is empty and the source has data.
///   * `has_remap_target`    - the index already has parts and the source
///                             changed (delta_in or delta_out non-empty).
struct ReconcileResult
{
    MergeTreeData::DataPartsVector delta_in;
    std::vector<UUID> delta_out;
    bool has_build_candidate = false;
    bool has_remap_target = false;
};

/// Stateless cycle-head helper. The cycle pulls the source / mi snapshots
/// once (I-BG-14) and the aggregated coverage UUID set, and runs this
/// function to decide whether to schedule a Build or a Remap for this tick.
class SnapshotDiffReconciler
{
public:
    /// Time complexity is O(|source| + |mi|).
    static ReconcileResult run(
        const MergeTreeData::DataPartsVector & source_snapshot,
        const MergeTreeData::DataPartsVector & mi_snapshot,
        const std::unordered_set<UUID> & coverage);

    /// UUID-only overload that powers the Storage-driven path above. Useful
    /// in unit tests where building full IMergeTreeDataPart instances is
    /// prohibitively heavy. The `delta_in` field of the returned result is
    /// left empty (full DataPart references are only meaningful with the
    /// pointer-bearing overload).
    static ReconcileResult runOnUuids(
        const std::vector<UUID> & source_uuid_list,
        bool mi_snapshot_non_empty,
        const std::unordered_set<UUID> & coverage);
};

}
