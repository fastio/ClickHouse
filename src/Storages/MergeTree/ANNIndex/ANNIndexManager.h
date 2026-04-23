#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/ANNIndex/IANNIndexBuilder.h>
#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/DiskANNIndex.h>

#include <Common/Logger.h>
#include <Common/MultiVersion.h>
#include <Core/Types.h>
#include <Disks/IVolume.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB
{

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

class MergeTreeData;

/// Immutable snapshot of the active ANN index groups, published atomically via copy-on-write
/// so the query hot path can read `groups` without acquiring any locks.
struct ANNActiveGroupsSnapshot
{
    std::vector<ANNIndexGroupPtr> groups;

    bool empty() const { return groups.empty(); }
    size_t size() const { return groups.size(); }
};
using ANNActiveGroupsSnapshotPtr = std::shared_ptr<const ANNActiveGroupsSnapshot>;

/// Single hit returned from the table-level `search`, already mapped back to the physical row
/// identity (part/row) through the per-group `PartRowIdMapReader`.
struct ANNSearchHit
{
    PartRowId row;
    float distance;
};

/// Table-level coordinator of the group-based ANN index.
///
/// Responsibilities:
///   - maintain the in-memory snapshot of active groups (copy-on-write publish);
///   - rebuild the in-memory state at server start by scanning the ANN root directory;
///   - serve top-K search by merging per-group results and mapping them to `PartRowId`;
///   - answer the `isPartCovered` predicate for scheduling decisions;
///   - provide transitions (`registerGroup`, `invalidateGroupsForMutation`,
///     `invalidateAllGroupsForShapeChange`) that move groups between active and retired;
///   - hand out per-group storages (`createTempGroupStorage`, `openGroupStorage`).
///
/// The manager does not own a `MergeTreeData` reference — `DataPartPtr`s are always passed in.
/// It also does not schedule build tasks or touch the filesystem to remove retired groups;
/// those responsibilities belong to higher layers.
class ANNIndexManager
{
public:
    struct Config
    {
        VolumePtr volume;
        std::string relative_root_path;     /// e.g. "data/db/tbl/anns"
        ANNIndexShapeFingerprint shape;
        String hash_algo = "sipHash64";
        UInt64 hash_seed = 0;
        DiskANNSearchOptions search_defaults;
        LoggerPtr log;
    };

    explicit ANNIndexManager(Config config_);
    ~ANNIndexManager() = default;
    ANNIndexManager(const ANNIndexManager &) = delete;
    ANNIndexManager & operator=(const ANNIndexManager &) = delete;

    /// Rebuild the active/retired lists by scanning `<relative_root_path>/` and classifying
    /// each child directory by its name prefix:
    ///   - `ann_<uuid>/`          → load as an active group.
    ///   - `deleting_ann_<uuid>/` → record as retired (awaiting GC).
    ///   - `tmp_ann_<uuid>/`      → ignored (left for the orphan-sweep pass).
    ///
    /// The table-level `meta.json` is consulted only for the shape fingerprint and hash seed:
    /// a shape mismatch atomically renames every `ann_*` to `deleting_ann_*` and rewrites the
    /// `meta.json` to reflect the new shape. On the very first call for a fresh root (no
    /// `meta.json`, no group directories), a `meta.json` is written with the manager's
    /// configured shape so that subsequent starts can detect a shape change.
    ///
    /// Per-group load failures are tolerated and translated into a retired-group entry with a
    /// best-effort log message. Only a corrupt `meta.json` is allowed to propagate out as an
    /// exception.
    void loadFromDisk();

    /// Table-level search. Queries every active group for top `k * max(rescoring_factor, 1)`
    /// candidates, merges the results globally by ascending distance, maps internal ids back
    /// to `PartRowId`, and truncates to that same size. The hot path is lock-free: the
    /// snapshot pointer is loaded once with `memory_order_acquire`.
    std::vector<ANNSearchHit> search(
        const float * query,
        size_t query_dim,
        size_t k,
        size_t rescoring_factor = 1) const;

    /// Does any active group cover the entire `[min_block, max_block]` range of `part` in
    /// its partition? Uses the same `hashPartitionId` as the builder side.
    bool isPartCovered(const DataPartPtr & part) const;

    /// Hashed-range variant of `isPartCovered`. Exists so that callers (and tests) that have
    /// already computed the triplet can avoid re-hashing — and so that unit tests can drive
    /// the same code path without materialising a concrete `IMergeTreeDataPart`.
    bool isRangeCovered(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const;

    /// Pick the next batch of unindexed parts from `data` and package them together with the
    /// supplied snapshot + definition into a ready-to-consume `ANNBuildSelectedEntry`.
    ///
    /// Returns `nullptr` when there is nothing to build:
    ///   - every active part is already covered, or
    ///   - total unindexed rows are below `min_rows` (wait for more), or
    ///   - `max_rows == 0` or `max_parts == 0` (feature disabled).
    ///
    /// Cumulative row count may exceed `max_rows` by the size of the last single part — an
    /// oversized part is still accepted as the minimum indivisible granule. Selection is pure
    /// over the current active-groups snapshot and stable given identical inputs.
    ANNBuildSelectedEntryPtr selectPartsForBuild(
        const MergeTreeData & data,
        StorageSnapshotPtr storage_snapshot,
        ANNIndexDefinition definition,
        UInt64 min_rows,
        UInt64 max_rows,
        UInt64 max_parts) const;

    /// Append a freshly-built group to the active snapshot (copy-on-write publish).
    /// The group must match the manager's `shape` and `hash_seed`; otherwise this throws.
    void registerGroup(ANNIndexGroupPtr new_group);

    /// Create a storage pointed at a fresh `tmp_ann_<uuid>/` directory, intended for a new
    /// build. Writes go directly to disk (no shared transaction) — the atomic publish is
    /// performed through `renameGroupDir` after the build has finished populating the
    /// directory.
    ANNGroupStoragePtr createTempGroupStorage() const;

    /// Open a storage pointed at an existing group directory (`ann_<uuid>` or
    /// `deleting_ann_<uuid>`).
    ANNGroupStoragePtr openGroupStorage(const std::string & group_dir) const;

    /// Atomically rename a group directory within the manager's root (e.g. `tmp_ann_<uuid>` →
    /// `ann_<uuid>` on build commit, or `ann_<uuid>` → `deleting_ann_<uuid>` on retire).
    /// Throws on disk error.
    void renameGroupDir(const std::string & from_name, const std::string & to_name);

    /// Enumerate the immediate children of `relative_root_path` whose name starts with any of
    /// the ANN prefixes (`ann_`, `tmp_ann_`, `deleting_ann_`). Directories only; files are
    /// skipped.
    std::vector<std::string> listGroupDirsOnDisk() const;

    /// Remove from `active` every group whose coverage intersects any of the given parts;
    /// retire the removed groups (keeping the shared_ptr alive for drain / GC) and rename the
    /// corresponding on-disk directory from `ann_<uuid>` to `deleting_ann_<uuid>` so the group
    /// stops being a candidate for `loadFromDisk` on restart.
    void invalidateGroupsForMutation(const std::vector<DataPartPtr> & affected_parts);

    /// Hashed-range variant of `invalidateGroupsForMutation`, used by callers that have
    /// already computed `(partition_hash, min_block, max_block)` tuples (and by unit tests
    /// that cannot materialise a real `IMergeTreeDataPart`).
    struct AffectedRange
    {
        UInt64 partition_hash;
        UInt64 min_block;
        UInt64 max_block;
    };
    void invalidateGroupsForRanges(const std::vector<AffectedRange> & affected_ranges);

    /// Retire every active group at once. Used when the shape of the index definition has
    /// changed and the existing groups are no longer usable. Each `ann_<uuid>` directory is
    /// atomically renamed to `deleting_ann_<uuid>`, and the table-level `meta.json` is
    /// rewritten with the new shape so that a subsequent `loadFromDisk` sees the shape match.
    void invalidateAllGroupsForShapeChange();

    ANNActiveGroupsSnapshotPtr getActiveSnapshot() const
    {
        return active.get();
    }

    /// Sorted list of retired group directory names.
    std::vector<std::string> getRetiredGroupDirs() const;

    const ANNIndexShapeFingerprint & getShape() const { return config.shape; }
    UInt64 getHashSeed() const { return config.hash_seed; }

    /// Exclusive single-slot reservation for the table-level background build. Returns true
    /// iff the slot was successfully reserved; the caller must later pair every successful
    /// reservation with exactly one `releaseBuildSlot`.
    bool tryReserveBuildSlot();
    void releaseBuildSlot();

    VolumePtr getVolume() const { return config.volume; }
    const std::string & getRelativeRootPath() const { return config.relative_root_path; }

    /// Returns the steady-clock timestamp at which `dir` was retired. If `dir` is not a
    /// known retired group the zero time_point is returned — this avoids the exception cost
    /// on the GC path.
    std::chrono::steady_clock::time_point getRetiredAt(const std::string & dir) const;

    /// Returns the `ANNIndexGroupPtr` associated with the retired directory, or nullptr if
    /// the directory is unknown or was retired through `loadFromDisk` (which stores only the
    /// manifest-recorded name without a runtime group object).
    ANNIndexGroupPtr getRetiredGroupPtr(const std::string & dir) const;

    /// Drop `dir` from the retired map. Called by the GC path after the on-disk directory
    /// has been removed and no searches hold the group any more.
    void forgetRetiredGroup(const std::string & dir);

    /// Shared `partition_id -> UInt64` hash used on both build and search sides, seeded by
    /// `config.hash_seed` so that two managers configured with the same seed produce the
    /// same value for the same `partition_id`.
    UInt64 hashPartitionId(const String & partition_id) const;

private:
    template <typename F>
    void publishWithLock(F && func);

    DiskPtr getDisk() const;

    struct RetiredMeta
    {
        std::chrono::steady_clock::time_point retired_at;
        ANNIndexGroupPtr group_ptr;
    };

    Config config;

    /// Copy-on-write snapshot held by `MultiVersion`: readers call `active.get()` to obtain a
    /// `shared_ptr<const ANNActiveGroupsSnapshot>` which keeps the snapshot alive for the
    /// duration of the caller's use; writers replace it via `active.set(...)` under
    /// `write_mtx`.
    MultiVersion<ANNActiveGroupsSnapshot> active;

    mutable std::mutex write_mtx;

    std::atomic<bool> build_in_flight{false};

    std::unordered_map<std::string, RetiredMeta> retired_group_meta;
};

using ANNIndexManagerPtr = std::shared_ptr<ANNIndexManager>;

}
#endif
