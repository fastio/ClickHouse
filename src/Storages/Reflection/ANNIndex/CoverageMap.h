#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <chrono>
#include <condition_variable>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace DB
{

/// One entry of a materialized-index-part's coverage manifest: a source part it contributes
/// to, plus the row count covered there. With full-part coverage (the only
/// regime today) the same source part may appear under multiple materialized-index-parts with
/// equal `rows`, so the aggregator keeps the maximum to stay idempotent.
struct CoverageEntry
{
    UUID source_part_uuid;
    UInt64 rows = 0;
    String source_part_name;
    String partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt32 level = 0;
    Int64 mutation = 0;
    bool has_part_info = false;
};

/// Process-wide map kept on `ReflectionANNIndex` that materialises which
/// source parts the index already covers. The reconciler reads it (via
/// `coveredSourceUuids`); ANNIndexBuildTask appends to it after a successful commit;
/// ANNIndexRemapTask atomically swaps an old materialized-index-part for a new one; SYSTEM SYNC waits
/// on it via `waitForFullCoverage`. All mutating operations notify any waiter.
class CoverageMap
{
public:
    struct RemapCommit
    {
        UUID new_ann_index_part_uuid;
        UUID retired_ann_index_part_uuid;
        std::vector<CoverageEntry> incoming;
    };

    CoverageMap() = default;
    CoverageMap(const CoverageMap &) = delete;
    CoverageMap & operator=(const CoverageMap &) = delete;

    /// Replace the whole map. Used by `ReflectionANNIndex::startup` to
    /// load the manifest of every active materialized-index-part it found on disk.
    void replaceAll(std::vector<std::pair<UUID, std::vector<CoverageEntry>>> entries);

    /// Add one materialized-index-part's manifest to the map. Called from ANNIndexBuildTask::finish
    /// after the transaction has committed (and the lock has been released).
    void appendFromBuild(UUID ann_index_part_uuid, std::vector<CoverageEntry> entries);

    /// Atomically retire one materialized-index-part and install another. `outgoing_source_uuids`
    /// is informational; the source UUIDs that disappear are computed from the
    /// retired materialized-index-part's own entries plus the new materialized-index-part's incoming list.
    void applyRemap(
        UUID new_ann_index_part_uuid,
        UUID retired_ann_index_part_uuid,
        std::vector<CoverageEntry> incoming,
        std::vector<UUID> outgoing_source_uuids);
    void applyRemapBatch(std::vector<RemapCommit> commits);

    /// Atomically retire several materialized-index-parts and install their compacted replacement.
    void applyCompact(
        UUID new_ann_index_part_uuid,
        const std::vector<UUID> & retired_ann_index_part_uuids,
        std::vector<CoverageEntry> incoming);

    /// Forget a single materialized-index-part. Idempotent — calling it on an unknown UUID is
    /// not an error.
    void dropMiPart(UUID ann_index_part_uuid);

    /// Forget everything. Used by `ReflectionANNIndex::drop`.
    void clear();

    std::unordered_set<UUID> coveredSourceUuids() const;
    std::unordered_map<UUID, CoverageEntry> coverageEntriesBySourceUuid() const;
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverageEntriesByMiPartUuid() const;
    std::unordered_set<UUID> miPartUuidsCoveringAnySourceUuid(const std::vector<UUID> & source_uuids) const;
    UInt64 coveredRows() const;

    /// True iff every UUID in `source_active_uuids` is currently covered.
    bool isFullyCovering(const std::unordered_set<UUID> & source_active_uuids) const;

    /// Block until `isFullyCovering(source_active_uuids)` is true or `timeout`
    /// elapses. Returns true on success, false on timeout. Spurious wakeups
    /// are handled by the predicate form of `cv.wait_for`.
    bool waitForFullCoverage(
        const std::unordered_set<UUID> & source_active_uuids,
        std::chrono::milliseconds timeout);

private:
    void rebuildSourceMapNoLock();
    bool isFullyCoveringNoLock(const std::unordered_set<UUID> & source_active_uuids) const;

    mutable std::shared_mutex mutex;
    std::condition_variable_any cv;

    /// ann_index_part_uuid -> entries it contributes. Authoritative.
    std::unordered_map<UUID, std::vector<CoverageEntry>> ann_index_to_entries;

    /// Derived view: source UUID -> max rows seen across all materialized-index-parts. Rebuilt
    /// after every mutation. Reads happen far more often than writes (the
    /// reconciler queries every cycle), so it pays to keep this materialised.
    std::unordered_map<UUID, UInt64> source_uuid_to_rows;
};

}
