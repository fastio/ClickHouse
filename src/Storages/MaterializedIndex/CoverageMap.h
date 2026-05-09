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

/// One entry of an mi-part's coverage manifest: a source part it contributes
/// to, plus the row count covered there. With full-part coverage (the only
/// regime today) the same source part may appear under multiple mi-parts with
/// equal `rows`, so the aggregator keeps the maximum to stay idempotent.
struct CoverageEntry
{
    UUID source_part_uuid;
    UInt64 rows = 0;
};

/// Process-wide map kept on `StorageMaterializedIndex` that materialises which
/// source parts the index already covers. The reconciler reads it (via
/// `coveredSourceUuids`); BuildTask appends to it after a successful commit;
/// RemapTask atomically swaps an old mi-part for a new one; SYSTEM SYNC waits
/// on it via `waitForFullCoverage`. All mutating operations notify any waiter.
class CoverageMap
{
public:
    CoverageMap() = default;
    CoverageMap(const CoverageMap &) = delete;
    CoverageMap & operator=(const CoverageMap &) = delete;

    /// Replace the whole map. Used by `StorageMaterializedIndex::startup` to
    /// load the manifest of every active mi-part it found on disk.
    void replaceAll(std::vector<std::pair<UUID, std::vector<CoverageEntry>>> entries);

    /// Add one mi-part's manifest to the map. Called from BuildTask::finish
    /// after the transaction has committed (and the lock has been released).
    void appendFromBuild(UUID mi_part_uuid, std::vector<CoverageEntry> entries);

    /// Atomically retire one mi-part and install another. `outgoing_source_uuids`
    /// is informational; the source UUIDs that disappear are computed from the
    /// retired mi-part's own entries plus the new mi-part's incoming list.
    void applyRemap(
        UUID new_mi_part_uuid,
        UUID retired_mi_part_uuid,
        std::vector<CoverageEntry> incoming,
        std::vector<UUID> outgoing_source_uuids);

    /// Forget a single mi-part. Idempotent — calling it on an unknown UUID is
    /// not an error.
    void dropMiPart(UUID mi_part_uuid);

    /// Forget everything. Used by `StorageMaterializedIndex::drop`.
    void clear();

    std::unordered_set<UUID> coveredSourceUuids() const;
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

    /// mi_part_uuid -> entries it contributes. Authoritative.
    std::unordered_map<UUID, std::vector<CoverageEntry>> mi_to_entries;

    /// Derived view: source UUID -> max rows seen across all mi-parts. Rebuilt
    /// after every mutation. Reads happen far more often than writes (the
    /// reconciler queries every cycle), so it pays to keep this materialised.
    std::unordered_map<UUID, UInt64> source_uuid_to_rows;
};

}
