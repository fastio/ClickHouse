#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManifest.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapReader.h>
#include <Storages/MergeTree/DiskANNIndex.h>

#include <Core/Types.h>

#include <memory>
#include <vector>

namespace DB
{

/// Runtime handle for a fully-built ANN index group.
///
/// A group bundles four artefacts that were produced together by `ANNIndexBuilder::build`:
///   - a DiskANN FFI searcher (backed by `idx_*.*` in the group directory);
///   - a `PartRowIdMapReader` loaded once from `id_map.bin`;
///   - an `ANNGroupCoverage` loaded once from `coverage.bin`;
///   - a shape fingerprint and `hash_seed` read back from `meta.json`.
///
/// The object is immutable after construction: all members (in particular the FFI searcher) are
/// safe to use concurrently from multiple threads without additional synchronisation.
class ANNIndexGroup
{
public:
    struct SearchHit
    {
        UInt32 internal_id;
        float distance;
    };

    /// Reload a previously-built group from disk. Reads `meta.json` to obtain the shape and
    /// hash seed, opens the FFI searcher using the standard artefact prefix, and mmaps the
    /// id_map / coverage sidecars.
    ///
    /// Throws `CORRUPTED_DATA` if any sidecar is missing, corrupt, or inconsistent with
    /// `meta.json` (e.g. id_map row count does not match `num_points`).
    static std::shared_ptr<ANNIndexGroup> load(
        ANNGroupStoragePtr storage,
        const DiskANNSearchOptions & search_options);

    /// Directly construct from already-built components. Used by the builder to avoid reopening
    /// the FFI searcher and re-loading the sidecars immediately after `build`. Callers must
    /// guarantee consistency of the triplet (same `num_points`, same shape).
    ANNIndexGroup(
        ANNGroupStoragePtr storage_,
        ANNIndexShapeFingerprint shape_,
        UInt64 hash_seed_,
        DiskANNDiskIndexSearcherPtr searcher_,
        PartRowIdMapReader id_map_,
        ANNGroupCoverage coverage_);

    ~ANNIndexGroup() = default;
    ANNIndexGroup(const ANNIndexGroup &) = delete;
    ANNIndexGroup & operator=(const ANNIndexGroup &) = delete;

    /// Run an ANN search over the graph. `search_list_size` / `beam_width` default to the
    /// values stored in `meta.json` (passed to the FFI searcher at load time) when zero.
    std::vector<SearchHit> search(
        const float * query,
        size_t query_dim,
        size_t k,
        size_t search_list_size = 0,
        size_t beam_width = 0) const;

    /// Map a DiskANN `internal_id` (i.e. vertex id) back to the source row identity.
    /// Unchecked — caller must ensure `internal_id < numPoints()`.
    PartRowId lookup(UInt32 internal_id) const { return id_map.lookup(internal_id); }

    /// Coverage predicate exposed for the manager's `isPartCovered` check.
    bool containsPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const
    {
        return coverage.containsPart(partition_hash, min_block, max_block);
    }

    const ANNGroupCoverage & getCoverage() const { return coverage; }
    const ANNIndexShapeFingerprint & getShape() const { return shape; }
    UInt64 getHashSeed() const { return hash_seed; }
    size_t numPoints() const { return id_map.size(); }

    /// Last path component of the group directory, e.g. `group_<uuid>`.
    std::string getGroupDir() const { return storage->getGroupDir(); }
    const IANNGroupStorage & getStorage() const { return *storage; }

    /// `meta.json` file name used by the builder / loader pair.
    static constexpr std::string_view META_FILE_NAME = "meta.json";

private:
    ANNGroupStoragePtr storage;
    ANNIndexShapeFingerprint shape;
    UInt64 hash_seed;
    DiskANNDiskIndexSearcherPtr searcher;
    PartRowIdMapReader id_map;
    ANNGroupCoverage coverage;
};

using ANNIndexGroupPtr = std::shared_ptr<ANNIndexGroup>;

}
#endif
