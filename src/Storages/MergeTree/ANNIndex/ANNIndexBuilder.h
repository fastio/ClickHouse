#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManifest.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/DiskANNIndex.h>
#include <Storages/StorageSnapshot.h>

#include <Common/Logger.h>

#include <memory>
#include <vector>

namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
class MergeTreeData;

/// Aggregate input passed to `ANNIndexBuilder::build`. Keeps the builder signature stable as
/// the manager's upstream responsibilities evolve.
struct ANNBuildInput
{
    /// Parts to index. Order matters — the `.fbin` is written in this order and DiskANN assigns
    /// internal ids in the same order.
    std::vector<DataPartPtr> selected_parts;
    String vector_column_name;

    /// Storage that owns the parts and the snapshot used to resolve the vector column.
    const MergeTreeData * storage = nullptr;
    StorageSnapshotPtr storage_snapshot;

    /// Shape fingerprint derived from the `CREATE INDEX ... TYPE ann(...)` definition.
    ANNIndexShapeFingerprint shape;

    /// Partition hashing configuration. Mirrored into `meta.json` so that a query-time search
    /// can reproduce the same hash.
    String hash_algo = "sipHash64";
    UInt64 hash_seed = 0;

    /// DiskANN FFI parameters.
    DiskANNBuildOptions build_options;
    DiskANNSearchOptions search_defaults;
};

/// Orchestrates the three stages that transform a list of parts into a ready-to-query
/// `ANNIndexGroup`:
///   1. scan parts → write `vectors.fbin` + `id_map` + `coverage` (via `VectorStreamWriter`);
///   2. drive the DiskANN FFI build against `vectors.fbin`;
///   3. persist `id_map.bin`, `coverage.bin`, `meta.json` and remove the transient `.fbin`.
///
/// The class is a thin, stateless orchestrator (≤ 120 lines). It does not clean up the
/// temporary group directory on failure — the caller (background build task / manager startup
/// GC) owns the lifecycle of `tmp_storage`.
class ANNIndexBuilder
{
public:
    static ANNIndexGroupPtr build(
        const ANNBuildInput & input,
        ANNGroupStoragePtr tmp_storage,
        LoggerPtr log);

    /// Name of the transient file produced by the first stage and removed in the last. Exposed
    /// so that tests can assert on its absence after a successful build.
    static constexpr std::string_view FBIN_FILE_NAME = "vectors.fbin";
};

}
#endif
