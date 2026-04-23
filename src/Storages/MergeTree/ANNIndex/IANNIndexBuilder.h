#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/MergeTreeIndexANN.h>
#include <Storages/StorageSnapshot.h>

#include <memory>
#include <vector>


namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
class ANNIndexManager;

/// Batch of parts picked for a single ANN group build, together with the snapshot and index
/// definition under which they must be indexed.
///
/// Treated as immutable once constructed: the outer framework task wraps it in a `shared_ptr`
/// and hands it to an algorithm-specific builder, which reads from it across many `execute`
/// invocations but never mutates it.
struct ANNBuildSelectedEntry
{
    std::vector<DataPartPtr> selected_parts;
    StorageSnapshotPtr storage_snapshot;
    ANNIndexDefinition definition;
};
using ANNBuildSelectedEntryPtr = std::shared_ptr<ANNBuildSelectedEntry>;


/// Pluggable "build an ANN group from this batch of parts" strategy.
///
/// Selected at factory time by `entry->definition.shape.algorithm` (currently only `"diskann"`). The
/// outer framework task knows nothing about the algorithm — it only drives the state machine
/// via `execute` and rebinds / publishes the result via `getResult`.
///
/// Contract:
///   1. Factory (`createANNIndexBuilder`) creates the builder; the ctor eagerly opens its
///      `tmp_ann_<uuid>/` directory (via `manager.createTempGroupStorage`) and prepares any
///      internal writers so that the first `execute()` call can immediately make progress.
///   2. Caller repeatedly calls `execute()` until it returns `false`. Implementations are free
///      to yield between internal stages / subtasks by returning `true` mid-way.
///   3. When `execute()` returns `false`, the tmp directory is fully populated and an
///      in-memory `ANNIndexGroup` has been constructed from its contents. The group's internal
///      storage handle still points at the tmp path.
///   4. The outer task handles the directory rename, storage rebind, manager registration, and
///      build-slot release. The builder is NOT responsible for any of those.
class IANNIndexBuilder
{
public:
    virtual ~IANNIndexBuilder() = default;

    /// Incremental step. Returns `true` while more work remains; `false` once the in-memory
    /// group is ready to be retrieved via `getResult()`.
    virtual bool execute() = 0;

    /// The constructed group. Valid only after `execute()` has returned `false`.
    virtual ANNIndexGroupPtr getResult() = 0;
};


/// Algorithm dispatch. Throws `NOT_IMPLEMENTED` for unknown algorithms.
std::unique_ptr<IANNIndexBuilder> createANNIndexBuilder(
    ANNBuildSelectedEntryPtr entry,
    ANNIndexManager & manager);

}

#endif
