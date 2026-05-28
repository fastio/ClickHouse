#pragma once

#include <Common/ProfileEvents.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <array>
#include <atomic>
#include <future>
#include <memory>
#include <unordered_map>
#include <vector>


namespace DB
{

class IDataPartStorage;
class ReflectionANNIndex;
class WriteBufferFromFileBase;
class PullingPipelineExecutor;
class QueryPipeline;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;


/** Mid-layer background Build task for a ANNIndex table.
  *
  * Mirrors the `MergeTask::IStage` pattern: the Build work is split into a
  * fixed-size array of stages; each stage implements a 5-method contract
  * (setRuntimeContext / getContextForNextStage / getTotalTimeProfileEvent /
  * execute / cancel) and the outer `execute()` drives one stage per call so
  * the scheduler can co-operatively interleave many Build tasks.
  *
  * Scope of this class (by design):
  *   - Writes tmp-directory contents only. Does not touch `data_parts_indexes`.
  *   - Constructs a Temporary-state `MergeTreeDataPartANNIndex` at
  *     the end; the top-level Build task commits it via
  *     `MergeTreeData::Transaction`.
  *   - Owns an `AlgorithmBuildContext` whose `is_cancelled` points at the
  *     task's own `std::atomic<bool>`; the contained algorithm must not keep
  *     the pointer past `finishBuild`.
  *
  * The outer driver loop and cancellation semantics are deliberately aligned
  * with `MergeTask::execute` so that top-level `IExecutableTask` plumbing in
  * `ReflectionANNIndex` can follow the existing `MergePlainMergeTreeTask`
  * blueprint with minimal friction.
  */
class BuildTask
{
public:
    static constexpr auto TEMP_DIRECTORY_PREFIX = "tmp_ann_index_build_";

    BuildTask(
        MergeTreeData::DataPartsVector source_parts_,
        IANNIndexAlgorithm * algorithm_,
        ReflectionANNIndex * storage_,
        StoragePtr inner_storage_holder_,
        String new_part_name_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_,
        StorageMetadataPtr source_metadata_,
        ContextPtr context_,
        MutableDataPartStoragePtr output_storage_,
        MutableDataPartStoragePtr intermediate_storage_,
        UInt64 memory_budget_bytes_,
        UUID new_part_uuid_ = {});

    /// Drives one stage per call, mirroring MergeTask::execute. Returns true
    /// while more work remains, false once the final stage has completed and
    /// the promise has been fulfilled.
    bool execute();

    /// Idempotent cancellation hook. Sets the cancel flag observed by the
    /// algorithm via AlgorithmBuildContext::is_cancelled and forwards to the
    /// current stage so stage-local cleanup can run.
    void cancel() noexcept;

    std::future<MergeTreeData::MutableDataPartPtr> getFuture();

    MergeTreeData::MutableDataPartPtr getUnfinishedPart();

private:
    struct IStage;
    using StagePtr = std::shared_ptr<IStage>;

    struct IStageRuntimeContext {};
    using StageRuntimeContextPtr = std::shared_ptr<IStageRuntimeContext>;

    struct IStage
    {
        virtual void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) = 0;
        virtual StageRuntimeContextPtr getContextForNextStage() = 0;
        virtual ProfileEvents::Event getTotalTimeProfileEvent() const = 0;
        virtual bool execute() = 0;
        virtual void cancel() noexcept = 0;
        virtual ~IStage() = default;
    };

    struct GlobalRuntimeContext : public IStageRuntimeContext
    {
        /// Inputs (owned here; snapshot passed in by caller).
        MergeTreeData::DataPartsVector source_parts;
        IANNIndexAlgorithm * algorithm{nullptr};
        ReflectionANNIndex * storage{nullptr};
        StoragePtr inner_storage_holder;
        String new_part_name;
        UUID new_part_uuid;
        String source_partition_id;
        String source_partition_hash;
        Int64 source_min_block = 0;
        Int64 source_max_block = 0;

        /// Source-table plumbing required by stage 1 to spin up a sequential
        /// scan pipeline. The caller (top-level Build task) is responsible for
        /// keeping the source `StoragePtr` alive for the lifetime of this
        /// task; we hold a bare pointer plus the snapshots it produced.
        const MergeTreeData * source_storage{nullptr};
        StorageSnapshotPtr source_snapshot;
        StorageMetadataPtr source_metadata;
        ContextPtr context;

        MutableDataPartStoragePtr output_storage;
        MutableDataPartStoragePtr intermediate_storage;
        UInt64 memory_budget_bytes{0};

        /// Populated by the ctor and carried through all stages. The stages
        /// fill in the per-phase fields (e.g. segment_boundaries in stage 1)
        /// before the algorithm is invoked.
        AlgorithmBuildContext build_ctx;

        /// Owned cancellation flag. `build_ctx.is_cancelled` points at it.
        /// Kept as a member so its lifetime >= `build_ctx` lifetime.
        std::atomic<bool> is_cancelled{false};

        /// First-seen-order source UUID table built by stage 1. The vector is
        /// the on-disk authority written to `header.json::part_uuid_table`;
        /// the hash map is only for dedup while scanning source rows.
        std::unordered_map<UUID, UInt32> part_uuid_id_by_uuid;
        std::vector<UUID> part_uuid_table;

        /// Internal monotonically increasing row id assigned by stage 1.
        UInt64 internal_id_cursor{0};

        /// Stage 1 cursors. The stage reads source parts in order, one block
        /// per `execute()` call, until the source pipeline is exhausted.
        size_t current_source_part_index{0};
        UInt64 current_part_start_internal_id{0};
        UInt64 current_segment_row_count{0};
        std::unique_ptr<QueryPipeline> current_pipeline;
        std::unique_ptr<PullingPipelineExecutor> current_part_executor;

        /// Segment boundaries accumulated by stage 1 and consumed by the
        /// algorithm / metadata stages.
        /// Strictly non-decreasing; segment `[boundaries[i], boundaries[i+1])`
        /// covers internal_ids in that half-open range.
        std::vector<UInt64> segment_boundaries_buffer;

        /// Stage 1 writers. Both are rotated per segment and finalized at
        /// segment boundaries.
        std::unique_ptr<WriteBufferFromFileBase> current_locator_writer;
        std::unique_ptr<WriteBufferFromFileBase> current_source_row_id_writer;

        /// Produced by the final metadata stage; returned via `getFuture`.
        MergeTreeData::MutableDataPartPtr new_ann_index_part;
        std::promise<MergeTreeData::MutableDataPartPtr> promise;
    };

    using GlobalRuntimeContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    /// Stage 1: read source blocks, write locator/source_row_id entries, feed
    /// algorithm->prepareBuild per block.
    struct ReadColumnsWriteLocatorAndPrepareStage;

    /// Stage 2: exactly one algorithm->buildAlgorithmPrivate call.
    struct BuildAlgorithmStage;

    /// Stage 3: exactly one algorithm->finishBuild call.
    struct FinishAlgorithmStage;

    /// Stage 4: reclaim intermediate_storage.
    struct CleanupIntermediateStage;

    /// Stage 5: write header / coverage and the standard MergeTree part
    /// metadata envelope, fsync in the canonical data -> meta -> dir order,
    /// then construct the Temporary-state MergeTreeDataPartANNIndex.
    struct FinalizeMetadataStage;

    GlobalRuntimeContextPtr global_ctx;

    using Stages = std::array<StagePtr, 5>;
    const Stages stages;

    Stages::const_iterator stages_iterator = stages.begin();

    /// Ensures the promise is fulfilled exactly once: final stage on success,
    /// execute() catch on exception.
    bool promise_fulfilled{false};

    /// Factory for the stages array. Kept as a static member so it can name
    /// the private nested stage structs; invoked from the ctor's member
    /// initializer list.
    static Stages makeStages();
};

}
