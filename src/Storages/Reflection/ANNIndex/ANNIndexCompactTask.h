#pragma once

#include <base/scope_guard.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ANNIndexLog.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <string_view>


namespace DB
{

class ReflectionANNIndex;
class IDataPartStorage;
class IReservation;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using ReservationPtr = std::unique_ptr<IReservation>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

namespace ANNIndex
{

class BuildTaskImpl;

/// Top-level executable task that owns one COMPACT round of a materialized
/// index. Mirrors `ANNIndex::BuildTask`: a NEED_PREPARE -> NEED_EXECUTE ->
/// NEED_FINISH -> SUCCESS state machine, with `prepare` constructing the
/// mid-layer `BuildTaskImpl`, `executeStep` driving its stages, and `finish`
/// atomically replacing input MI parts through `MergeTreeData::Transaction`.
class CompactTask : public IExecutableTask
{
public:
    CompactTask(
        ReflectionANNIndex & storage_,
        StoragePtr storage_holder_,
        StoragePtr source_storage_holder_,
        ANNIndexBuildSelectedEntryPtr entry_,
        MergeTreeData::DataPartsVector source_snapshot_,
        MergeTreeData::DataPartsVector input_ann_index_parts_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_object_,
        StorageMetadataPtr source_metadata_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        UInt64 estimated_output_bytes_,
        IExecutableTask::TaskResultCallback task_result_callback_);

    ~CompactTask() override;

    bool executeStep() override;
    void onCompleted() override;
    void cancel() noexcept override;
    StorageID getStorageID() const override;
    String getQueryId() const override;
    Priority getPriority() const override { return priority; }

private:
    void prepare();
    void finish();
    void writeTaskLog(
        ANNIndexLogElement::Type type,
        std::string_view stage,
        UInt64 duration_ms,
        Int32 error_code,
        const String & error_message,
        UInt64 rows_added = 0,
        UInt64 bytes_added = 0) const;
    void cleanupTemporaryStorages(bool remove_output_storage = true) noexcept;
    void cleanupAfterFailedCommit() noexcept;

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,
        SUCCESS,
    };

    /// Lifetime anchors — see `ANNIndex::BuildTask` for the rationale.
    /// `source_snapshot`, `input_ann_index_parts` and
    /// `new_ann_index_part` hold part `shared_ptr`s whose `clearCaches`
    /// path needs both the source `MergeTreeData &` and the MI inner storage to
    /// still be alive.
    StoragePtr storage_holder;
    StoragePtr source_storage_holder;
    StoragePtr inner_storage_holder;

    State state{State::NEED_PREPARE};

    ReflectionANNIndex & storage_ref;
    ANNIndexBuildSelectedEntryPtr entry;
    MergeTreeData::DataPartsVector source_snapshot;
    MergeTreeData::DataPartsVector input_ann_index_parts;
    const MergeTreeData * source_storage = nullptr;
    StorageSnapshotPtr source_snapshot_object;
    StorageMetadataPtr source_metadata;
    ContextPtr context;
    UInt64 memory_budget_bytes = 0;
    UInt64 estimated_output_bytes = 0;
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<BuildTaskImpl> build_ann_index_part_task;
    MergeTreeData::MutableDataPartPtr new_ann_index_part;

    scope_guard tmp_output_dir_holder;
    scope_guard tmp_intermediate_dir_holder;
    ReservationPtr reserved_space;
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;

    Priority priority;
};

using CompactTaskPtr = std::shared_ptr<CompactTask>;

}

}
