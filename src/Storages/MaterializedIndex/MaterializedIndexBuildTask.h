#pragma once

#include <base/scope_guard.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/MaterializedIndexLog.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <string_view>


namespace DB
{

class StorageMaterializedIndex;
class BuildTask;
class IDataPartStorage;
class IReservation;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using ReservationPtr = std::unique_ptr<IReservation>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;


/// Top-level executable task that owns one BUILD round of a materialized
/// index. Mirrors MergePlainMergeTreeTask: a NEED_PREPARE -> NEED_EXECUTE ->
/// NEED_FINISH -> SUCCESS state machine, with `prepare` constructing the
/// mid-layer BuildTask, `executeStep` driving its stages,
/// and `finish` committing the produced part through MergeTreeData::Transaction.
class MaterializedIndexBuildTask : public IExecutableTask
{
public:
    MaterializedIndexBuildTask(
        StorageMaterializedIndex & storage_,
        MaterializedIndexBuildSelectedEntryPtr entry_,
        MergeTreeData::DataPartsVector source_snapshot_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_object_,
        StorageMetadataPtr source_metadata_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        UInt64 estimated_output_bytes_,
        IExecutableTask::TaskResultCallback task_result_callback_);

    ~MaterializedIndexBuildTask() override;

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
        MaterializedIndexLogElement::Type type,
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

    State state{State::NEED_PREPARE};

    StorageMaterializedIndex & storage_ref;
    MaterializedIndexBuildSelectedEntryPtr entry;
    MergeTreeData::DataPartsVector source_snapshot;
    const MergeTreeData * source_storage = nullptr;
    StorageSnapshotPtr source_snapshot_object;
    StorageMetadataPtr source_metadata;
    ContextPtr context;
    UInt64 memory_budget_bytes = 0;
    UInt64 estimated_output_bytes = 0;
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<BuildTask> build_materialized_index_part_task;
    MergeTreeData::MutableDataPartPtr new_materialized_index_part;

    /// Tmp directories and reserved space created in `prepare`. They must
    /// outlive the mid-layer BuildTask writer.
    scope_guard tmp_output_dir_holder;
    scope_guard tmp_intermediate_dir_holder;
    ReservationPtr reserved_space;
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;

    Priority priority;
};

using BuildTaskPtr = std::shared_ptr<MaterializedIndexBuildTask>;

}
