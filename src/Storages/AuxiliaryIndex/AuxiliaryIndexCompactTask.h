#pragma once

#include <base/scope_guard.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/AuxiliaryIndexLog.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <string_view>


namespace DB
{

class BuildTask;
class IDataPartStorage;
class IReservation;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;
using ReservationPtr = std::unique_ptr<IReservation>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

class StorageANN;

class AuxiliaryIndexCompactTask : public IExecutableTask
{
public:
    AuxiliaryIndexCompactTask(
        StorageANN & storage_,
        StoragePtr storage_holder_,
        StoragePtr source_storage_holder_,
        AuxiliaryIndexBuildSelectedEntryPtr entry_,
        MergeTreeData::DataPartsVector source_snapshot_,
        MergeTreeData::DataPartsVector input_auxiliary_index_parts_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_object_,
        StorageMetadataPtr source_metadata_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        UInt64 estimated_output_bytes_,
        IExecutableTask::TaskResultCallback task_result_callback_);

    ~AuxiliaryIndexCompactTask() override;

    bool executeStep() override;
    void onCompleted() override;
    void cancel() noexcept override;
    StorageID getStorageID() const override;
    String getQueryId() const override;
    Priority getPriority() const override { return priority; }

private:
    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,
        SUCCESS,
    };

    void prepare();
    void finish();
    void writeTaskLog(
        AuxiliaryIndexLogElement::Type type,
        std::string_view stage,
        UInt64 duration_ms,
        Int32 error_code,
        const String & error_message,
        UInt64 rows_added = 0,
        UInt64 bytes_added = 0) const;
    void cleanupTemporaryStorages(bool remove_output_storage = true) noexcept;
    void cleanupAfterFailedCommit() noexcept;

    /// Lifetime anchors — see `AuxiliaryIndexBuildTask` for the rationale.
    /// `source_snapshot`, `input_auxiliary_index_parts` and
    /// `new_auxiliary_index_part` hold part `shared_ptr`s whose `clearCaches`
    /// path needs both the source `MergeTreeData &` and the MI inner storage to
    /// still be alive.
    StoragePtr storage_holder;
    StoragePtr source_storage_holder;
    StoragePtr inner_storage_holder;

    State state{State::NEED_PREPARE};

    StorageANN & storage_ref;
    AuxiliaryIndexBuildSelectedEntryPtr entry;
    MergeTreeData::DataPartsVector source_snapshot;
    MergeTreeData::DataPartsVector input_auxiliary_index_parts;
    const MergeTreeData * source_storage = nullptr;
    StorageSnapshotPtr source_snapshot_object;
    StorageMetadataPtr source_metadata;
    ContextPtr context;
    UInt64 memory_budget_bytes = 0;
    UInt64 estimated_output_bytes = 0;
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<BuildTask> build_auxiliary_index_part_task;
    MergeTreeData::MutableDataPartPtr new_auxiliary_index_part;

    scope_guard tmp_output_dir_holder;
    scope_guard tmp_intermediate_dir_holder;
    ReservationPtr reserved_space;
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;

    Priority priority;
};

using CompactTaskPtr = std::shared_ptr<AuxiliaryIndexCompactTask>;

}
