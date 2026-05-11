#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>


namespace DB
{

class StorageMaterializedIndex;
class BuildTask;
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
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<BuildTask> build_mi_part_task;
    MergeTreeData::MutableDataPartPtr new_mi_part;

    Priority priority;
};

using BuildTaskPtr = std::shared_ptr<MaterializedIndexBuildTask>;

}
