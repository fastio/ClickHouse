#pragma once

#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>


namespace DB
{

class StorageMaterializedIndex;
class RemapTask;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;


/// Top-level executable task that owns one REMAP round of a materialized
/// index. Mirrors MaterializedIndexBuildTask: a four-state state machine where `prepare`
/// constructs the mid-layer RemapTask, `executeStep` drives
/// its stages, and `finish` commits the produced parts atomically through a
/// MergeTreeData::Transaction (which also marks superseded materialized-index-parts Outdated).
class MaterializedIndexRemapTask : public IExecutableTask
{
public:
    MaterializedIndexRemapTask(
        StorageMaterializedIndex & storage_,
        MaterializedIndexRemapSelectedEntryPtr entry_,
        MergeTreeData::DataPartsVector affected_mi_parts_,
        MergeTreeData::DataPartsVector delta_in_source_parts_,
        std::vector<UUID> delta_out_source_uuids_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_object_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        IExecutableTask::TaskResultCallback task_result_callback_);

    ~MaterializedIndexRemapTask() override;

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
    MaterializedIndexRemapSelectedEntryPtr entry;
    MergeTreeData::DataPartsVector affected_mi_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    const MergeTreeData * source_storage = nullptr;
    StorageSnapshotPtr source_snapshot_object;
    ContextPtr context;
    UInt64 memory_budget_bytes = 0;
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<RemapTask> remap_mi_part_task;
    std::vector<MergeTreeData::MutableDataPartPtr> new_mi_parts;

    Priority priority;
};

using RemapTaskPtr = std::shared_ptr<MaterializedIndexRemapTask>;

}
