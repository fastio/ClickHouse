#pragma once

#include <Core/UUID.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ANNIndexLog.h>
#include <Storages/Reflection/ANNIndex/ANNIndexRemapKind.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSelectedEntry.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <memory>
#include <string_view>
#include <vector>


namespace DB
{

class ReflectionANNIndex;
class IReservation;
using ReservationPtr = std::unique_ptr<IReservation>;
struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

namespace ANNIndex
{

class RemapTaskImpl;

/// Top-level executable task that owns one REMAP round of a materialized
/// index. Mirrors `ANNIndex::BuildTask`: a four-state state machine where `prepare`
/// constructs the mid-layer RemapTaskImpl, `executeStep` drives
/// its stages, and `finish` commits the produced parts atomically through a
/// MergeTreeData::Transaction (which also marks superseded materialized-index-parts Outdated).
class RemapTask : public IExecutableTask
{
public:
    RemapTask(
        ReflectionANNIndex & storage_,
        StoragePtr storage_holder_,
        StoragePtr source_storage_holder_,
        ANNIndexRemapSelectedEntryPtr entry_,
        MergeTreeData::DataPartsVector affected_ann_index_parts_,
        MergeTreeData::DataPartsVector delta_in_source_parts_,
        std::vector<UUID> delta_out_source_uuids_,
        ANNIndexRemapKind remap_kind_,
        const MergeTreeData * source_storage_,
        StorageSnapshotPtr source_snapshot_object_,
        ContextPtr context_,
        UInt64 memory_budget_bytes_,
        IExecutableTask::TaskResultCallback task_result_callback_);

    ~RemapTask() override;

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
    void cleanupAfterFailedCommit() noexcept;

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,
        SUCCESS,
    };

    /// Lifetime anchors — see `ANNIndex::BuildTask` for the rationale.
    /// `affected_ann_index_parts`, `delta_in_source_parts` and
    /// `new_ann_index_parts` hold part `shared_ptr`s whose `clearCaches`
    /// path needs the enclosing `MergeTreeData &` (MI inner storage and the
    /// source storage respectively) to still be alive.
    StoragePtr storage_holder;
    StoragePtr source_storage_holder;
    StoragePtr inner_storage_holder;

    State state{State::NEED_PREPARE};

    ReflectionANNIndex & storage_ref;
    ANNIndexRemapSelectedEntryPtr entry;
    MergeTreeData::DataPartsVector affected_ann_index_parts;
    MergeTreeData::DataPartsVector delta_in_source_parts;
    std::vector<UUID> delta_out_source_uuids;
    ANNIndexRemapKind remap_kind = ANNIndexRemapKind::None;
    const MergeTreeData * source_storage = nullptr;
    StorageSnapshotPtr source_snapshot_object;
    ContextPtr context;
    UInt64 memory_budget_bytes = 0;
    IExecutableTask::TaskResultCallback task_result_callback;

    std::unique_ptr<RemapTaskImpl> remap_ann_index_part_task;
    std::vector<MergeTreeData::MutableDataPartPtr> new_ann_index_parts;
    std::vector<ReservationPtr> reserved_spaces;

    Priority priority;
};

using RemapTaskPtr = std::shared_ptr<RemapTask>;

}

}
