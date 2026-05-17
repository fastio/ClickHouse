#include <Storages/MaterializedIndex/MaterializedIndexPartCommitter.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

StorageReplicatedMergeTree * asReplicatedInner(StorageMaterializedIndex & storage)
{
    return dynamic_cast<StorageReplicatedMergeTree *>(storage.getInnerTable().get());
}

StorageReplicatedMergeTree::MaterializedIndexKeeperChecks getKeeperChecks(
    StorageReplicatedMergeTree & replicated,
    const FutureMaterializedIndexPart & future_part)
{
    StorageReplicatedMergeTree::MaterializedIndexKeeperChecks keeper_checks;
    replicated.assertMaterializedIndexLeaderLease(
        future_part.replicated_leader_lease_path,
        future_part.replicated_leader_lease_payload,
        &keeper_checks);
    replicated.assertMaterializedIndexTaskReservation(
        future_part.replicated_task_lock_path,
        future_part.replicated_task_lock_payload,
        &keeper_checks);
    return keeper_checks;
}

void commitNewPartToReplicatedInner(
    StorageMaterializedIndex & storage,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureMaterializedIndexPart & future_part)
{
    auto * replicated = asReplicatedInner(storage);
    if (!replicated)
        return;

    /// Background task threads do not have a Keeper component set; the
    /// inner `ReplicatedMergeTreeSink::commit` runs `tryMultiNoThrow`,
    /// which under `enforce_keeper_component_tracking` throws unless a
    /// component is set on this thread.
    auto component_guard = Coordination::setCurrentComponent("MaterializedIndexPartCommitter::commitNewPartToReplicatedInner");

    auto keeper_checks = getKeeperChecks(*replicated, future_part);
    const String expected_part_name = part ? part->name : String{};
    auto replaced_parts = replicated->commitReplacingPartFromBackgroundTask(part, keeper_checks);
    if (!replaced_parts.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndex Build part {} unexpectedly replaced {} existing replicated inner parts",
            expected_part_name,
            replaced_parts.size());
}

}

void MaterializedIndexPartCommitter::commitNewPart(
    StorageMaterializedIndex & storage,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureMaterializedIndexPart & future_part)
{
    if (asReplicatedInner(storage))
    {
        commitNewPartToReplicatedInner(storage, part, future_part);
        return;
    }

    auto & inner = storage.getInnerMergeTreeData();
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    auto lock = inner.lockParts();
    inner.renameTempPartAndAdd(part, t, lock, /*rename_in_transaction=*/false);
    t.commit(lock);
}

void MaterializedIndexPartCommitter::commitNewParts(
    StorageMaterializedIndex & storage,
    std::vector<MergeTreeData::MutableDataPartPtr> & parts,
    const FutureMaterializedIndexPart & future_part)
{
    if (auto * replicated = asReplicatedInner(storage))
    {
        auto keeper_checks = getKeeperChecks(*replicated, future_part);
        for (auto & part : parts)
            replicated->commitReplacingPartFromBackgroundTask(part, keeper_checks);
        return;
    }

    auto & inner = storage.getInnerMergeTreeData();
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    for (auto & part : parts)
        t.addPart(part, /*need_rename=*/true);
    t.renameParts();
    auto lock = inner.lockParts();
    t.commit(lock);
}

MergeTreeData::DataPartsVector MaterializedIndexPartCommitter::commitReplacingPart(
    StorageMaterializedIndex & storage,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureMaterializedIndexPart & future_part)
{
    if (auto * replicated = asReplicatedInner(storage))
        return replicated->commitReplacingPartFromBackgroundTask(part, getKeeperChecks(*replicated, future_part));

    auto & inner = storage.getInnerMergeTreeData();
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    auto lock = inner.lockParts();
    auto replaced_parts = inner.renameTempPartAndReplaceUnlocked(
        part,
        lock,
        t,
        /*rename_in_transaction=*/false);
    t.commit(lock);
    return replaced_parts;
}

}
