#include <Storages/Reflection/ANNIndex/ANNIndexPartCommitter.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSelectedEntry.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

StorageReplicatedMergeTree * asReplicatedInner(const StoragePtr & inner_table_snapshot)
{
    return dynamic_cast<StorageReplicatedMergeTree *>(inner_table_snapshot.get());
}

StorageReplicatedMergeTree::ANNIndexKeeperChecks getKeeperChecks(
    StorageReplicatedMergeTree & replicated,
    const FutureANNIndexPart & future_part)
{
    StorageReplicatedMergeTree::ANNIndexKeeperChecks keeper_checks;
    replicated.assertANNIndexLeaderLease(
        future_part.replicated_leader_lease_path,
        future_part.replicated_leader_lease_payload,
        &keeper_checks);
    replicated.assertANNIndexTaskReservation(
        future_part.replicated_task_lock_path,
        future_part.replicated_task_lock_payload,
        &keeper_checks);
    return keeper_checks;
}

void commitNewPartToReplicatedInner(
    StorageReplicatedMergeTree & replicated,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureANNIndexPart & future_part)
{
    /// Background task threads do not have a Keeper component set; the
    /// inner `ReplicatedMergeTreeSink::commit` runs `tryMultiNoThrow`,
    /// which under `enforce_keeper_component_tracking` throws unless a
    /// component is set on this thread.
    auto component_guard = Coordination::setCurrentComponent("ANNIndexPartCommitter::commitNewPartToReplicatedInner");

    auto keeper_checks = getKeeperChecks(replicated, future_part);
    const String expected_part_name = part ? part->name : String{};
    auto replaced_parts = replicated.commitReplacingPartFromBackgroundTask(part, keeper_checks);
    if (!replaced_parts.empty())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ANNIndex Build part {} unexpectedly replaced {} existing replicated inner parts",
            expected_part_name,
            replaced_parts.size());
}

}

void ANNIndexPartCommitter::commitNewPart(
    ReflectionANNIndex & storage,
    const StoragePtr & inner_table_snapshot,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureANNIndexPart & future_part)
{
    if (auto * replicated = asReplicatedInner(inner_table_snapshot))
    {
        commitNewPartToReplicatedInner(*replicated, part, future_part);
        return;
    }

    auto & inner = storage.getInnerMergeTreeData(inner_table_snapshot);
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    auto lock = inner.lockParts();
    inner.renameTempPartAndAdd(part, t, lock, /*rename_in_transaction=*/false);
    t.commit(lock);
}

void ANNIndexPartCommitter::commitNewParts(
    ReflectionANNIndex & storage,
    const StoragePtr & inner_table_snapshot,
    std::vector<MergeTreeData::MutableDataPartPtr> & parts,
    const FutureANNIndexPart & future_part)
{
    if (auto * replicated = asReplicatedInner(inner_table_snapshot))
    {
        auto keeper_checks = getKeeperChecks(*replicated, future_part);
        replicated->commitReplacingPartsFromBackgroundTask(parts, keeper_checks);
        return;
    }

    auto & inner = storage.getInnerMergeTreeData(inner_table_snapshot);
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    auto lock = inner.lockParts();
    for (auto & part : parts)
        inner.renameTempPartAndReplaceUnlocked(
            part,
            lock,
            t,
            /*rename_in_transaction=*/false);
    t.commit(lock);
}

MergeTreeData::DataPartsVector ANNIndexPartCommitter::commitReplacingPart(
    ReflectionANNIndex & storage,
    const StoragePtr & inner_table_snapshot,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureANNIndexPart & future_part)
{
    if (auto * replicated = asReplicatedInner(inner_table_snapshot))
        return replicated->commitReplacingPartFromBackgroundTask(part, getKeeperChecks(*replicated, future_part));

    auto & inner = storage.getInnerMergeTreeData(inner_table_snapshot);
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
