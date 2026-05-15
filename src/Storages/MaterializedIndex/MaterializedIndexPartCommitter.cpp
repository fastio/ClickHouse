#include <Storages/MaterializedIndex/MaterializedIndexPartCommitter.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

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

Coordination::Requests makeKeeperCheckOps(const StorageReplicatedMergeTree::MaterializedIndexKeeperChecks & keeper_checks)
{
    Coordination::Requests ops;
    ops.reserve(keeper_checks.size());
    for (const auto & check : keeper_checks)
        ops.emplace_back(zkutil::makeCheckRequest(check.path, check.version));
    return ops;
}

void commitNewPartToReplicatedInner(
    StorageMaterializedIndex & storage,
    MergeTreeData::MutableDataPartPtr & part,
    const FutureMaterializedIndexPart & future_part)
{
    auto * replicated = asReplicatedInner(storage);
    if (!replicated)
        return;

    auto keeper_checks = getKeeperChecks(*replicated, future_part);
    auto metadata_snapshot = replicated->getInMemoryMetadataPtr(replicated->getContext(), false);
    ReplicatedMergeTreeSink sink(
        /*async_insert_=*/false,
        *replicated,
        metadata_snapshot,
        /*quorum_=*/0,
        /*quorum_timeout_ms_=*/0,
        /*max_parts_per_block_=*/0,
        /*quorum_parallel_=*/false,
        /*majority_quorum_=*/false,
        replicated->getContext());
    sink.setAdditionalCommitChecks(makeKeeperCheckOps(keeper_checks));
    sink.writeExistingPart(part);
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
