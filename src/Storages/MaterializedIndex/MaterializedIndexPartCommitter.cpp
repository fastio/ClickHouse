#include <Storages/MaterializedIndex/MaterializedIndexPartCommitter.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
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

void commitNewPartToReplicatedInner(StorageMaterializedIndex & storage, MergeTreeData::MutableDataPartPtr & part)
{
    auto * replicated = asReplicatedInner(storage);
    if (!replicated)
        return;

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
    sink.writeExistingPart(part);
}

}

void MaterializedIndexPartCommitter::commitNewPart(StorageMaterializedIndex & storage, MergeTreeData::MutableDataPartPtr & part)
{
    if (asReplicatedInner(storage))
    {
        commitNewPartToReplicatedInner(storage, part);
        return;
    }

    auto & inner = storage.getInnerMergeTreeData();
    MergeTreeData::Transaction t(inner, /*txn=*/nullptr);
    auto lock = inner.lockParts();
    inner.renameTempPartAndAdd(part, t, lock, /*rename_in_transaction=*/false);
    t.commit(lock);
}

void MaterializedIndexPartCommitter::commitNewParts(StorageMaterializedIndex & storage, std::vector<MergeTreeData::MutableDataPartPtr> & parts)
{
    if (auto * replicated = asReplicatedInner(storage))
    {
        for (auto & part : parts)
            replicated->commitReplacingPartFromBackgroundTask(part);
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
    MergeTreeData::MutableDataPartPtr & part)
{
    if (auto * replicated = asReplicatedInner(storage))
        return replicated->commitReplacingPartFromBackgroundTask(part);

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
