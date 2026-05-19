#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class StorageMaterializedIndex;
struct FutureMaterializedIndexPart;

class MaterializedIndexPartCommitter
{
public:
    static void commitNewPart(
        StorageMaterializedIndex & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureMaterializedIndexPart & future_part);
    static void commitNewParts(
        StorageMaterializedIndex & storage,
        const StoragePtr & inner_table_snapshot,
        std::vector<MergeTreeData::MutableDataPartPtr> & parts,
        const FutureMaterializedIndexPart & future_part);
    static MergeTreeData::DataPartsVector commitReplacingPart(
        StorageMaterializedIndex & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureMaterializedIndexPart & future_part);
};

}
