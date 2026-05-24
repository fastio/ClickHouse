#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class StorageANN;
struct FutureAuxiliaryIndexPart;

class AuxiliaryIndexPartCommitter
{
public:
    static void commitNewPart(
        StorageANN & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureAuxiliaryIndexPart & future_part);
    static void commitNewParts(
        StorageANN & storage,
        const StoragePtr & inner_table_snapshot,
        std::vector<MergeTreeData::MutableDataPartPtr> & parts,
        const FutureAuxiliaryIndexPart & future_part);
    static MergeTreeData::DataPartsVector commitReplacingPart(
        StorageANN & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureAuxiliaryIndexPart & future_part);
};

}
