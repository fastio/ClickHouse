#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class ReflectionANNIndex;
struct FutureANNIndexPart;

class ANNIndexPartCommitter
{
public:
    static void commitNewPart(
        ReflectionANNIndex & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureANNIndexPart & future_part);
    static void commitNewParts(
        ReflectionANNIndex & storage,
        const StoragePtr & inner_table_snapshot,
        std::vector<MergeTreeData::MutableDataPartPtr> & parts,
        const FutureANNIndexPart & future_part);
    static MergeTreeData::DataPartsVector commitReplacingPart(
        ReflectionANNIndex & storage,
        const StoragePtr & inner_table_snapshot,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureANNIndexPart & future_part);
};

}
