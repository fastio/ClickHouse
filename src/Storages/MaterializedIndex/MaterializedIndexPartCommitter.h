#pragma once

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
        MergeTreeData::MutableDataPartPtr & part,
        const FutureMaterializedIndexPart & future_part);
    static void commitNewParts(
        StorageMaterializedIndex & storage,
        std::vector<MergeTreeData::MutableDataPartPtr> & parts,
        const FutureMaterializedIndexPart & future_part);
    static MergeTreeData::DataPartsVector commitReplacingPart(
        StorageMaterializedIndex & storage,
        MergeTreeData::MutableDataPartPtr & part,
        const FutureMaterializedIndexPart & future_part);
};

}
