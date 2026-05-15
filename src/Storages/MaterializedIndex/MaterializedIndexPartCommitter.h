#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

namespace DB
{

class StorageMaterializedIndex;

class MaterializedIndexPartCommitter
{
public:
    static void commitNewPart(StorageMaterializedIndex & storage, MergeTreeData::MutableDataPartPtr & part);
    static void commitNewParts(StorageMaterializedIndex & storage, std::vector<MergeTreeData::MutableDataPartPtr> & parts);
    static MergeTreeData::DataPartsVector commitReplacingPart(StorageMaterializedIndex & storage, MergeTreeData::MutableDataPartPtr & part);
};

}
