#pragma once

#include <Core/NamesAndTypes.h>
#include <Core/UUID.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <memory>


namespace DB
{

class MergedBlockOutputStream;
class IDataPartStorage;
using MutableDataPartStoragePtr = std::shared_ptr<IDataPartStorage>;

/// The columnar write path shared by the ANN-index build and remap tasks
/// (Option B): an ANN-index part is a standard Wide MergeTree part holding the
/// locator columns. This bundle owns everything the stages need to stream
/// blocks into it and finalize it.
struct AnnPartStream
{
    MergeTreeData::MutableDataPartPtr part;
    std::shared_ptr<IMergeTreeDataPart::MinMaxIndex> minmax_idx;
    std::unique_ptr<MergedBlockOutputStream> stream;
    StorageMetadataPtr inner_metadata;
    NamesAndTypesList columns;
};

/// Creates the Temporary-state Wide part plus a `MergedBlockOutputStream` ready
/// to receive the locator columns. The single-source-partition invariant lets
/// us seed the partition / minmax up-front from `source_partition_id`.
AnnPartStream openAnnPartStream(
    MergeTreeData & inner,
    const String & part_name,
    UUID part_uuid,
    const String & source_partition_id,
    const MutableDataPartStoragePtr & output_storage);

}
