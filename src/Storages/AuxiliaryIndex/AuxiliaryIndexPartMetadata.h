#pragma once

#include <Core/UUID.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>


namespace DB
{

class IDataPartStorage;

/// Writes the standard MergeTree metadata envelope for a materialized-index
/// part. The materialized-index payload remains opaque, but every payload file
/// is listed in `checksums.txt` so the generic replicated fetch path can load
/// and validate the part.
MergeTreeDataPartChecksums writeAuxiliaryIndexPartMetadata(
    IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage,
    UInt64 rows_count,
    const String & source_partition_id,
    UUID part_uuid);

}
