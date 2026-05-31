#pragma once

#include <Core/UUID.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>


namespace DB
{

class IDataPartStorage;
struct WriteSettings;

/// Rewrites `uuid.txt` for an ANN-index part. Remap uses this after hardlinking
/// old part files so the new logical part identity is persisted on disk before
/// `checksums.txt` is recomputed.
void writeANNIndexPartUUIDFile(
    IDataPartStorage & part_storage,
    const UUID & uuid,
    const WriteSettings & write_settings);

/// Recomputes `checksums.txt` over *every* file in the part directory (columnar
/// `.bin`/`.mrk`, algorithm-private payload, and the framework's JSON sidecars),
/// excluding the canonical no-checksum files. Used by the columnar build / remap
/// write paths after `MergedBlockOutputStream::finalizePart` has written the
/// locator columns, so the algorithm-private files end up inside `checksums.txt`
/// and replicated fetch compares identical checksum sets on both ends.
MergeTreeDataPartChecksums finalizeANNIndexPartChecksums(
    IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage);

}
