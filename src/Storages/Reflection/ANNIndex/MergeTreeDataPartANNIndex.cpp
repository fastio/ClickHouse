#include <Storages/Reflection/ANNIndex/MergeTreeDataPartANNIndex.h>

#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>


namespace DB
{

MergeTreeDataPartANNIndex::MergeTreeDataPartANNIndex(
    const MergeTreeData & storage_,
    const MergeTreeSettings & storage_settings,
    const String & name_,
    const MergeTreePartInfo & info_,
    const MutableDataPartStoragePtr & data_part_storage_,
    const IMergeTreeDataPart * parent_part_)
    /// Type::ANNIndex marks the part as non-column-oriented so the
    /// shared `IMergeTreeDataPart` machinery (marks, granularity, column
    /// serialization) bypasses the Wide/Compact codepaths. The builder also
    /// stamps `Kind::ANNIndex` on `MergeTreePartInfo` after detecting
    /// this layout from disk.
    : IMergeTreeDataPart(storage_, storage_settings, name_, info_, data_part_storage_, Type::ANNIndex, parent_part_)
{
}

MergeTreeDataPartANNIndex::~MergeTreeDataPartANNIndex() = default;

bool MergeTreeDataPartANNIndex::isStoredOnReadonlyDisk() const
{
    return getDataPartStorage().isReadonly();
}

bool MergeTreeDataPartANNIndex::isStoredOnRemoteDisk() const
{
    return getDataPartStorage().isStoredOnRemoteDisk();
}

bool MergeTreeDataPartANNIndex::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return getDataPartStorage().supportZeroCopyReplication();
}

std::optional<time_t> MergeTreeDataPartANNIndex::getColumnModificationTime(const String & /*column_name*/) const
{
    /// Materialized-index parts have no per-column files; mtime is n/a.
    return std::nullopt;
}

std::optional<String> MergeTreeDataPartANNIndex::getFileNameForColumn(const NameAndTypePair & /*column*/) const
{
    /// Materialized-index parts are column-less; algorithm-private files
    /// are not enumerated via this path.
    return std::nullopt;
}

void MergeTreeDataPartANNIndex::loadMarksToCache(const Names & /*column_names*/, MarkCache * /*mark_cache*/) const
{
    /// No mark files exist for a materialized-index part.
}

void MergeTreeDataPartANNIndex::removeMarksFromCache(MarkCache * /*mark_cache*/) const
{
    /// Mirrors loadMarksToCache: nothing was ever inserted for this part.
}

void MergeTreeDataPartANNIndex::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    /// Per-column accounting is not applicable; total size of the
    /// algorithm-private layout is surfaced via the system tables.
    each_columns_size.clear();
    total_size = {};
}

void MergeTreeDataPartANNIndex::loadIndexGranularity()
{
    auto granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();

    if (auto buf = readFileIfExists("count.txt"))
    {
        UInt64 loaded_rows_count = 0;
        readIntText(loaded_rows_count, *buf);
        assertEOF(*buf);
        if (loaded_rows_count > 0)
            granularity->appendMark(loaded_rows_count);
    }

    index_granularity = std::move(granularity);
}

void MergeTreeDataPartANNIndex::doCheckConsistency(bool /*require_part_metadata*/) const
{
    /// `checkConsistencyBase` already verifies the standard checksums against
    /// the files listed in `checksums.txt`. There is no column stream / mark
    /// structure for a materialized-index part to validate here.
}

}
