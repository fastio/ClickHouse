#include <Storages/MaterializedIndex/MergeTreeDataPartMaterializedIndex.h>


namespace DB
{

MergeTreeDataPartMaterializedIndex::MergeTreeDataPartMaterializedIndex(
    const MergeTreeData & storage_,
    const MergeTreeSettings & storage_settings,
    const String & name_,
    const MergeTreePartInfo & info_,
    const MutableDataPartStoragePtr & data_part_storage_,
    const IMergeTreeDataPart * parent_part_)
    /// Type::MaterializedIndex marks the part as non-column-oriented so the
    /// shared `IMergeTreeDataPart` machinery (marks, granularity, column
    /// serialization) bypasses the Wide/Compact codepaths and the identity
    /// is also reflected at the framework level (in addition to
    /// `Kind::MaterializedIndex` on `MergeTreePartInfo`).
    : IMergeTreeDataPart(storage_, storage_settings, name_, info_, data_part_storage_, Type::MaterializedIndex, parent_part_)
{
}

MergeTreeDataPartMaterializedIndex::~MergeTreeDataPartMaterializedIndex() = default;

bool MergeTreeDataPartMaterializedIndex::isStoredOnReadonlyDisk() const
{
    return getDataPartStorage().isReadonly();
}

bool MergeTreeDataPartMaterializedIndex::isStoredOnRemoteDisk() const
{
    return getDataPartStorage().isStoredOnRemoteDisk();
}

bool MergeTreeDataPartMaterializedIndex::isStoredOnRemoteDiskWithZeroCopySupport() const
{
    return getDataPartStorage().supportZeroCopyReplication();
}

std::optional<time_t> MergeTreeDataPartMaterializedIndex::getColumnModificationTime(const String & /*column_name*/) const
{
    /// Materialized-index parts have no per-column files; mtime is n/a.
    return std::nullopt;
}

std::optional<String> MergeTreeDataPartMaterializedIndex::getFileNameForColumn(const NameAndTypePair & /*column*/) const
{
    /// Materialized-index parts are column-less; algorithm-private files
    /// are not enumerated via this path.
    return std::nullopt;
}

void MergeTreeDataPartMaterializedIndex::loadMarksToCache(const Names & /*column_names*/, MarkCache * /*mark_cache*/) const
{
    /// No mark files exist for a materialized-index part.
}

void MergeTreeDataPartMaterializedIndex::removeMarksFromCache(MarkCache * /*mark_cache*/) const
{
    /// Mirrors loadMarksToCache: nothing was ever inserted for this part.
}

void MergeTreeDataPartMaterializedIndex::calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const
{
    /// Per-column accounting is not applicable; total size of the
    /// algorithm-private layout is surfaced via the system tables.
    each_columns_size.clear();
    total_size = {};
}

}
