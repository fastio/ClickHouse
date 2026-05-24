#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

/** Part of a auxiliary index backing table.
  *
  * Unlike Wide/Compact parts, a materialized-index part is not column-oriented:
  * its on-disk layout is flat: `algorithm_private_*`, `locator_*`,
  * `source_row_id_*` and `coverage.json` managed by the index algorithm,
  * not `<column>.bin` / `<column>.mrk` pairs.
  * Consequently the eight column-oriented pure-virtual methods of
  * IMergeTreeDataPart are stubbed: three delegate to the underlying storage
  * (those are disk-level properties, not column-level), and the remaining five
  * either return std::nullopt or produce empty output because the notion of a
  * per-column file does not apply.
  *
  * Per-column size accounting is intentionally empty; total size for the
  * algorithm-private artifacts is surfaced via the system tables instead.
  */
class MergeTreeDataPartAuxiliaryIndex : public IMergeTreeDataPart
{
public:
    MergeTreeDataPartAuxiliaryIndex(
        const MergeTreeData & storage_,
        const MergeTreeSettings & storage_settings,
        const String & name_,
        const MergeTreePartInfo & info_,
        const MutableDataPartStoragePtr & data_part_storage_,
        const IMergeTreeDataPart * parent_part_ = nullptr);

    ~MergeTreeDataPartAuxiliaryIndex() override;

    bool isStoredOnReadonlyDisk() const override;
    bool isStoredOnRemoteDisk() const override;
    bool isStoredOnRemoteDiskWithZeroCopySupport() const override;

    std::optional<time_t> getColumnModificationTime(const String & column_name) const override;
    std::optional<String> getFileNameForColumn(const NameAndTypePair & column) const override;

    void loadMarksToCache(const Names & column_names, MarkCache * mark_cache) const override;
    void removeMarksFromCache(MarkCache * mark_cache) const override;

    void calculateEachColumnSizes(ColumnSizeByName & each_columns_size, ColumnSize & total_size) const override;

private:
    void loadIndexGranularity() override;
    void doCheckConsistency(bool require_part_metadata) const override;
};

}
