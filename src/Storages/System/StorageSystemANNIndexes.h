#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// Snapshot of all ann indexes in all databases, one row per index.
/// Runtime columns (state / coverage_ratio / ann_index_part_count / total_rows /
/// total_bytes_on_disk / last_refresh_time) are placeholders until the
/// background build pipeline populates them.
class StorageSystemANNIndexes final : public IStorageSystemOneBlock
{
public:
    StorageSystemANNIndexes(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemANNIndexes"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
