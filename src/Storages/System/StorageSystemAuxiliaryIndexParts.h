#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;


/// One row per Active materialized-index part. Surfaces both the generic
/// MergeTree part identity (physical partition id, block range, rows, bytes)
/// and the materialized-index-specific provenance fields read from
/// `header.json` (source partition id and source block range).
///
/// Source provenance fields come from `header.json`, not from any synthetic
/// data column — `_source_partition_id` is metadata-only and never written as
/// a normal column stream.
class StorageSystemAuxiliaryIndexParts final : public IStorageSystemOneBlock
{
public:
    StorageSystemAuxiliaryIndexParts(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemAuxiliaryIndexParts"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
