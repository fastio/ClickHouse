#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>

namespace DB
{

class StorageSystemReflectionParts final : public IStorageSystemOneBlock
{
public:
    StorageSystemReflectionParts(const StorageID & storage_id_, ColumnsDescription columns_description_);

    std::string getName() const override { return "SystemReflectionParts"; }

    static ColumnsDescription getColumnsDescription();

protected:
    void fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const override;
};

}
