#pragma once

#include <DataTypes/DataTypeCustom.h>

namespace DB
{

class DataTypePostingList : public IDataTypeCustomName
{
    static constexpr auto NAME = "PostingList";
public:
    explicit DataTypePostingList() = default;

    String getName() const override
    {
        return NAME;
    }
};

}
