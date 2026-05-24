#pragma once

#include <string_view>


namespace DB
{

enum class AuxiliaryIndexRemapKind
{
    None,
    MergeLineage,
    MutationLineage,
    ObsoleteCoverageCleanup,
};

inline std::string_view materializedIndexRemapKindName(AuxiliaryIndexRemapKind kind)
{
    switch (kind)
    {
        case AuxiliaryIndexRemapKind::None:
            return "None";
        case AuxiliaryIndexRemapKind::MergeLineage:
            return "MergeLineage";
        case AuxiliaryIndexRemapKind::MutationLineage:
            return "MutationLineage";
        case AuxiliaryIndexRemapKind::ObsoleteCoverageCleanup:
            return "ObsoleteCoverageCleanup";
    }
    return "None";
}

}
