#pragma once

#include <string_view>


namespace DB
{

enum class MaterializedIndexRemapKind
{
    None,
    MergeLineage,
    MutationLineage,
    ObsoleteCoverageCleanup,
};

inline std::string_view materializedIndexRemapKindName(MaterializedIndexRemapKind kind)
{
    switch (kind)
    {
        case MaterializedIndexRemapKind::None:
            return "None";
        case MaterializedIndexRemapKind::MergeLineage:
            return "MergeLineage";
        case MaterializedIndexRemapKind::MutationLineage:
            return "MutationLineage";
        case MaterializedIndexRemapKind::ObsoleteCoverageCleanup:
            return "ObsoleteCoverageCleanup";
    }
    return "None";
}

}
