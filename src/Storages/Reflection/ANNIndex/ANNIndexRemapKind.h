#pragma once

#include <string_view>


namespace DB
{

enum class ANNIndexRemapKind
{
    None,
    MergeLineage,
    MutationLineage,
    ObsoleteCoverageCleanup,
};

inline std::string_view materializedIndexRemapKindName(ANNIndexRemapKind kind)
{
    switch (kind)
    {
        case ANNIndexRemapKind::None:
            return "None";
        case ANNIndexRemapKind::MergeLineage:
            return "MergeLineage";
        case ANNIndexRemapKind::MutationLineage:
            return "MutationLineage";
        case ANNIndexRemapKind::ObsoleteCoverageCleanup:
            return "ObsoleteCoverageCleanup";
    }
    return "None";
}

}
