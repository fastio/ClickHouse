#pragma once

#include <Common/Exception.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <algorithm>
#include <optional>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

inline String makeMaterializedIndexCompactPartNameFromInfos(
    const std::vector<MergeTreePartInfo> & materialized_index_part_infos,
    MergeTreeDataFormatVersion format_version)
{
    std::optional<MergeTreePartInfo> compact_info;
    UInt32 max_level = 0;
    Int64 max_mutation = 0;

    for (const auto & part_info : materialized_index_part_infos)
    {
        if (!part_info.isMaterializedIndex())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compact non-materialized-index part {}", part_info.getPartNameForLogs());

        if (!compact_info)
        {
            compact_info = part_info;
        }
        else
        {
            if (compact_info->getPartitionId() != part_info.getPartitionId())
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot compact materialized-index parts from different partitions: {} and {}",
                    compact_info->getPartitionId(),
                    part_info.getPartitionId());

            compact_info->min_block = std::min(compact_info->min_block, part_info.min_block);
            compact_info->max_block = std::max(compact_info->max_block, part_info.max_block);
        }

        max_level = std::max(max_level, part_info.level);
        max_mutation = std::max(max_mutation, part_info.mutation);
    }

    if (!compact_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compact an empty materialized-index part set");
    if (max_level >= MergeTreePartInfo::MAX_LEVEL)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compact materialized-index parts with max level {}", max_level);

    compact_info->level = max_level + 1;
    compact_info->mutation = max_mutation;
    compact_info->use_legacy_max_level = false;
    return compact_info->getPartNameAndCheckFormat(format_version);
}

}
