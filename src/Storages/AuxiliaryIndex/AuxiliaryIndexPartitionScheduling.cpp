#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartitionScheduling.h>

#include <algorithm>
#include <tuple>


namespace DB
{

BuildBatchSelection pickContiguousBatchInOldestPartition(
    std::vector<BuildBatchCandidateView> candidates)
{
    BuildBatchSelection selection;
    if (candidates.empty())
        return selection;

    auto oldest_it = std::min_element(
        candidates.begin(),
        candidates.end(),
        [](const auto & lhs, const auto & rhs)
        {
            return lhs.first_seen < rhs.first_seen;
        });
    const String selected_partition_id = oldest_it->source_partition_id;

    candidates.erase(
        std::remove_if(
            candidates.begin(),
            candidates.end(),
            [&](const auto & candidate)
            {
                return candidate.source_partition_id != selected_partition_id;
            }),
        candidates.end());

    std::sort(
        candidates.begin(),
        candidates.end(),
        [](const auto & lhs, const auto & rhs)
        {
            return std::tie(lhs.min_block, lhs.max_block, lhs.source_part_uuid)
                < std::tie(rhs.min_block, rhs.max_block, rhs.source_part_uuid);
        });

    std::optional<Int64> last_max_block;
    for (const auto & candidate : candidates)
    {
        if (last_max_block && candidate.min_block != *last_max_block + 1)
            break;

        selection.picked_uuids.push_back(candidate.source_part_uuid);
        selection.rows += candidate.rows;
        selection.bytes += candidate.bytes;
        if (!selection.oldest_first_seen || candidate.first_seen < *selection.oldest_first_seen)
            selection.oldest_first_seen = candidate.first_seen;
        last_max_block = candidate.max_block;
    }

    return selection;
}


bool compactRebuildPartitionConditionMet(
    const std::vector<PartIdentityView> & source_parts,
    const std::vector<PartIdentityView> & auxiliary_index_parts,
    const std::unordered_set<UUID> & covered_source_uuids)
{
    if (auxiliary_index_parts.empty() || source_parts.empty())
        return false;

    const String & physical_partition_id = auxiliary_index_parts.front().partition_id;
    for (const auto & part : auxiliary_index_parts)
    {
        if (part.partition_id != physical_partition_id)
            return false;
    }

    for (const auto & part : source_parts)
    {
        if (!covered_source_uuids.contains(part.uuid))
            return false;
        if (getAuxiliaryIndexPhysicalPartitionId(part.partition_id) != physical_partition_id)
            return false;
    }

    return true;
}

}
