#pragma once

#include <Core/Block.h>
#include <Common/SipHash.h>
#include <Common/Exception.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>

#include <memory>
#include <optional>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct AuxiliaryIndexSourcePartitionRange
{
    String source_partition_id;
    String source_partition_hash;
    Int64 min_block = 0;
    Int64 max_block = 0;
    Int64 mutation = 0;
};

inline constexpr auto AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN = "_source_partition_id";

inline const Block & getAuxiliaryIndexPartitionKeySampleBlock()
{
    static const Block sample_block
    {
        ColumnWithTypeAndName{
            nullptr,
            std::make_shared<DataTypeString>(),
            AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN}
    };
    return sample_block;
}

inline String getAuxiliaryIndexSourcePartitionHash(const String & source_partition_id)
{
    SipHash hash;
    hash.update(source_partition_id);
    return getSipHash128AsHexString(hash);
}

inline String getAuxiliaryIndexPhysicalPartitionId(const String & source_partition_id)
{
    MergeTreePartition partition(Row{source_partition_id});
    return partition.getID(getAuxiliaryIndexPartitionKeySampleBlock());
}

inline MergeTreePartInfo markAsAuxiliaryIndexPartInfo(MergeTreePartInfo part_info)
{
    part_info.setKind(MergeTreePartInfo::Kind::AuxiliaryIndex);
    return part_info;
}

template <typename DataParts>
AuxiliaryIndexSourcePartitionRange getAuxiliaryIndexSourcePartitionRange(const DataParts & source_parts)
{
    std::optional<AuxiliaryIndexSourcePartitionRange> range;

    for (const auto & part : source_parts)
    {
        if (!part)
            continue;

        const String & source_partition_id = part->info.getPartitionId();
        if (!range)
        {
            range.emplace();
            range->source_partition_id = source_partition_id;
            range->source_partition_hash = getAuxiliaryIndexSourcePartitionHash(source_partition_id);
            range->min_block = part->info.min_block;
            range->max_block = part->info.max_block;
            range->mutation = part->info.mutation;
            continue;
        }

        if (range->source_partition_id != source_partition_id)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot build one materialized-index part from different source partitions: {} and {}",
                range->source_partition_id,
                source_partition_id);

        range->min_block = std::min(range->min_block, part->info.min_block);
        range->max_block = std::max(range->max_block, part->info.max_block);
        range->mutation = std::max(range->mutation, part->info.mutation);
    }

    if (!range)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot derive materialized-index source partition range from an empty source part set");

    return *range;
}

template <typename DataParts>
String makeAuxiliaryIndexBuildPartNameFromSourceParts(
    const DataParts & source_parts,
    MergeTreeDataFormatVersion format_version)
{
    const auto range = getAuxiliaryIndexSourcePartitionRange(source_parts);
    MergeTreePartInfo part_info(
        getAuxiliaryIndexPhysicalPartitionId(range.source_partition_id),
        range.min_block,
        range.max_block,
        /*level_=*/0,
        range.mutation);
    return part_info.getPartNameAndCheckFormat(format_version);
}

inline String makeAuxiliaryIndexCompactPartNameFromInfos(
    const std::vector<MergeTreePartInfo> & auxiliary_index_part_infos,
    MergeTreeDataFormatVersion format_version)
{
    std::optional<MergeTreePartInfo> compact_info;
    UInt32 max_level = 0;
    Int64 max_mutation = 0;

    for (const auto & part_info : auxiliary_index_part_infos)
    {
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
    return markAsAuxiliaryIndexPartInfo(*compact_info).getPartNameAndCheckFormat(format_version);
}

}
