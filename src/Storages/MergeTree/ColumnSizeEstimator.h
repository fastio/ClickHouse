#pragma once

#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <map>
#include <vector>


namespace DB
{

/// Assign progress bytes for `with_key_columns` Map gathering columns.
/// `accumulateColumnSizes` only stores top-level names, so expanded `m.key_*` items would otherwise
/// get weight 0 and the Map bytes would drop out of the denominator.
/// Shared metadata (`keys_info`, template) stays inside the top-level Map size and is split across keys.
/// A key with no payload still gets 1 byte so default-filling work moves progress.
inline void addMapKeyColumnsGatheringSizes(
    std::map<String, UInt64> & column_to_size,
    const std::vector<String> & key_column_names,
    UInt64 total_map_bytes)
{
    if (key_column_names.empty())
        return;

    const UInt64 per_key = total_map_bytes / key_column_names.size();
    UInt64 remainder = total_map_bytes % key_column_names.size();
    for (const auto & name : key_column_names)
    {
        UInt64 size = per_key;
        if (remainder > 0)
        {
            ++size;
            --remainder;
        }
        if (size == 0)
            size = 1;
        column_to_size[name] = size;
    }
}

/* Allow to compute more accurate progress statistics */
class ColumnSizeEstimator
{
    using ColumnToSize = std::map<String, UInt64>;
    ColumnToSize map;
public:

    /// Stores approximate size of columns in bytes
    /// Exact values are not required since it used for relative values estimation (progress).
    size_t sum_total = 0;
    size_t sum_index_columns = 0;
    size_t sum_ordinary_columns = 0;

    ColumnSizeEstimator(ColumnToSize && map_, const NamesAndTypesList & key_columns, const NamesAndTypesList & ordinary_columns)
        : map(std::move(map_))
    {
        for (const auto & [name, _] : key_columns)
            if (!map.contains(name)) map[name] = 0;
        for (const auto & [name, _] : ordinary_columns)
            if (!map.contains(name)) map[name] = 0;

        for (const auto & [name, _] : key_columns)
            sum_index_columns += map.at(name);

        for (const auto & [name, _] : ordinary_columns)
            sum_ordinary_columns += map.at(name);

        sum_total = std::max(static_cast<decltype(sum_index_columns)>(1), sum_index_columns + sum_ordinary_columns);
    }

    Float64 columnWeight(const String & column) const
    {
        return static_cast<Float64>(map.at(column)) / static_cast<Float64>(sum_total);
    }

    Float64 keyColumnsWeight() const
    {
        return static_cast<Float64>(sum_index_columns) / static_cast<Float64>(sum_total);
    }
};
}
