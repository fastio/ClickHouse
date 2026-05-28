#include <Storages/MergeTree/MergeTreeANNIndexFill.h>

#include <Common/Exception.h>
#include <base/defines.h>

#include <algorithm>
#include <unordered_map>


namespace DB
{

void buildMatchingFilterAndDistanceColumnForANNIndex(
    const std::vector<UInt64> & row_offsets_from_index,
    const std::vector<float> & distances_from_index,
    const ColumnUInt64::Container & part_offsets,
    IColumn::Filter & filter,
    ColumnFloat32::Container & distances_out)
{
    chassert(row_offsets_from_index.size() == distances_from_index.size());
    chassert(filter.size() == part_offsets.size());
    chassert(distances_out.size() == part_offsets.size());

    std::unordered_map<UInt64, Float32> offset_to_distance;
    offset_to_distance.reserve(row_offsets_from_index.size());
    for (size_t i = 0; i < distances_from_index.size(); ++i)
        offset_to_distance[row_offsets_from_index[i]] = distances_from_index[i];

    std::vector<UInt64> sorted_rows = row_offsets_from_index;
    std::sort(sorted_rows.begin(), sorted_rows.end());

    size_t j = 0;
    for (size_t i = 0; i < part_offsets.size(); ++i)
    {
        while (j < sorted_rows.size() && part_offsets[i] > sorted_rows[j])
            j++;

        if (j >= sorted_rows.size())
            break;

        if (part_offsets[i] == sorted_rows[j])
        {
            filter[i] = true;
            distances_out[i] = offset_to_distance[part_offsets[i]];
            j++;
        }
    }
}

}
