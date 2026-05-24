#pragma once

#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <base/types.h>

#include <vector>


namespace DB
{

/// Matching algorithm shared by MergeTreeRangeReader's AuxiliaryIndex fill function
/// and its unit tests. Pulled out of MergeTreeRangeReader.cpp into its own translation
/// unit so the algorithm is reachable from gtest without dragging the whole reader along
/// (and without having to friend the test harness).
///
/// The helper is intentionally AuxiliaryIndex-only. The vector_similarity_index variant
/// keeps its inlined copy of the same algorithm because the two paths must remain
/// physically independent — see the design notes around `auxiliary_index_search_results`.
///
/// Inputs:
///   row_offsets_from_index    rows returned by the AuxiliaryIndex search.
///   distances_from_index      distances aligned 1:1 with row_offsets_from_index.
///   part_offsets              auto-generated _part_offset values for the current block.
/// Outputs:
///   filter[i]                 set to 1 iff part_offsets[i] matches an entry in rows.
///   distances_out[i]          set to the matching distance for matched rows; otherwise
///                             left at the caller-provided default.
///
/// Preconditions (asserted with chassert):
///   row_offsets_from_index.size() == distances_from_index.size()
///   filter.size() == part_offsets.size()
///   distances_out.size() == part_offsets.size()
void buildMatchingFilterAndDistanceColumnForAuxiliaryIndex(
    const std::vector<UInt64> & row_offsets_from_index,
    const std::vector<float> & distances_from_index,
    const ColumnUInt64::Container & part_offsets,
    IColumn::Filter & filter,
    ColumnFloat32::Container & distances_out);

}
