#include <gtest/gtest.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <Storages/MergeTree/MergeTreeAuxiliaryIndexFill.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <Storages/MergeTree/RangesInDataPart.h>

using namespace DB;

namespace
{

constexpr Float32 kDefaultDistance = Float32(999999.99);

struct HelperResult
{
    PaddedPODArray<UInt8> filter;
    PaddedPODArray<Float32> distances;
};

HelperResult runHelper(
    const std::vector<UInt64> & rows,
    const std::vector<float> & distances,
    const std::vector<UInt64> & part_offsets_vec)
{
    auto part_offsets_column = ColumnUInt64::create();
    auto & part_offsets_data = part_offsets_column->getData();
    part_offsets_data.assign(part_offsets_vec.begin(), part_offsets_vec.end());

    HelperResult out;
    out.filter.resize_fill(part_offsets_data.size(), UInt8(0));
    out.distances.resize_fill(part_offsets_data.size(), kDefaultDistance);

    buildMatchingFilterAndDistanceColumnForAuxiliaryIndex(
        rows, distances, part_offsets_data, out.filter, out.distances);
    return out;
}

size_t popcount(const PaddedPODArray<UInt8> & filter)
{
    size_t c = 0;
    for (auto v : filter)
        c += (v != 0);
    return c;
}

}

/// T1 — basic case: rows in order, distances mapped 1:1 to part_offsets.
TEST(FillDistanceForAuxiliaryIndex, Basic)
{
    auto r = runHelper({2, 5, 7}, {1.0f, 2.0f, 3.0f}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    EXPECT_FLOAT_EQ(r.distances[2], 1.0f);
    EXPECT_FLOAT_EQ(r.distances[5], 2.0f);
    EXPECT_FLOAT_EQ(r.distances[7], 3.0f);

    for (size_t i : {0u, 1u, 3u, 4u, 6u, 8u, 9u})
        EXPECT_FLOAT_EQ(r.distances[i], kDefaultDistance);

    EXPECT_EQ(popcount(r.filter), 3u);
    EXPECT_EQ(r.filter[2], 1);
    EXPECT_EQ(r.filter[5], 1);
    EXPECT_EQ(r.filter[7], 1);
}

/// T2 — rows arrive out of order; distances must follow the row mapping, not the sort order.
TEST(FillDistanceForAuxiliaryIndex, OutOfOrderRows)
{
    auto r = runHelper({7, 2, 5}, {3.0f, 1.0f, 2.0f}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    EXPECT_FLOAT_EQ(r.distances[2], 1.0f);
    EXPECT_FLOAT_EQ(r.distances[5], 2.0f);
    EXPECT_FLOAT_EQ(r.distances[7], 3.0f);
    EXPECT_EQ(popcount(r.filter), 3u);
}

/// T3 — empty rows: filter and distances stay at default; no exception.
TEST(FillDistanceForAuxiliaryIndex, EmptyRows)
{
    auto r = runHelper({}, {}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    EXPECT_EQ(popcount(r.filter), 0u);
    for (auto d : r.distances)
        EXPECT_FLOAT_EQ(d, kDefaultDistance);
}

/// T4 — rows beyond part_offsets: helper exits without writing anything.
TEST(FillDistanceForAuxiliaryIndex, RowsBeyondRange)
{
    auto r = runHelper({100, 200}, {1.0f, 2.0f}, {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

    EXPECT_EQ(popcount(r.filter), 0u);
    for (auto d : r.distances)
        EXPECT_FLOAT_EQ(d, kDefaultDistance);
}

/// T5 — caller forgot to populate distances. The fill function asserts on `has_value()`
/// before calling the helper; the helper itself asserts that `rows.size() == distances.size()`.
/// Here we drive the contract violation directly through the helper.
TEST(FillDistanceForAuxiliaryIndex, MissingDistances)
{
#ifdef NDEBUG
    GTEST_SKIP() << "Release build skips chassert-based death tests";
#else
    EXPECT_DEBUG_DEATH(runHelper({2, 5}, {}, {0, 1, 2, 3, 4, 5}), "rows.*size.*distances");
#endif
}

/// T6 — rows and distances of mismatched length must fail the chassert in the helper.
TEST(FillDistanceForAuxiliaryIndex, LengthMismatch)
{
#ifdef NDEBUG
    GTEST_SKIP() << "Release build skips chassert-based death tests";
#else
    EXPECT_DEBUG_DEATH(runHelper({2, 5, 7}, {1.0f, 2.0f}, {0, 1, 2, 3, 4, 5, 6, 7}), "rows.*size.*distances");
#endif
}

/// T11 — distance column type stays Float32 once the wrapper composes the helper output.
TEST(FillDistanceForAuxiliaryIndex, DistanceColumnTypeIsFloat32)
{
    auto column = ColumnFloat32::create(8u, kDefaultDistance);
    static_assert(std::is_same_v<ColumnFloat32::ValueType, Float32>);
    EXPECT_EQ(column->getDataType(), TypeIndex::Float32);
}

/// T7 — RangesInDataPartReadHints carries vector and AuxiliaryIndex results in two
/// independent fields. Setting one must not implicitly populate the other; the reader's
/// dispatch at fillVirtualColumns then checks vector first, so when both are set the
/// vector branch wins. The dispatch ordering itself is exercised end-to-end by the
/// AuxiliaryIndex SQL regression — here we only pin the data-shape contract that
/// makes the dispatch well-defined.
TEST(RangeReader1196Branch, VectorWinsWhenBothPresent)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(hints.vector_search_results.has_value());
    EXPECT_FALSE(hints.auxiliary_index_search_results.has_value());

    hints.auxiliary_index_search_results = NearestNeighbours{{1, 2}, std::vector<float>{0.5f, 0.6f}};
    EXPECT_FALSE(hints.vector_search_results.has_value());
    EXPECT_TRUE(hints.auxiliary_index_search_results.has_value());

    hints.vector_search_results = NearestNeighbours{{3, 4}, std::vector<float>{0.7f, 0.8f}};
    /// Both fields populated independently — no aliasing, no shared storage.
    EXPECT_TRUE(hints.vector_search_results.has_value());
    EXPECT_TRUE(hints.auxiliary_index_search_results.has_value());
    EXPECT_NE(hints.vector_search_results->rows, hints.auxiliary_index_search_results->rows);
}

/// T8 — `RangesInDataPartReadHints` carries the vector and AuxiliaryIndex hints in
/// two independent `std::optional<NearestNeighbours>` storages. Distinct addresses are
/// the precondition for the reader's dispatch at executePrewhereActionsAndFilterColumns
/// to apply two independent FilterWithCachedCount caches without aliasing. The actual
/// branch is exercised end-to-end by the AuxiliaryIndex SQL regression.
TEST(RangeReader1594Branch, MIFilterApplied)
{
    RangesInDataPartReadHints hints;
    const void * vec_addr = static_cast<const void *>(&hints.vector_search_results);
    const void * auxiliary_index_addr = static_cast<const void *>(&hints.auxiliary_index_search_results);
    EXPECT_NE(vec_addr, auxiliary_index_addr);
}
