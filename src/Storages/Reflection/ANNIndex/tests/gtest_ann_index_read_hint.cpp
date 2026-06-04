#include <Core/UUID.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

#include <vector>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

NearestNeighbours makeNearestNeighbours(std::vector<UInt64> rows, std::vector<float> distances)
{
    NearestNeighbours nn;
    nn.rows = std::move(rows);
    nn.distances = std::move(distances);
    return nn;
}

UUID makeUUID(UInt64 lo)
{
    UUID u;
    UUIDHelpers::getLowBytes(u) = lo;
    UUIDHelpers::getHighBytes(u) = lo + 1;
    return u;
}

RangesInDataPartReadHints emptyHints()
{
    return RangesInDataPartReadHints{};
}

}

TEST(ANNIndexReadHint, NonDistributed)
{
    UUID uuid_a = makeUUID(1);
    UUID uuid_b = makeUUID(2);
    UUID uuid_c = makeUUID(3);

    ANNIndexHints hints;
    hints.covered_source_parts = {uuid_a, uuid_b};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({10, 20, 30}, {0.1f, 0.2f, 0.3f});
    hints.hits_per_part[uuid_b] = makeNearestNeighbours({40, 50}, {0.4f, 0.5f});

    RangesInDataPartReadHints rh_a = emptyHints();
    RangesInDataPartReadHints rh_b = emptyHints();
    RangesInDataPartReadHints rh_c = emptyHints();

    attachANNIndexHintForPart(uuid_a, rh_a, hints);
    attachANNIndexHintForPart(uuid_b, rh_b, hints);
    attachANNIndexHintForPart(uuid_c, rh_c, hints);

    ASSERT_TRUE(rh_a.ann_index_search_results.has_value());
    EXPECT_EQ(rh_a.ann_index_search_results->rows, (std::vector<UInt64>{10, 20, 30}));
    ASSERT_TRUE(rh_a.ann_index_search_results->distances.has_value());
    EXPECT_EQ(rh_a.ann_index_search_results->distances->size(), 3u);

    ASSERT_TRUE(rh_b.ann_index_search_results.has_value());
    EXPECT_EQ(rh_b.ann_index_search_results->rows, (std::vector<UInt64>{40, 50}));

    EXPECT_FALSE(rh_c.ann_index_search_results.has_value());
    EXPECT_FALSE(rh_a.vector_search_results.has_value());
    EXPECT_FALSE(rh_b.vector_search_results.has_value());
}

TEST(ANNIndexReadHint, DistributedBranch)
{
    /// The distributed branch in ReadFromMergeTree calls the same kernel after
    /// std::move(result_parts_ranges); behavioural equivalence is guaranteed by reusing the same
    /// helper. Re-driving it here documents that intent and fails if the contract drifts.
    UUID uuid_a = makeUUID(7);
    UUID uuid_b = makeUUID(8);

    ANNIndexHints hints;
    hints.covered_source_parts = {uuid_a};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({100}, {0.01f});

    RangesInDataPartReadHints rh_a = emptyHints();
    RangesInDataPartReadHints rh_b = emptyHints();

    attachANNIndexHintForPart(uuid_a, rh_a, hints);
    attachANNIndexHintForPart(uuid_b, rh_b, hints);

    ASSERT_TRUE(rh_a.ann_index_search_results.has_value());
    EXPECT_EQ(rh_a.ann_index_search_results->rows, (std::vector<UInt64>{100}));
    EXPECT_FALSE(rh_b.ann_index_search_results.has_value());
}

TEST(ANNIndexReadHint, DoubleWriteAssert)
{
#ifdef NDEBUG
    GTEST_SKIP() << "chassert is compiled out in release";
#else
    UUID uuid_a = makeUUID(42);
    ANNIndexHints hints;
    hints.covered_source_parts = {uuid_a};
    hints.hits_per_part[uuid_a] = makeNearestNeighbours({1}, {0.0f});

    RangesInDataPartReadHints rh_a = emptyHints();
    attachANNIndexHintForPart(uuid_a, rh_a, hints);
    ASSERT_TRUE(rh_a.ann_index_search_results.has_value());

    EXPECT_DEATH(attachANNIndexHintForPart(uuid_a, rh_a, hints), "");
#endif
}

TEST(ANNIndexReadHint, MarkRangesForHintedOffsetsAreMerged)
{
    MergeTreeIndexGranularityConstant granularity(10);
    for (size_t i = 0; i < 10; ++i)
        granularity.appendMark(10);

    const auto ranges = buildMarkRangesForPartOffsetsForANNIndex(granularity, 100, "part", {35, 4, 5, 36, 99});

    ASSERT_EQ(ranges.size(), 3u);
    EXPECT_EQ(ranges[0], (MarkRange{0, 1}));
    EXPECT_EQ(ranges[1], (MarkRange{3, 4}));
    EXPECT_EQ(ranges[2], (MarkRange{9, 10}));
}

TEST(ANNIndexReadHint, CoveredPartWithoutHitsLeavesNoReaderEntry)
{
    /// Covered-but-zero-hits parts are signalled by `covered_source_parts`
    /// alone (no entry in `hits_per_part`). `pruneANNIndexRangesForPart`
    /// clears their ranges so the reader never sees them, and consequently
    /// `attachANNIndexHintForPart` deliberately writes nothing for them —
    /// any read-hint state would be dead.
    UUID uuid_covered_no_hits = makeUUID(100);
    UUID uuid_uncovered = makeUUID(101);

    ANNIndexHints hints;
    hints.covered_source_parts = {uuid_covered_no_hits};

    RangesInDataPartReadHints rh_covered = emptyHints();
    RangesInDataPartReadHints rh_uncovered = emptyHints();

    attachANNIndexHintForPart(uuid_covered_no_hits, rh_covered, hints);
    attachANNIndexHintForPart(uuid_uncovered, rh_uncovered, hints);

    EXPECT_FALSE(rh_covered.ann_index_search_results.has_value());
    EXPECT_FALSE(rh_uncovered.ann_index_search_results.has_value());
}
