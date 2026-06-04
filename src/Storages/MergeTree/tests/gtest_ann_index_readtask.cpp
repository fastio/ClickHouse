#include <gtest/gtest.h>

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

using namespace DB;

/// MergeTreeReadTask::createReaders triggers the shared "_part_offset + setReadHints"
/// behavior whenever nearest-neighbour read hints are populated. Building a real
/// MergeTreeReadTask in a unit test would require a live MergeTreeData storage,
/// so we pin the truth-table contract that the trigger relies on. End-to-end
/// coverage of the actual setReadHints call lives in the ANNIndex SQL regression.

namespace
{

bool triggerExpression(const RangesInDataPartReadHints & hints)
{
    return hints.hasNearestNeighbourSearchResults();
}

}

/// T9 — only the ANNIndex field set: the OR trigger fires.
TEST(CreateReaders, MIPathTriggersSharedBehavior)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(triggerExpression(hints));

    hints.ann_index_search_results = NearestNeighbours{{0, 1}, std::vector<float>{0.0f, 1.0f}};
    EXPECT_FALSE(hints.vector_search_results.has_value());
    EXPECT_TRUE(triggerExpression(hints));
}

/// T9b — only the vector field set: trigger still fires; behavior unchanged from before.
TEST(CreateReaders, VectorPathStillTriggersSharedBehavior)
{
    RangesInDataPartReadHints hints;
    hints.vector_search_results = NearestNeighbours{{2, 3}, std::vector<float>{2.0f, 3.0f}};
    EXPECT_FALSE(hints.ann_index_search_results.has_value());
    EXPECT_TRUE(triggerExpression(hints));
}

/// T9c — neither set: trigger does not fire; createReaders skips setReadHints.
TEST(CreateReaders, NoHintsLeavesSharedBehaviorOff)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(triggerExpression(hints));
}

TEST(CreateReaders, EmptyANNIndexHitsStillCarryRowFilterContract)
{
    RangesInDataPartReadHints hints;
    hints.ann_index_search_results = NearestNeighbours{{}, std::vector<float>{}};

    EXPECT_TRUE(triggerExpression(hints));
    ASSERT_NE(hints.getNearestNeighbourSearchResults(), nullptr);
    EXPECT_TRUE(hints.getNearestNeighbourSearchResults()->rows.empty());
}

TEST(CreateReaders, DistanceColumnIsNotRequiredForNearestNeighbourHints)
{
    RangesInDataPartReadHints hints;
    hints.ann_index_search_results = NearestNeighbours{{10}, std::vector<float>{0.5f}};

    const auto * nearest_neighbours = hints.getNearestNeighbourSearchResults();
    ASSERT_NE(nearest_neighbours, nullptr);
    EXPECT_EQ(nearest_neighbours->rows, std::vector<UInt64>{10});
    ASSERT_TRUE(nearest_neighbours->distances.has_value());
    EXPECT_FLOAT_EQ(nearest_neighbours->distances->at(0), 0.5f);
}
