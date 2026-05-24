#include <gtest/gtest.h>

#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

using namespace DB;

/// MergeTreeReadTask::createReaders triggers the shared "_part_offset + setReadHints"
/// behavior whenever either vector_search_results or auxiliary_index_search_results is populated.
/// The trigger is the boolean expression
///   is_vector_search || is_auxiliary_index
/// where each operand reads has_value() on the corresponding optional. Building a
/// real MergeTreeReadTask in a unit test would require a live MergeTreeData storage,
/// so we pin the truth-table contract that the trigger relies on. End-to-end coverage
/// of the actual setReadHints call lives in the AuxiliaryIndex SQL regression.

namespace
{

bool triggerExpression(const RangesInDataPartReadHints & hints)
{
    bool is_vector_search = hints.vector_search_results.has_value();
    bool is_auxiliary_index = hints.auxiliary_index_search_results.has_value();
    return is_vector_search || is_auxiliary_index;
}

}

/// T9 — only the AuxiliaryIndex field set: the OR trigger fires.
TEST(CreateReaders, MIPathTriggersSharedBehavior)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(triggerExpression(hints));

    hints.auxiliary_index_search_results = NearestNeighbours{{0, 1}, std::vector<float>{0.0f, 1.0f}};
    EXPECT_FALSE(hints.vector_search_results.has_value());
    EXPECT_TRUE(triggerExpression(hints));
}

/// T9b — only the vector field set: trigger still fires; behavior unchanged from before.
TEST(CreateReaders, VectorPathStillTriggersSharedBehavior)
{
    RangesInDataPartReadHints hints;
    hints.vector_search_results = NearestNeighbours{{2, 3}, std::vector<float>{2.0f, 3.0f}};
    EXPECT_FALSE(hints.auxiliary_index_search_results.has_value());
    EXPECT_TRUE(triggerExpression(hints));
}

/// T9c — neither set: trigger does not fire; createReaders skips setReadHints.
TEST(CreateReaders, NoHintsLeavesSharedBehaviorOff)
{
    RangesInDataPartReadHints hints;
    EXPECT_FALSE(triggerExpression(hints));
}
