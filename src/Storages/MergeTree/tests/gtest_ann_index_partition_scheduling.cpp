#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/ANNIndexPartitionScheduling.h>


using namespace DB;

namespace
{

UUID makeUuid(UInt64 value)
{
    UUID uuid;
    uuid.toUnderType().items[0] = value;
    return uuid;
}

BuildBatchCandidateView makeCandidate(UInt64 uuid, Int64 block, UInt64 rows)
{
    return BuildBatchCandidateView{
        .source_part_uuid = makeUuid(uuid),
        .source_partition_id = "p",
        .min_block = block,
        .max_block = block,
        .rows = rows,
        .bytes = rows,
        .first_seen = std::chrono::steady_clock::time_point{} + std::chrono::seconds(uuid),
    };
}

}

TEST(ANNIndexPartitionScheduling, StopsContiguousBatchAtMaxRows)
{
    auto selection = pickContiguousBatchInOldestPartition(
        {
            makeCandidate(1, 1, 4),
            makeCandidate(2, 2, 4),
            makeCandidate(3, 3, 4),
        },
        8);

    ASSERT_EQ(selection.picked_uuids.size(), 2u);
    EXPECT_EQ(selection.picked_uuids[0], makeUuid(1));
    EXPECT_EQ(selection.picked_uuids[1], makeUuid(2));
    EXPECT_EQ(selection.rows, 8u);
}

TEST(ANNIndexPartitionScheduling, KeepsSingleOversizedPartAtomic)
{
    auto selection = pickContiguousBatchInOldestPartition(
        {
            makeCandidate(1, 1, 12),
            makeCandidate(2, 2, 1),
        },
        8);

    ASSERT_EQ(selection.picked_uuids.size(), 1u);
    EXPECT_EQ(selection.picked_uuids[0], makeUuid(1));
    EXPECT_EQ(selection.rows, 12u);
}
