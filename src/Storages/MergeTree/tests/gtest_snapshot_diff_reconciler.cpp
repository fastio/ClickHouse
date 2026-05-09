#include <gtest/gtest.h>

#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

using namespace DB;


namespace
{

UUID uuid(uint64_t lo, uint64_t hi)
{
    UUID u;
    UUIDHelpers::getLowBytes(u) = lo;
    UUIDHelpers::getHighBytes(u) = hi;
    return u;
}

}


// SnapshotDiffReconciler::run takes IMergeTreeDataPart pointers. Constructing
// real parts in unit tests is prohibitively heavy because IMergeTreeDataPart
// requires a live MergeTreeData instance. The UUID-only overload runOnUuids
// preserves the same decision logic, so the four cycle-decision cases are
// covered through it; the pointer-bearing overload is exercised end-to-end
// in the Pack 6 stateless cases.

TEST(SnapshotDiffReconcilerTest, EmptyMiSnapshotYieldsBuildCandidate)
{
    auto u1 = uuid(1, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {u1},
        /*mi_snapshot_non_empty=*/false,
        /*coverage=*/{});

    EXPECT_TRUE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_TRUE(result.delta_out.empty());
}

TEST(SnapshotDiffReconcilerTest, DeltaInYieldsRemapTarget)
{
    auto covered = uuid(1, 0);
    auto fresh = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {covered, fresh},
        /*mi_snapshot_non_empty=*/true,
        /*coverage=*/{covered});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_TRUE(result.has_remap_target);
    EXPECT_TRUE(result.delta_out.empty());
}

TEST(SnapshotDiffReconcilerTest, DeltaOutYieldsRemapTarget)
{
    auto vanished = uuid(1, 0);
    auto kept = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {kept},
        /*mi_snapshot_non_empty=*/true,
        /*coverage=*/{vanished, kept});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_TRUE(result.has_remap_target);
    ASSERT_EQ(result.delta_out.size(), 1u);
    EXPECT_EQ(result.delta_out.front(), vanished);
}

TEST(SnapshotDiffReconcilerTest, NoDiffIsNoop)
{
    auto u1 = uuid(1, 0);
    auto u2 = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {u1, u2},
        /*mi_snapshot_non_empty=*/true,
        /*coverage=*/{u1, u2});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_TRUE(result.delta_out.empty());
}
