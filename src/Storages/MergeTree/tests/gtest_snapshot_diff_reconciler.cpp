#include <gtest/gtest.h>

#include <Storages/MaterializedIndex/CoverageMap.h>
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

TEST(SnapshotDiffReconcilerTest, FeedsRealCoverageIsIdempotent)
{
    /// Three rounds of `runOnUuids` driven by a real `CoverageMap` fed with
    /// the manifest that a successful Build would commit. After round 1 the
    /// reconciler must report a no-op for rounds 2 and 3.
    auto u1 = uuid(1, 0);
    auto u2 = uuid(2, 0);
    auto u3 = uuid(3, 0);

    CoverageMap cov;
    UUID mi_a = uuid(0xA, 0);

    /// Round 1: empty coverage; mi snapshot empty; expect build candidate.
    auto coverage_set = cov.coveredSourceUuids();
    bool mi_present = false;
    auto round1 = SnapshotDiffReconciler::runOnUuids(
        {u1, u2, u3},
        mi_present,
        coverage_set);
    EXPECT_TRUE(round1.has_build_candidate);
    EXPECT_FALSE(round1.has_remap_target);

    /// Simulate Build commit: BuildTask::finish would write coverage.json
    /// for {u1, u2, u3} and call appendFromBuild on the same set.
    cov.appendFromBuild(mi_a, {{u1, 100}, {u2, 200}, {u3, 300}});
    mi_present = true;

    /// Round 2 & 3: with the freshly populated coverage, reconciler must
    /// see no work — no missing source UUIDs (delta_in empty) and no
    /// vanished UUIDs (delta_out empty), so neither has_build_candidate
    /// nor has_remap_target should fire.
    for (int round = 2; round <= 3; ++round)
    {
        auto cov_uuids = cov.coveredSourceUuids();
        auto result = SnapshotDiffReconciler::runOnUuids(
            {u1, u2, u3},
            mi_present,
            cov_uuids);
        EXPECT_FALSE(result.has_build_candidate) << "round " << round;
        EXPECT_FALSE(result.has_remap_target) << "round " << round;
        EXPECT_TRUE(result.delta_out.empty()) << "round " << round;
    }
}
