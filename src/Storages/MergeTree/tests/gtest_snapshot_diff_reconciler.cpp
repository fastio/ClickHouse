#include <gtest/gtest.h>

#include <Storages/MaterializedIndex/CoverageMap.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartName.h>
#include <Storages/MaterializedIndex/MaterializedIndexSchedulerPolicy.h>
#include <Storages/MaterializedIndex/MaterializedIndexSchedulerState.h>
#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <chrono>

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

CoverageEntry coverageEntry(UUID source_uuid, UInt64 rows)
{
    CoverageEntry entry;
    entry.source_part_uuid = source_uuid;
    entry.rows = rows;
    return entry;
}

MergeTreePartInfo materializedIndexPartInfo(std::string_view suffix, Int64 min_block, Int64 max_block, UInt32 level)
{
    return MergeTreePartInfo(
        String{MergeTreePartInfo::MATERIALIZED_INDEX_PART_PREFIX} + String{suffix},
        min_block,
        max_block,
        level);
}

}


// SnapshotDiffReconciler::run takes IMergeTreeDataPart pointers. Constructing
// real parts in unit tests is prohibitively heavy because IMergeTreeDataPart
// requires a live MergeTreeData instance. The UUID-only overload runOnUuids
// preserves the same high-level decision logic, so the UUID-only cases are
// covered through it; the pointer-bearing overload is exercised end-to-end
// in the Pack 6 stateless cases.

TEST(MaterializedIndexPartNameTest, CompactPartNameCoversInputParts)
{
    const auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    std::vector<MergeTreePartInfo> inputs{
        materializedIndexPartInfo("build", 10, 10, 0),
        materializedIndexPartInfo("build", 11, 12, 2),
        materializedIndexPartInfo("build", 13, 13, 1),
    };

    const auto compact_name = makeMaterializedIndexCompactPartNameFromInfos(inputs, format_version);
    const auto compact_info = MergeTreePartInfo::fromPartName(compact_name, format_version);

    EXPECT_EQ(compact_info.getPartitionId(), inputs.front().getPartitionId());
    EXPECT_EQ(compact_info.min_block, 10);
    EXPECT_EQ(compact_info.max_block, 13);
    EXPECT_EQ(compact_info.level, 3);
    for (const auto & input : inputs)
        EXPECT_TRUE(compact_info.contains(input)) << compact_name << " does not cover " << input.getPartNameForLogs();
}

TEST(MaterializedIndexPartNameTest, CompactPartNameRejectsDifferentPartitions)
{
    const auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    std::vector<MergeTreePartInfo> inputs{
        materializedIndexPartInfo("build", 10, 10, 0),
        materializedIndexPartInfo("other", 11, 11, 0),
    };

    EXPECT_THROW(
        makeMaterializedIndexCompactPartNameFromInfos(inputs, format_version),
        DB::Exception);
}

TEST(SnapshotDiffReconcilerTest, EmptyMiSnapshotYieldsBuildCandidate)
{
    auto u1 = uuid(1, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {u1},
        /*materialized_index_snapshot_non_empty=*/false,
        /*coverage=*/{});

    EXPECT_TRUE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::BuildBatch);
    ASSERT_EQ(result.build_batch.source_part_uuids.size(), 1u);
    EXPECT_EQ(result.build_batch.source_part_uuids.front(), u1);
    EXPECT_TRUE(result.delta_out.empty());
}

TEST(SnapshotDiffReconcilerTest, DeltaInYieldsBuildBatch)
{
    auto covered = uuid(1, 0);
    auto fresh = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {covered, fresh},
        /*materialized_index_snapshot_non_empty=*/true,
        /*coverage=*/{covered});

    EXPECT_TRUE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::BuildBatch);
    ASSERT_EQ(result.build_batch.source_part_uuids.size(), 1u);
    EXPECT_EQ(result.build_batch.source_part_uuids.front(), fresh);
    EXPECT_TRUE(result.delta_out.empty());
}

TEST(SnapshotDiffReconcilerTest, DeltaOutYieldsObsoleteCoverage)
{
    auto vanished = uuid(1, 0);
    auto kept = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {kept},
        /*materialized_index_snapshot_non_empty=*/true,
        /*coverage=*/{vanished, kept});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::ObsoleteCoverage);
    ASSERT_EQ(result.delta_out.size(), 1u);
    EXPECT_EQ(result.delta_out.front(), vanished);
    ASSERT_EQ(result.obsolete_coverage.obsolete_source_part_uuids.size(), 1u);
    EXPECT_EQ(result.obsolete_coverage.obsolete_source_part_uuids.front(), vanished);
}

TEST(SnapshotDiffReconcilerTest, DeltaInTakesBuildBatchPriorityOverRemap)
{
    auto vanished = uuid(1, 0);
    auto covered = uuid(2, 0);
    auto fresh = uuid(3, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {covered, fresh},
        /*materialized_index_snapshot_non_empty=*/true,
        /*coverage=*/{vanished, covered});

    EXPECT_TRUE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::BuildBatch);
    ASSERT_EQ(result.build_batch.source_part_uuids.size(), 1u);
    EXPECT_EQ(result.build_batch.source_part_uuids.front(), fresh);
    ASSERT_EQ(result.delta_out.size(), 1u);
    EXPECT_EQ(result.delta_out.front(), vanished);
}

TEST(SnapshotDiffReconcilerTest, NoDiffIsNoop)
{
    auto u1 = uuid(1, 0);
    auto u2 = uuid(2, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {u1, u2},
        /*materialized_index_snapshot_non_empty=*/true,
        /*coverage=*/{u1, u2});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::Nothing);
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
    UUID materialized_index_a = uuid(0xA, 0);

    /// Round 1: empty coverage; materialized_index snapshot empty; expect build candidate.
    auto coverage_set = cov.coveredSourceUuids();
    bool materialized_index_present = false;
    auto round1 = SnapshotDiffReconciler::runOnUuids(
        {u1, u2, u3},
        materialized_index_present,
        coverage_set);
    EXPECT_TRUE(round1.has_build_candidate);
    EXPECT_FALSE(round1.has_remap_target);
    EXPECT_EQ(round1.candidate_kind, ReconcileCandidateKind::BuildBatch);

    /// Simulate Build commit: MaterializedIndexBuildTask::finish would write coverage.json
    /// for {u1, u2, u3} and call appendFromBuild on the same set.
    cov.appendFromBuild(
        materialized_index_a,
        {coverageEntry(u1, 100), coverageEntry(u2, 200), coverageEntry(u3, 300)});
    materialized_index_present = true;

    /// Round 2 & 3: with the freshly populated coverage, reconciler must
    /// see no work — no missing source UUIDs (delta_in empty) and no
    /// vanished UUIDs (delta_out empty), so neither has_build_candidate
    /// nor has_remap_target should fire.
    for (int round = 2; round <= 3; ++round)
    {
        auto cov_uuids = cov.coveredSourceUuids();
        auto result = SnapshotDiffReconciler::runOnUuids(
            {u1, u2, u3},
            materialized_index_present,
            cov_uuids);
        EXPECT_FALSE(result.has_build_candidate) << "round " << round;
        EXPECT_FALSE(result.has_remap_target) << "round " << round;
        EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::Nothing) << "round " << round;
        EXPECT_TRUE(result.delta_out.empty()) << "round " << round;
    }
}

TEST(MaterializedIndexSchedulerStateTest, BuildBatchReservationRejectsOverlapAndReleases)
{
    MaterializedIndexSchedulerState state;
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);

    EXPECT_TRUE(state.reserveBuildBatch("task_a", {source_a, source_b}, uuid(10, 0)));
    EXPECT_TRUE(state.isSourceReserved(source_a));
    EXPECT_TRUE(state.isSourceReserved(source_b));
    EXPECT_FALSE(state.reserveBuildBatch("task_b", {source_b}, uuid(11, 0)));

    state.releaseTask("task_a");
    EXPECT_FALSE(state.isSourceReserved(source_a));
    EXPECT_FALSE(state.isSourceReserved(source_b));
    EXPECT_TRUE(state.reserveBuildBatch("task_b", {source_b}, uuid(11, 0)));
}

TEST(MaterializedIndexSchedulerStateTest, RemapReservationRejectsOverlappingMiPartAndReleases)
{
    MaterializedIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);
    auto source_a = uuid(1, 0);

    EXPECT_TRUE(state.reserveRemapLineage("task_a", {mi_a, mi_b}, {source_a}, uuid(12, 0)));
    EXPECT_TRUE(state.isMiPartReserved(mi_a));
    EXPECT_TRUE(state.isMiPartReserved(mi_b));
    EXPECT_TRUE(state.isSourceReserved(source_a));
    EXPECT_FALSE(state.reserveRemapLineage("task_b", {mi_b}, {}, uuid(13, 0)));

    state.releaseTask("task_a");
    EXPECT_FALSE(state.isMiPartReserved(mi_a));
    EXPECT_FALSE(state.isMiPartReserved(mi_b));
    EXPECT_FALSE(state.isSourceReserved(source_a));
    EXPECT_TRUE(state.reserveRemapLineage("task_b", {mi_b}, {}, uuid(13, 0)));
}

TEST(MaterializedIndexSchedulerStateTest, CompactReservationDoesNotReserveCoveredSources)
{
    MaterializedIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);
    auto source_a = uuid(1, 0);

    EXPECT_TRUE(state.reserveCompactRebuild("compact_a", {mi_a, mi_b}, {source_a}, uuid(12, 0)));
    EXPECT_TRUE(state.isMiPartReserved(mi_a));
    EXPECT_TRUE(state.isMiPartReserved(mi_b));
    EXPECT_FALSE(state.isSourceReserved(source_a));
    EXPECT_FALSE(state.reserveCompactRebuild("compact_b", {mi_b}, {source_a}, uuid(13, 0)));

    state.releaseTask("compact_a");
    EXPECT_FALSE(state.isMiPartReserved(mi_a));
    EXPECT_FALSE(state.isMiPartReserved(mi_b));
    EXPECT_TRUE(state.reserveBuildBatch("build_a", {source_a}, uuid(14, 0)));
}

TEST(MaterializedIndexSchedulerStateTest, ReadyCoverageCanHavePendingCompact)
{
    MaterializedIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto source_a = uuid(1, 0);

    state.appendReadyCoverage(mi_a, {coverageEntry(source_a, 10)});
    EXPECT_TRUE(state.reserveCompactRebuild("compact_a", {mi_a}, {source_a}, uuid(11, 0)));
    EXPECT_TRUE(state.hasPendingTaskForSource(source_a));
    EXPECT_TRUE(state.isMiPartReservedBy(mi_a, "compact_a"));
    EXPECT_FALSE(state.isSourceReserved(source_a));

    state.releaseTask("compact_a");
    EXPECT_FALSE(state.hasPendingTaskForSource(source_a));
    EXPECT_EQ(state.readyMiPartCount(), 1u);
}

TEST(MaterializedIndexSchedulerPolicyTest, ObsoleteCoverageBeatsCompactFallback)
{
    ReconcileResult reconciled;
    auto vanished = uuid(1, 0);
    auto mi_a = uuid(10, 0);
    reconciled.candidate_kind = ReconcileCandidateKind::ObsoleteCoverage;
    reconciled.delta_out = {vanished};
    reconciled.obsolete_coverage.obsolete_source_part_uuids = {vanished};
    reconciled.obsolete_coverage.affected_materialized_index_part_uuids = {mi_a};
    reconciled.obsolete_coverage.affected_materialized_index_parts = {MergeTreeData::DataPartPtr{}};

    auto decision = MaterializedIndexSchedulerPolicy::choose(
        reconciled,
        {},
        /*compact_rebuild_candidate=*/true,
        {},
        {});

    EXPECT_EQ(decision.kind, MaterializedIndexSchedulerDecisionKind::ObsoleteCoverageCleanup);
    EXPECT_EQ(decision.delta_out_source_uuids, std::vector<UUID>{vanished});
}

TEST(MaterializedIndexSchedulerStateTest, ResourceBackoffIsObservableAndClearable)
{
    MaterializedIndexSchedulerState state;
    MaterializedIndexSchedulerState::BacklogStats stats;
    stats.rows = 10;
    stats.bytes = 20;
    stats.parts = 2;
    state.setBacklogStats(stats);

    state.postponeForResourceFailure("resource limit", std::chrono::seconds(60));
    EXPECT_TRUE(state.isResourceBackoffActive());

    auto snapshot = state.getObservabilitySnapshot();
    EXPECT_EQ(snapshot.backlog.rows, 10u);
    EXPECT_EQ(snapshot.backlog.bytes, 20u);
    EXPECT_EQ(snapshot.backlog.parts, 2u);
    EXPECT_EQ(snapshot.retry_count, 1u);
    EXPECT_EQ(snapshot.last_error, "resource limit");

    state.clearResourceBackoff();
    EXPECT_FALSE(state.isResourceBackoffActive());

    snapshot = state.getObservabilitySnapshot();
    EXPECT_EQ(snapshot.retry_count, 0u);
    EXPECT_TRUE(snapshot.last_error.empty());
    EXPECT_EQ(snapshot.next_retry_time, std::chrono::system_clock::time_point{});
}

TEST(MaterializedIndexSchedulerStateTest, TaskFailureBackoffIsClearableAndPruned)
{
    MaterializedIndexSchedulerState state;

    state.recordTaskFailure("task_a", "transient failure", std::chrono::seconds(60));
    EXPECT_TRUE(state.isTaskFailureBackoffActive("task_a"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 1u);

    state.clearTaskFailure("task_a");
    EXPECT_FALSE(state.isTaskFailureBackoffActive("task_a"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 0u);

    state.recordTaskFailure("expired_task", "expired failure", std::chrono::seconds(0));
    EXPECT_FALSE(state.isTaskFailureBackoffActive("expired_task"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 0u);
}
