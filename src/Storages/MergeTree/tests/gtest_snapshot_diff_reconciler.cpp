#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/CoverageMap.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSchedulerPolicy.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSchedulerState.h>
#include <Storages/Reflection/ANNIndex/SnapshotDiffReconciler.h>

#include <chrono>
#include <unordered_map>
#include <unordered_set>

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

CoverageEntry coverageEntryWithPartInfo(
    UUID source_uuid,
    UInt64 rows,
    String partition_id,
    Int64 min_block,
    Int64 max_block,
    UInt32 level,
    Int64 mutation)
{
    CoverageEntry entry;
    entry.source_part_uuid = source_uuid;
    entry.rows = rows;
    entry.source_part_name = partition_id + "_" + std::to_string(min_block) + "_" + std::to_string(max_block);
    entry.partition_id = std::move(partition_id);
    entry.min_block = min_block;
    entry.max_block = max_block;
    entry.level = level;
    entry.mutation = mutation;
    entry.has_part_info = true;
    return entry;
}

ReconcileSourcePart sourcePartView(
    UUID source_uuid,
    String partition_id,
    Int64 min_block,
    Int64 max_block,
    UInt32 level,
    Int64 mutation,
    bool indexed_columns_unchanged_by_mutation = false)
{
    ReconcileSourcePart part;
    part.uuid = source_uuid;
    part.partition_id = std::move(partition_id);
    part.min_block = min_block;
    part.max_block = max_block;
    part.level = level;
    part.mutation = mutation;
    part.rows = static_cast<UInt64>(max_block - min_block + 1);
    part.has_part_info = true;
    part.indexed_columns_unchanged_by_mutation = indexed_columns_unchanged_by_mutation;
    return part;
}

MergeTreePartInfo materializedIndexPartInfo(std::string_view suffix, Int64 min_block, Int64 max_block, UInt32 level)
{
    return markAsANNIndexPartInfo(MergeTreePartInfo(String{suffix}, min_block, max_block, level));
}

}


// SnapshotDiffReconciler::run takes IMergeTreeDataPart pointers. Constructing
// real parts in unit tests is prohibitively heavy because IMergeTreeDataPart
// requires a live MergeTreeData instance. The UUID-only overload runOnUuids
// preserves the same high-level decision logic, so the UUID-only cases are
// covered through it; the pointer-bearing overload is exercised end-to-end
// in the Pack 6 stateless cases.

TEST(ANNIndexPartNameTest, CompactPartNameCoversInputParts)
{
    const auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    std::vector<MergeTreePartInfo> inputs{
        materializedIndexPartInfo("build", 10, 10, 0),
        materializedIndexPartInfo("build", 11, 12, 2),
        materializedIndexPartInfo("build", 13, 13, 1),
    };

    const auto compact_name = makeANNIndexCompactPartNameFromInfos(inputs, format_version);
    const auto compact_info = MergeTreePartInfo::fromPartName(compact_name, format_version);

    EXPECT_EQ(compact_info.getPartitionId(), inputs.front().getPartitionId());
    EXPECT_EQ(compact_info.min_block, 10);
    EXPECT_EQ(compact_info.max_block, 13);
    EXPECT_EQ(compact_info.level, 3);
    for (const auto & input : inputs)
        EXPECT_TRUE(compact_info.contains(input)) << compact_name << " does not cover " << input.getPartNameForLogs();
}

TEST(ANNIndexPartNameTest, CompactPartNameRejectsDifferentPartitions)
{
    const auto format_version = MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING;
    std::vector<MergeTreePartInfo> inputs{
        materializedIndexPartInfo("build", 10, 10, 0),
        materializedIndexPartInfo("other", 11, 11, 0),
    };

    EXPECT_THROW(
        makeANNIndexCompactPartNameFromInfos(inputs, format_version),
        DB::Exception);
}

TEST(ANNIndexPartNameTest, PhysicalPartitionIdUsesMergeTreePartitionSemantics)
{
    const String source_partition_id = "tenant_2026_05";
    const auto physical_partition_id = getANNIndexPhysicalPartitionId(source_partition_id);
    MergeTreePartition expected_partition(Row{source_partition_id});

    EXPECT_EQ(physical_partition_id, expected_partition.getID(getANNIndexPartitionKeySampleBlock()));
    EXPECT_EQ(physical_partition_id.find(source_partition_id), String::npos);
    EXPECT_EQ(physical_partition_id, getANNIndexPhysicalPartitionId(source_partition_id));
    EXPECT_NE(physical_partition_id, getANNIndexPhysicalPartitionId("tenant_2026_06"));
}

TEST(SnapshotDiffReconcilerTest, EmptyMiSnapshotYieldsBuildCandidate)
{
    auto u1 = uuid(1, 0);
    auto result = SnapshotDiffReconciler::runOnUuids(
        {u1},
        /*ann_index_snapshot_non_empty=*/false,
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
        /*ann_index_snapshot_non_empty=*/true,
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
        /*ann_index_snapshot_non_empty=*/true,
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
        /*ann_index_snapshot_non_empty=*/true,
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
        /*ann_index_snapshot_non_empty=*/true,
        /*coverage=*/{u1, u2});

    EXPECT_FALSE(result.has_build_candidate);
    EXPECT_FALSE(result.has_remap_target);
    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::Nothing);
    EXPECT_TRUE(result.delta_out.empty());
}

TEST(SnapshotDiffReconcilerTest, CoverageCommitValueCountsActiveRows)
{
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);

    auto value = SnapshotDiffReconciler::evaluateCoverageCommitValue(
        {
            sourcePartView(source_a, "p", 1, 1, 0, 0),
            sourcePartView(source_b, "p", 2, 2, 0, 0),
        },
        {
            coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0),
            coverageEntryWithPartInfo(source_b, 20, "p", 2, 2, 0, 0),
        });

    EXPECT_EQ(value.total_rows, 30);
    EXPECT_EQ(value.active_rows, 30);
    EXPECT_EQ(value.remappable_stale_rows, 0);
    EXPECT_EQ(value.valuableRows(), 30);
    EXPECT_EQ(value.valuableRatioPercent(), 100);
}

TEST(SnapshotDiffReconcilerTest, CoverageCommitValueCountsMergeRemappableStaleRows)
{
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);
    auto source_c = uuid(3, 0);

    auto value = SnapshotDiffReconciler::evaluateCoverageCommitValue(
        {sourcePartView(source_c, "p", 1, 2, 1, 0)},
        {
            coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0),
            coverageEntryWithPartInfo(source_b, 20, "p", 2, 2, 0, 0),
        });

    EXPECT_EQ(value.total_rows, 30);
    EXPECT_EQ(value.active_rows, 0);
    EXPECT_EQ(value.remappable_stale_rows, 30);
    EXPECT_EQ(value.valuableRows(), 30);
    EXPECT_EQ(value.valuableRatioPercent(), 100);
}

TEST(SnapshotDiffReconcilerTest, CoverageCommitValueRejectsIncompleteStaleLineage)
{
    auto source_a = uuid(1, 0);
    auto source_c = uuid(3, 0);

    auto value = SnapshotDiffReconciler::evaluateCoverageCommitValue(
        {sourcePartView(source_c, "p", 1, 2, 1, 0)},
        {coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0)});

    EXPECT_EQ(value.total_rows, 10);
    EXPECT_EQ(value.active_rows, 0);
    EXPECT_EQ(value.remappable_stale_rows, 0);
    EXPECT_EQ(value.valuableRows(), 0);
    EXPECT_EQ(value.valuableRatioPercent(), 0);
}

TEST(SnapshotDiffReconcilerTest, CoverageCommitValueMixesActiveAndStaleRows)
{
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);
    auto source_c = uuid(3, 0);
    auto source_d = uuid(4, 0);

    auto value = SnapshotDiffReconciler::evaluateCoverageCommitValue(
        {
            sourcePartView(source_a, "p", 1, 1, 0, 0),
            sourcePartView(source_d, "p", 2, 3, 1, 0),
        },
        {
            coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0),
            coverageEntryWithPartInfo(source_b, 20, "p", 2, 2, 0, 0),
            coverageEntryWithPartInfo(source_c, 30, "p", 3, 3, 0, 0),
        });

    EXPECT_EQ(value.total_rows, 60);
    EXPECT_EQ(value.active_rows, 10);
    EXPECT_EQ(value.remappable_stale_rows, 50);
    EXPECT_EQ(value.valuableRows(), 60);
    EXPECT_EQ(value.valuableRatioPercent(), 100);
}

TEST(SnapshotDiffReconcilerTest, CoveredMergeLineageYieldsMergeRemap)
{
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);
    auto source_c = uuid(3, 0);
    auto mi_a = uuid(10, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a,
         {
             coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0),
             coverageEntryWithPartInfo(source_b, 10, "p", 2, 2, 0, 0),
         }},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(source_c, "p", 1, 2, 1, 0)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::RemapLineage);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::MergeLineage);
    EXPECT_EQ(result.remap_lineage.remap_kind, ANNIndexRemapKind::MergeLineage);
    EXPECT_EQ(result.remap_lineage.new_source_part_uuid, source_c);
    EXPECT_EQ(result.remap_lineage.old_ann_index_part_uuids, std::vector<UUID>{mi_a});
    EXPECT_EQ(result.build_batch.source_part_uuids.size(), 0u);
}

TEST(SnapshotDiffReconcilerTest, MergeLineageSelectsMultipleMiPartsCollectively)
{
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);
    auto source_c = uuid(3, 0);
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a, {coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0)}},
        {mi_b, {coverageEntryWithPartInfo(source_b, 20, "p", 2, 2, 0, 0)}},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(source_c, "p", 1, 2, 1, 0)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::RemapLineage);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::MergeLineage);
    EXPECT_EQ(result.remap_lineage.remap_kind, ANNIndexRemapKind::MergeLineage);
    EXPECT_EQ(result.remap_lineage.new_source_part_uuid, source_c);

    std::unordered_set<UUID> selected(
        result.remap_lineage.old_ann_index_part_uuids.begin(),
        result.remap_lineage.old_ann_index_part_uuids.end());
    EXPECT_EQ(selected.size(), 2u);
    EXPECT_TRUE(selected.contains(mi_a));
    EXPECT_TRUE(selected.contains(mi_b));
    EXPECT_EQ(result.build_batch.source_part_uuids.size(), 0u);
}

TEST(SnapshotDiffReconcilerTest, MergeLineageRejectsMixedCoveredAndUncoveredPredecessors)
{
    auto source_a = uuid(1, 0);
    auto source_c = uuid(3, 0);
    auto mi_a = uuid(10, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a, {coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0)}},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(source_c, "p", 1, 2, 1, 0)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::RebuildSourcePart);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::None);
    EXPECT_EQ(result.rebuild_source_part.source_part_uuid, source_c);
    EXPECT_EQ(result.rebuild_source_part.affected_ann_index_part_uuids, std::vector<UUID>{mi_a});
}

TEST(SnapshotDiffReconcilerTest, SafeMutationLineageYieldsMutationRemap)
{
    auto source_a = uuid(1, 0);
    auto source_a_mutated = uuid(2, 0);
    auto mi_a = uuid(10, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a, {coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0)}},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(source_a_mutated, "p", 1, 1, 0, 1, /*indexed_columns_unchanged_by_mutation=*/true)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::RemapLineage);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::MutationLineage);
    EXPECT_EQ(result.remap_lineage.remap_kind, ANNIndexRemapKind::MutationLineage);
    EXPECT_EQ(result.remap_lineage.new_source_part_uuid, source_a_mutated);
    EXPECT_EQ(result.remap_lineage.old_source_part_uuids, std::vector<UUID>{source_a});
}

TEST(SnapshotDiffReconcilerTest, MutationLineageRejectsIndexedColumnMutation)
{
    auto source_a = uuid(1, 0);
    auto source_a_mutated = uuid(2, 0);
    auto mi_a = uuid(10, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a, {coverageEntryWithPartInfo(source_a, 10, "p", 1, 1, 0, 0)}},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(source_a_mutated, "p", 1, 1, 0, 1, /*indexed_columns_unchanged_by_mutation=*/false)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::RebuildSourcePart);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::None);
    EXPECT_EQ(result.rebuild_source_part.source_part_uuid, source_a_mutated);
}

TEST(SnapshotDiffReconcilerTest, PureDeltaOutYieldsObsoleteCoverageCleanup)
{
    auto vanished = uuid(1, 0);
    auto kept = uuid(2, 0);
    auto mi_a = uuid(10, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_a,
         {
             coverageEntryWithPartInfo(vanished, 10, "p", 1, 1, 0, 0),
             coverageEntryWithPartInfo(kept, 10, "p", 2, 2, 0, 0),
         }},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {sourcePartView(kept, "p", 2, 2, 0, 0)},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::ObsoleteCoverage);
    EXPECT_EQ(result.remap_kind, ANNIndexRemapKind::ObsoleteCoverageCleanup);
    EXPECT_EQ(result.obsolete_coverage.obsolete_source_part_uuids, std::vector<UUID>{vanished});
    EXPECT_EQ(result.obsolete_coverage.affected_ann_index_part_uuids, std::vector<UUID>{mi_a});
}

TEST(SnapshotDiffReconcilerTest, ObsoleteCoverageCleanupIsScopedToOneSourcePartition)
{
    auto vanished_p1 = uuid(1, 0);
    auto vanished_p2 = uuid(2, 0);
    auto vanished_q1 = uuid(3, 0);
    auto mi_p = uuid(10, 0);
    auto mi_q = uuid(11, 0);

    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_mi{
        {mi_p,
         {
             coverageEntryWithPartInfo(vanished_p1, 100, "p", 1, 1, 0, 0),
             coverageEntryWithPartInfo(vanished_p2, 100, "p", 2, 2, 0, 0),
         }},
        {mi_q,
         {
             coverageEntryWithPartInfo(vanished_q1, 10, "q", 1, 1, 0, 0),
         }},
    };

    auto result = SnapshotDiffReconciler::runOnPartViews(
        {},
        /*ann_index_snapshot_non_empty=*/true,
        coverage_by_mi);

    EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::ObsoleteCoverage);
    std::unordered_set<UUID> obsolete(
        result.obsolete_coverage.obsolete_source_part_uuids.begin(),
        result.obsolete_coverage.obsolete_source_part_uuids.end());
    EXPECT_EQ(obsolete.size(), 2u);
    EXPECT_TRUE(obsolete.contains(vanished_p1));
    EXPECT_TRUE(obsolete.contains(vanished_p2));
    EXPECT_FALSE(obsolete.contains(vanished_q1));

    ASSERT_EQ(result.obsolete_coverage.affected_ann_index_part_uuids.size(), 1u);
    EXPECT_EQ(result.obsolete_coverage.affected_ann_index_part_uuids.front(), mi_p);
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
    UUID ann_index_a = uuid(0xA, 0);

    /// Round 1: empty coverage; ann_index snapshot empty; expect build candidate.
    auto coverage_set = cov.coveredSourceUuids();
    bool ann_index_present = false;
    auto round1 = SnapshotDiffReconciler::runOnUuids(
        {u1, u2, u3},
        ann_index_present,
        coverage_set);
    EXPECT_TRUE(round1.has_build_candidate);
    EXPECT_FALSE(round1.has_remap_target);
    EXPECT_EQ(round1.candidate_kind, ReconcileCandidateKind::BuildBatch);

    /// Simulate Build commit: `ANNIndex::BuildTask::finish` would write coverage.json
    /// for {u1, u2, u3} and call appendFromBuild on the same set.
    cov.appendFromBuild(
        ann_index_a,
        {coverageEntry(u1, 100), coverageEntry(u2, 200), coverageEntry(u3, 300)});
    ann_index_present = true;

    /// Round 2 & 3: with the freshly populated coverage, reconciler must
    /// see no work — no missing source UUIDs (delta_in empty) and no
    /// vanished UUIDs (delta_out empty), so neither has_build_candidate
    /// nor has_remap_target should fire.
    for (int round = 2; round <= 3; ++round)
    {
        auto cov_uuids = cov.coveredSourceUuids();
        auto result = SnapshotDiffReconciler::runOnUuids(
            {u1, u2, u3},
            ann_index_present,
            cov_uuids);
        EXPECT_FALSE(result.has_build_candidate) << "round " << round;
        EXPECT_FALSE(result.has_remap_target) << "round " << round;
        EXPECT_EQ(result.candidate_kind, ReconcileCandidateKind::Nothing) << "round " << round;
        EXPECT_TRUE(result.delta_out.empty()) << "round " << round;
    }
}

TEST(ANNIndexSchedulerStateTest, BuildBatchReservationRejectsOverlapAndReleases)
{
    ANNIndexSchedulerState state;
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

TEST(ANNIndexSchedulerStateTest, BuildBatchReservationAllowsDisjointConcurrentBuilds)
{
    ANNIndexSchedulerState state;
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);
    auto source_c = uuid(3, 0);

    EXPECT_TRUE(state.reserveBuildBatch("task_a", {source_a}, uuid(10, 0)));
    EXPECT_TRUE(state.reserveBuildBatch("task_b", {source_b}, uuid(11, 0)));
    EXPECT_EQ(state.pendingTaskCount(), 2u);
    EXPECT_FALSE(state.hasActiveNonBuildTasks());

    EXPECT_FALSE(state.reserveBuildBatch("task_c", {source_a, source_c}, uuid(12, 0)));
    EXPECT_FALSE(state.isSourceReserved(source_c));

    state.releaseTask("task_a");
    EXPECT_TRUE(state.reserveBuildBatch("task_c", {source_a, source_c}, uuid(12, 0)));
    EXPECT_EQ(state.pendingTaskCount(), 2u);
}

TEST(ANNIndexSchedulerStateTest, NonBuildTaskIsObservableForExclusiveScheduling)
{
    ANNIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto source_a = uuid(1, 0);

    EXPECT_FALSE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::RemapLineage));
    EXPECT_FALSE(state.hasActiveNonBuildTasks());
    EXPECT_TRUE(state.reserveRemapLineage("remap_a", {mi_a}, {source_a}, uuid(11, 0)));
    EXPECT_TRUE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::RemapLineage));
    EXPECT_TRUE(state.hasActiveNonBuildTasks());

    state.releaseTask("remap_a");
    EXPECT_FALSE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::RemapLineage));
    EXPECT_FALSE(state.hasActiveNonBuildTasks());
}

TEST(ANNIndexSchedulerStateTest, BuildAndRemapReservationsCanCoexist)
{
    ANNIndexSchedulerState state;
    auto build_source = uuid(1, 0);
    auto remap_source = uuid(2, 0);
    auto mi_a = uuid(10, 0);

    EXPECT_TRUE(state.reserveBuildBatch("build_a", {build_source}, uuid(11, 0)));
    EXPECT_TRUE(state.reserveRemapLineage("remap_a", {mi_a}, {remap_source}, uuid(12, 0)));
    EXPECT_EQ(state.pendingTaskCount(), 2u);
    EXPECT_TRUE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::BuildBatch));
    EXPECT_TRUE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::RemapLineage));

    auto active_build_sources = state.activeBuildSourceUuids();
    EXPECT_EQ(active_build_sources.size(), 1u);
    EXPECT_TRUE(active_build_sources.contains(build_source));
    EXPECT_FALSE(active_build_sources.contains(remap_source));
}

TEST(ANNIndexSchedulerStateTest, RemapReservationRejectsOverlappingMiPartAndReleases)
{
    ANNIndexSchedulerState state;
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

TEST(ANNIndexSchedulerStateTest, RemapReservationAllowsDisjointConcurrentRemaps)
{
    ANNIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);
    auto source_a = uuid(1, 0);
    auto source_b = uuid(2, 0);

    EXPECT_TRUE(state.reserveRemapLineage("task_a", {mi_a}, {source_a}, uuid(12, 0)));
    EXPECT_TRUE(state.reserveRemapLineage("task_b", {mi_b}, {source_b}, uuid(13, 0)));
    EXPECT_EQ(state.pendingTaskCount(), 2u);
    EXPECT_TRUE(state.isMiPartReservedBy(mi_a, "task_a"));
    EXPECT_TRUE(state.isMiPartReservedBy(mi_b, "task_b"));
    EXPECT_TRUE(state.isSourceReservedBy(source_a, "task_a"));
    EXPECT_TRUE(state.isSourceReservedBy(source_b, "task_b"));
}

TEST(ANNIndexSchedulerStateTest, RemapReservationRejectsOverlappingSourceUuid)
{
    ANNIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);
    auto source_a = uuid(1, 0);

    EXPECT_TRUE(state.reserveRemapLineage("task_a", {mi_a}, {source_a}, uuid(12, 0)));
    EXPECT_FALSE(state.reserveRemapLineage("task_b", {mi_b}, {source_a}, uuid(13, 0)));
    EXPECT_FALSE(state.isMiPartReserved(mi_b));
}

TEST(ANNIndexSchedulerStateTest, CompactReservationDoesNotReserveCoveredSources)
{
    ANNIndexSchedulerState state;
    auto mi_a = uuid(10, 0);
    auto mi_b = uuid(11, 0);
    auto source_a = uuid(1, 0);

    EXPECT_TRUE(state.reserveCompactRebuild("compact_a", {mi_a, mi_b}, {source_a}, uuid(12, 0)));
    EXPECT_TRUE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactRebuild));
    EXPECT_TRUE(state.isMiPartReserved(mi_a));
    EXPECT_TRUE(state.isMiPartReserved(mi_b));
    EXPECT_FALSE(state.isSourceReserved(source_a));
    EXPECT_FALSE(state.reserveCompactRebuild("compact_b", {mi_b}, {source_a}, uuid(13, 0)));

    state.releaseTask("compact_a");
    EXPECT_FALSE(state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactRebuild));
    EXPECT_FALSE(state.isMiPartReserved(mi_a));
    EXPECT_FALSE(state.isMiPartReserved(mi_b));
    EXPECT_TRUE(state.reserveBuildBatch("build_a", {source_a}, uuid(14, 0)));
}

TEST(ANNIndexSchedulerStateTest, ReadyCoverageCanHavePendingCompact)
{
    ANNIndexSchedulerState state;
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

TEST(ANNIndexSchedulerPolicyTest, ObsoleteCoverageBeatsCompactFallback)
{
    ReconcileResult reconciled;
    auto vanished = uuid(1, 0);
    auto mi_a = uuid(10, 0);
    reconciled.candidate_kind = ReconcileCandidateKind::ObsoleteCoverage;
    reconciled.delta_out = {vanished};
    reconciled.obsolete_coverage.obsolete_source_part_uuids = {vanished};
    reconciled.obsolete_coverage.affected_ann_index_part_uuids = {mi_a};
    reconciled.obsolete_coverage.affected_ann_index_parts = {MergeTreeData::DataPartPtr{}};

    auto decision = ANNIndexSchedulerPolicy::choose(
        reconciled,
        {},
        /*compact_rebuild_candidate=*/true,
        {},
        {});

    EXPECT_EQ(decision.kind, ANNIndexSchedulerDecisionKind::ObsoleteCoverageCleanup);
    EXPECT_EQ(decision.delta_out_source_uuids, std::vector<UUID>{vanished});
}

TEST(ANNIndexSchedulerPolicyTest, RemapLineagePreservesSubtype)
{
    ReconcileResult reconciled;
    auto vanished = uuid(1, 0);
    reconciled.candidate_kind = ReconcileCandidateKind::RemapLineage;
    reconciled.delta_out = {vanished};
    reconciled.remap_lineage.remap_kind = ANNIndexRemapKind::MutationLineage;

    auto decision = ANNIndexSchedulerPolicy::choose(
        reconciled,
        {},
        /*compact_rebuild_candidate=*/false,
        {},
        {});

    EXPECT_EQ(decision.kind, ANNIndexSchedulerDecisionKind::RemapLineage);
    EXPECT_EQ(decision.remap_kind, ANNIndexRemapKind::MutationLineage);
    EXPECT_EQ(decision.delta_out_source_uuids, std::vector<UUID>{vanished});
}

TEST(ANNIndexSchedulerPolicyTest, RemapLineageBeatsBuildBatch)
{
    ReconcileResult reconciled;
    auto vanished = uuid(1, 0);
    reconciled.candidate_kind = ReconcileCandidateKind::RemapLineage;
    reconciled.delta_out = {vanished};
    reconciled.remap_lineage.remap_kind = ANNIndexRemapKind::MergeLineage;

    auto decision = ANNIndexSchedulerPolicy::choose(
        reconciled,
        {MergeTreeData::DataPartPtr{}},
        /*compact_rebuild_candidate=*/false,
        {},
        {});

    EXPECT_EQ(decision.kind, ANNIndexSchedulerDecisionKind::RemapLineage);
    EXPECT_EQ(decision.remap_kind, ANNIndexRemapKind::MergeLineage);
    EXPECT_EQ(decision.delta_out_source_uuids, std::vector<UUID>{vanished});
}

TEST(ANNIndexSchedulerStateTest, ResourceBackoffIsObservableAndClearable)
{
    ANNIndexSchedulerState state;
    ANNIndexSchedulerState::BacklogStats stats;
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

TEST(ANNIndexSchedulerStateTest, TaskFailureBackoffIsClearableAndPruned)
{
    ANNIndexSchedulerState state;

    state.recordTaskFailure("task_a", "transient failure", std::chrono::seconds(60), 0);
    EXPECT_TRUE(state.isTaskFailureBackoffActive("task_a"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 1u);

    state.clearTaskFailure("task_a");
    EXPECT_FALSE(state.isTaskFailureBackoffActive("task_a"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 0u);

    state.recordTaskFailure("expired_task", "expired failure", std::chrono::seconds(0), 0);
    EXPECT_FALSE(state.isTaskFailureBackoffActive("expired_task"));
    EXPECT_EQ(state.getObservabilitySnapshot().repeated_failure_count, 0u);
}
