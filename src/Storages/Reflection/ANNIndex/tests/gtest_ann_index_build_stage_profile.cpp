#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/ANNIndexBuildStageProfile.h>

using namespace DB;
using namespace DB::ANNIndex;

TEST(ANNIndexBuildStageProfile, RegisterIsGetOrCreate)
{
    const String task_id = "build-stage-profile-get-or-create";
    unregisterBuildStageTimings(task_id);

    auto a = registerBuildStageTimings(task_id);
    auto b = registerBuildStageTimings(task_id);
    ASSERT_TRUE(a);
    /// Same task id must hand back the very same accumulator so the top-level
    /// BuildTask and the mid-layer BuildTaskImpl write into one instance.
    EXPECT_EQ(a.get(), b.get());

    EXPECT_EQ(getBuildStageTimings(task_id).get(), a.get());

    unregisterBuildStageTimings(task_id);
    EXPECT_EQ(getBuildStageTimings(task_id), nullptr);
}

TEST(ANNIndexBuildStageProfile, DistinctTasksAreIsolated)
{
    auto a = registerBuildStageTimings("build-stage-profile-task-a");
    auto b = registerBuildStageTimings("build-stage-profile-task-b");
    EXPECT_NE(a.get(), b.get());
    unregisterBuildStageTimings("build-stage-profile-task-a");
    unregisterBuildStageTimings("build-stage-profile-task-b");
}

TEST(ANNIndexBuildStageProfile, EmptyTaskIdIsDetached)
{
    /// Empty task id returns a valid (so instrumentation never null-checks) but
    /// untracked instance — nothing to look up later.
    auto detached = registerBuildStageTimings("");
    ASSERT_TRUE(detached);
    EXPECT_EQ(getBuildStageTimings(""), nullptr);
}

TEST(ANNIndexBuildStageProfile, CoreEventsAlwaysEmittedOptionalOnlyWhenSet)
{
    BuildStageTimings timings;
    auto events = makeBuildStageProfileEvents(timings);

    /// Core timings are present even at zero so the column set stays stable.
    EXPECT_TRUE(events.count("ANNIndexBuildStage1PrepareMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage2ScanReadSourceMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage2ScanIngestMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage3GraphMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage4FinishAlgorithmMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage6FinalizePartMicroseconds"));
    EXPECT_TRUE(events.count("ANNIndexBuildStage7CommitMicroseconds"));
    EXPECT_EQ(events.at("ANNIndexBuildStage3GraphMicroseconds"), 0u);

    /// Optional sub-timers are suppressed while zero.
    EXPECT_FALSE(events.count("ANNIndexBuildStage5CleanupMicroseconds"));
    EXPECT_FALSE(events.count("ANNIndexBuildStage6FinalizeWriteLocatorMicroseconds"));

    timings.stage3_graph_us.store(1234);
    timings.stage5_cleanup_us.store(7);
    timings.stage6_finalize_write_locator_us.store(56);
    events = makeBuildStageProfileEvents(timings);

    EXPECT_EQ(events.at("ANNIndexBuildStage3GraphMicroseconds"), 1234u);
    EXPECT_EQ(events.at("ANNIndexBuildStage5CleanupMicroseconds"), 7u);
    EXPECT_EQ(events.at("ANNIndexBuildStage6FinalizeWriteLocatorMicroseconds"), 56u);
}
