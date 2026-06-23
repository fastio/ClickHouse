#include <Storages/Reflection/ANNIndex/ANNIndexBuildStageProfile.h>

#include <mutex>
#include <unordered_map>


namespace DB::ANNIndex
{

namespace
{

std::mutex build_stage_timings_registry_mutex;
std::unordered_map<String, BuildStageTimingsPtr> build_stage_timings_registry;

}

BuildStageTimingsPtr registerBuildStageTimings(const String & task_id)
{
    if (task_id.empty())
        return std::make_shared<BuildStageTimings>();

    std::lock_guard lock(build_stage_timings_registry_mutex);
    auto it = build_stage_timings_registry.find(task_id);
    if (it == build_stage_timings_registry.end())
        it = build_stage_timings_registry.emplace(task_id, std::make_shared<BuildStageTimings>()).first;
    return it->second;
}

BuildStageTimingsPtr getBuildStageTimings(const String & task_id)
{
    if (task_id.empty())
        return nullptr;

    std::lock_guard lock(build_stage_timings_registry_mutex);
    auto it = build_stage_timings_registry.find(task_id);
    if (it == build_stage_timings_registry.end())
        return nullptr;
    return it->second;
}

void unregisterBuildStageTimings(const String & task_id)
{
    if (task_id.empty())
        return;

    std::lock_guard lock(build_stage_timings_registry_mutex);
    build_stage_timings_registry.erase(task_id);
}

std::map<String, UInt64> makeBuildStageProfileEvents(const BuildStageTimings & timings)
{
    std::map<String, UInt64> result;

    const auto load = [](const std::atomic<UInt64> & v) { return v.load(std::memory_order_relaxed); };

    /// Core stage timings: emitted unconditionally so the column set stays
    /// stable across tasks and 0 is recorded as a meaningful value.
    result.emplace("ANNIndexBuildStage1PrepareMicroseconds", load(timings.stage1_prepare_us));
    result.emplace("ANNIndexBuildStage2ScanReadSourceMicroseconds", load(timings.stage2_scan_read_source_us));
    result.emplace("ANNIndexBuildStage2ScanIngestMicroseconds", load(timings.stage2_scan_ingest_us));
    result.emplace("ANNIndexBuildStage3GraphMicroseconds", load(timings.stage3_graph_us));
    result.emplace("ANNIndexBuildStage4FinishAlgorithmMicroseconds", load(timings.stage4_finish_algorithm_us));
    result.emplace("ANNIndexBuildStage6FinalizePartMicroseconds", load(timings.stage6_finalize_part_us));
    result.emplace("ANNIndexBuildStage7CommitMicroseconds", load(timings.stage7_commit_us));

    /// Fine-grained sub-timers: emitted only when non-zero to avoid noise.
    const auto emit_if_set = [&](const char * name, UInt64 value)
    {
        if (value)
            result.emplace(name, value);
    };
    emit_if_set("ANNIndexBuildStage2ScanColumnWriteMicroseconds", load(timings.stage2_scan_column_write_us));
    emit_if_set("ANNIndexBuildStage2ScanLocatorBuildMicroseconds", load(timings.stage2_scan_locator_build_us));
    emit_if_set("ANNIndexBuildStage5CleanupMicroseconds", load(timings.stage5_cleanup_us));
    emit_if_set("ANNIndexBuildStage6FinalizeWriteLocatorMicroseconds", load(timings.stage6_finalize_write_locator_us));
    emit_if_set("ANNIndexBuildStage6FinalizeChecksumsMicroseconds", load(timings.stage6_finalize_checksums_us));
    emit_if_set("ANNIndexBuildStage6FinalizePrecommitMicroseconds", load(timings.stage6_finalize_precommit_us));

    return result;
}

}
