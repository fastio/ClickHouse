#pragma once

#include <base/types.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <map>
#include <memory>


namespace DB::ANNIndex
{

/// Per-task wall-clock accumulators for the ANN-index build pipeline, in
/// microseconds. One instance per build task, addressed by `task_id` through
/// the registry below — mirroring the DiskANN/SPANN build-status registries so
/// that `makeSchedulerTaskSnapshot` can fold these timings into the
/// `system.reflection_job_log` `profile_events` map at log-write time.
///
/// Fields are atomic: they are written by the (serialized) background pool
/// thread driving `executeStep`, and may be read concurrently by the live
/// `system.reflection_jobs` snapshot while the build is still running.
struct BuildStageTimings
{
    /// Stage 1 — top-level prepare (reserve space, create dirs, clone algorithm).
    std::atomic<UInt64> stage1_prepare_us{0};

    /// Stage 2 — Scan: read source blocks, feed the algorithm, write the anchor
    /// column and build the in-memory locator sidecars.
    std::atomic<UInt64> stage2_scan_read_source_us{0};
    std::atomic<UInt64> stage2_scan_ingest_us{0};
    std::atomic<UInt64> stage2_scan_column_write_us{0};
    std::atomic<UInt64> stage2_scan_locator_build_us{0};

    /// Stage 3 — algorithm graph build (`buildAlgorithmPrivate`).
    std::atomic<UInt64> stage3_graph_us{0};

    /// Stage 4 — algorithm finish (`finishBuild`).
    std::atomic<UInt64> stage4_finish_algorithm_us{0};

    /// Stage 5 — reclaim intermediate storage.
    std::atomic<UInt64> stage5_cleanup_us{0};

    /// Stage 6 — finalize the columnar part + framework sidecars + checksums.
    std::atomic<UInt64> stage6_finalize_part_us{0};
    std::atomic<UInt64> stage6_finalize_write_locator_us{0};
    std::atomic<UInt64> stage6_finalize_checksums_us{0};
    std::atomic<UInt64> stage6_finalize_precommit_us{0};

    /// Stage 7 — top-level commit (`commitNewPart`, the Keeper transaction).
    std::atomic<UInt64> stage7_commit_us{0};
};

using BuildStageTimingsPtr = std::shared_ptr<BuildStageTimings>;

/// Get-or-create the timings instance for `task_id`. Always returns a valid
/// pointer. For an empty `task_id` (non-scheduler-driven builds, tests) a
/// detached instance is returned that is NOT tracked in the registry — its
/// timings are simply never read back.
BuildStageTimingsPtr registerBuildStageTimings(const String & task_id);

/// Returns the tracked timings for `task_id`, or nullptr if absent.
BuildStageTimingsPtr getBuildStageTimings(const String & task_id);

/// Drops the tracked timings for `task_id`. No-op for an unknown key.
void unregisterBuildStageTimings(const String & task_id);

/// Render the accumulators into `profile_events` entries keyed
/// `ANNIndexBuildStage<N><Stage>[<Action>]Microseconds`. Core stage timings are
/// emitted unconditionally (0 is meaningful — the stage ran but was negligible
/// or skipped); fine-grained sub-timers are emitted only when non-zero to avoid
/// noise columns.
std::map<String, UInt64> makeBuildStageProfileEvents(const BuildStageTimings & timings);

/// RAII helper: accumulates its lifetime (in microseconds) into the target
/// atomic on destruction. Handles early returns / exceptions in the timed scope.
struct ScopedStageTimer
{
    explicit ScopedStageTimer(std::atomic<UInt64> & target_)
        : target(target_)
    {
    }

    ~ScopedStageTimer()
    {
        target.fetch_add(watch.elapsedMicroseconds(), std::memory_order_relaxed);
    }

    std::atomic<UInt64> & target;
    Stopwatch watch{CLOCK_MONOTONIC};
};

}
