-- Tags: no-fasttest, no-parallel
-- Verifies that the ANN-index build pipeline records per-stage wall-clock
-- timings into system.reflection_job_log.profile_events (the ANNIndexBuild*
-- keys), so the build flow can be analyzed stage by stage.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_stage_timings SYNC;
DROP TABLE IF EXISTS src_stage_timings;

CREATE TABLE src_stage_timings (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_stage_timings
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(256);

CREATE REFLECTION mi_stage_timings
ON src_stage_timings (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 30;

SYSTEM SYNC REFLECTION mi_stage_timings;

SYSTEM FLUSH LOGS;

-- The finished build row must carry every core per-stage timing. Presence is
-- deterministic (core stage keys are always emitted, even when a stage is
-- negligibly fast), so this does not depend on machine speed.
SELECT
    mapContains(profile_events, 'ANNIndexBuildStage1PrepareMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage2ScanReadSourceMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage2ScanIngestMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage3GraphMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage4FinishAlgorithmMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage6FinalizePartMicroseconds')
  AND mapContains(profile_events, 'ANNIndexBuildStage7CommitMicroseconds') AS has_core_stage_timings
FROM system.reflection_job_log
WHERE database = currentDatabase()
  AND reflection_name = 'mi_stage_timings'
  AND event_type = 'TaskFinished'
ORDER BY event_time DESC
LIMIT 1;

-- A real DiskANN build performs disk reservation, graph construction, part
-- finalization (fsync) and a commit, so the summed core timings are positive.
SELECT
    profile_events['ANNIndexBuildStage1PrepareMicroseconds']
  + profile_events['ANNIndexBuildStage3GraphMicroseconds']
  + profile_events['ANNIndexBuildStage6FinalizePartMicroseconds']
  + profile_events['ANNIndexBuildStage7CommitMicroseconds'] > 0 AS total_positive
FROM system.reflection_job_log
WHERE database = currentDatabase()
  AND reflection_name = 'mi_stage_timings'
  AND event_type = 'TaskFinished'
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE mi_stage_timings SYNC;
DROP TABLE src_stage_timings;
