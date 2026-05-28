-- Tags: no-fasttest, no-parallel
-- When `tryReserveFuturePart` throws after `tryAcquireTaskResources` succeeded,
-- `rollbackUncommittedTaskReservation` must release the global/per-source counters.
-- Otherwise `ann_index_max_global_background_tasks = 1` would block all
-- future scheduling until restart.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_res_fp SYNC;
DROP TABLE IF EXISTS src_res_fp;

CREATE TABLE src_res_fp (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_res_fp
ON src_res_fp (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60,
         ann_index_max_global_background_tasks = 1,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1;

SYSTEM ENABLE FAILPOINT ann_index_throw_in_try_reserve_future_part;

INSERT INTO src_res_fp
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(32);

-- Deterministic ordering: SYNC blocks until the MI part is committed. The
-- failpoint has `ONCE` semantics, so the first scheduler tick reaching
-- `tryReserveFuturePart` throws (consuming the failpoint and exercising
-- `rollbackUncommittedTaskReservation`); the next tick passes through cleanly
-- and the build completes. SYNC returning therefore proves *both* (1) the
-- rollback path released the global/per-source counters — otherwise SYNC would
-- time out under `ann_index_max_global_background_tasks = 1`, and
-- (2) the failpoint actually fired. Probing `enabled` after SYNC is then race-free.
SYSTEM SYNC REFLECTION mi_res_fp;

SELECT count() AS reserve_failpoint_fired_and_disabled
FROM system.fail_points
WHERE name = 'ann_index_throw_in_try_reserve_future_part' AND enabled = 1;

SELECT
    ann_index_part_count > 0 AS build_committed_after_counter_release,
    pending_task_count = 0 AS no_stale_pending_tasks
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_res_fp';

DROP TABLE mi_res_fp SYNC;
DROP TABLE src_res_fp;
