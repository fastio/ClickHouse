-- Tags: no-fasttest, zookeeper, no-parallel
-- no-parallel because failpoints are process-global; another test running
-- against the same server could trip the same failpoint we enabled.
--
-- Exercises the Keeper hardware-error recovery branch inside
-- `StorageReplicatedMergeTree::commitReplacingPartFromBackgroundTask`,
-- which is the commit path used by `ReplicatedANN` builds.
-- With `replicated_merge_tree_commit_zk_fail_after_op` enabled, the
-- background commit observes a hardware error after the Keeper multi
-- op was already applied server-side. The recovery loop must then check
-- that the part node exists in Keeper and commit the local rename
-- atomically with the materialized-index lease / task-lock check ops.
--
-- This path is unique to background commits (not insert path), so the
-- existing `02718` / `02919` insert-side tests do not exercise it.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_repl_fp SYNC;
DROP TABLE IF EXISTS src_repl_fp SYNC;

CREATE TABLE src_repl_fp (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_repl_fp_04189', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_repl_fp
ON src_repl_fp (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_repl_fp_04189', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60;

-- Trip the next ZK multi inside `commitReplacingPartFromBackgroundTask`.
-- This is a ONCE failpoint: it auto-disables after the first hit, so the
-- recovery path must complete the commit on its retry, not on a fresh
-- build cycle.
SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

INSERT INTO src_repl_fp
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(32);

-- The background scheduler picks up the new source part, builds an MI
-- part, and commits through `commitReplacingPartFromBackgroundTask`. The
-- first multi sees a forced hardware error; the recovery loop confirms
-- the op was applied and commits the local transaction. SYSTEM SYNC
-- blocks until that recovered commit lands in `system.ann_indexes`.
SYSTEM SYNC REFLECTION mi_repl_fp;

-- The failpoint is ONCE: it auto-disables after the first hit. If the
-- recovery path was exercised, it must be gone from `system.fail_points`
-- here. If it is still enabled, the build commit never went through the
-- injected ZK multi at all and the test would be silently meaningless.
SELECT count() AS failpoint_fired_and_disabled
FROM system.fail_points
WHERE name = 'replicated_merge_tree_commit_zk_fail_after_op' AND enabled = 1;

-- Defensive: idempotent disable in case the failpoint somehow survived
-- (e.g. build raced an empty source snapshot before the INSERT was
-- visible) so it cannot leak into other queries on the same instance.
SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

-- The recovered commit must produce exactly the indexed rows; if the
-- restore-on-failure branch had fired instead, `total_rows` would be 0
-- and `ann_index_part_count` would stay at 0.
SELECT
    ann_index_part_count > 0 AS has_part,
    total_rows = 32 AS rows_match
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_fp';

-- The committed MI part must be queryable. Self-query a row by k=7: the
-- top-1 row by L2Distance against its own embedding is itself with d=0.
-- If the part were silently broken or empty, the optimizer would either
-- fall back to brute-force (still distance 0) or the query would fail.
-- Self-query is robust to DiskANN approximation: distance 0 has no peer.
WITH (SELECT embedding FROM src_repl_fp WHERE k = 7) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_repl_fp
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS force_ann_index = 'mi_repl_fp';

DROP TABLE mi_repl_fp SYNC;
DROP TABLE src_repl_fp SYNC;
