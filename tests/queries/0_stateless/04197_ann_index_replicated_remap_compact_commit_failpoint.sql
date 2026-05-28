-- Tags: no-fasttest, zookeeper, no-parallel
-- no-parallel because failpoints are process-global; another test running
-- against the same server could trip the same failpoint we enabled.
--
-- Exercises the Keeper hardware-error recovery branch inside
-- `StorageReplicatedMergeTree::commitReplacingPartFromBackgroundTask` for
-- replicated `ANNIndex` Remap and Compact tasks. The failpoint makes
-- the Keeper multi apply server-side while the client observes a hardware
-- error; the background commit must recover from Keeper and keep the local
-- rename instead of rolling the part back.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_repl_remap_fp SYNC;
DROP TABLE IF EXISTS src_repl_remap_fp SYNC;
DROP TABLE IF EXISTS mi_repl_compact_fp SYNC;
DROP TABLE IF EXISTS src_repl_compact_fp SYNC;

CREATE TABLE src_repl_remap_fp (k UInt64, payload UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_repl_remap_fp_04197', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES src_repl_remap_fp;

CREATE REFLECTION mi_repl_remap_fp
ON src_repl_remap_fp (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_repl_remap_fp_04197', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1,
         ann_index_compact_min_parts = 0;

INSERT INTO src_repl_remap_fp
SELECT number, number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

SYSTEM SYNC REFLECTION mi_repl_remap_fp;

INSERT INTO src_repl_remap_fp
SELECT number, number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16, 16);

SYSTEM SYNC REFLECTION mi_repl_remap_fp;

SELECT ann_index_part_count >= 2 AS remap_has_two_input_parts
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_remap_fp';

SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

SYSTEM START MERGES src_repl_remap_fp;
OPTIMIZE TABLE src_repl_remap_fp FINAL;

SYSTEM SYNC REFLECTION mi_repl_remap_fp;

SELECT count() AS remap_failpoint_fired_and_disabled
FROM system.fail_points
WHERE name = 'replicated_merge_tree_commit_zk_fail_after_op' AND enabled = 1;

SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

SELECT
    ann_index_part_count = 2 AS remap_outputs_visible_together,
    total_rows = 32 AS remap_rows_match
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_remap_fp';

SYSTEM FLUSH LOGS ann_index_log;

SELECT count() > 0 AS remap_finish_logged
FROM system.ann_index_log
WHERE database = currentDatabase()
    AND name = 'mi_repl_remap_fp'
    AND task_kind = 'MergeLineage'
    AND stage = 'finish'
    AND error_code = 0;

WITH (SELECT embedding FROM src_repl_remap_fp WHERE k = 7) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_repl_remap_fp
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS force_ann_index = 'mi_repl_remap_fp';

CREATE TABLE src_repl_compact_fp (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_repl_compact_fp_04197', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_repl_compact_fp
ON src_repl_compact_fp (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_repl_compact_fp_04197', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60;

INSERT INTO src_repl_compact_fp
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

SYSTEM SYNC REFLECTION mi_repl_compact_fp;

SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

ALTER TABLE src_repl_compact_fp
    UPDATE embedding = [toFloat32(k + 1000), toFloat32(k + 2000), toFloat32(k + 3000), toFloat32(k + 4000)]
    WHERE k < 8
    SETTINGS mutations_sync = 1;

SYSTEM SYNC REFLECTION mi_repl_compact_fp;

SELECT count() AS compact_failpoint_fired_and_disabled
FROM system.fail_points
WHERE name = 'replicated_merge_tree_commit_zk_fail_after_op' AND enabled = 1;

SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

SELECT
    ann_index_part_count > 0 AS compact_has_part,
    total_rows = 16 AS compact_rows_match
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_compact_fp';

SYSTEM FLUSH LOGS ann_index_log;

SELECT count() > 0 AS compact_finish_logged
FROM system.ann_index_log
WHERE database = currentDatabase()
    AND name = 'mi_repl_compact_fp'
    AND task_kind = 'CompactRebuild'
    AND stage = 'finish'
    AND error_code = 0;

WITH (SELECT embedding FROM src_repl_compact_fp WHERE k = 7) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_repl_compact_fp
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS force_ann_index = 'mi_repl_compact_fp';

DROP TABLE mi_repl_compact_fp SYNC;
DROP TABLE src_repl_compact_fp SYNC;
DROP TABLE mi_repl_remap_fp SYNC;
DROP TABLE src_repl_remap_fp SYNC;
