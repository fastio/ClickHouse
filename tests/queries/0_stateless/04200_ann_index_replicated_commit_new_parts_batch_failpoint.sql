-- Tags: no-fasttest, zookeeper, no-parallel
-- Exercises `ANNIndexPartCommitter::commitNewParts` on a replicated
-- inner table: Remap emits multiple new materialized-index-parts and commits
-- them through `StorageReplicatedMergeTree::commitReplacingPartsFromBackgroundTask`
-- (batch Keeper multi). With `replicated_merge_tree_commit_zk_fail_after_op`,
-- the first multi observes a hardware error after the op was applied; recovery
-- must confirm every part znode exists and finish the local transaction.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_repl_batch_fp SYNC;
DROP TABLE IF EXISTS src_repl_batch_fp SYNC;

CREATE TABLE src_repl_batch_fp (k UInt64, payload UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_repl_batch_fp_04200', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES src_repl_batch_fp;

CREATE REFLECTION mi_repl_batch_fp
ON src_repl_batch_fp (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_repl_batch_fp_04200', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1,
         ann_index_compact_min_parts = 0;

INSERT INTO src_repl_batch_fp
SELECT number, number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

SYSTEM SYNC REFLECTION mi_repl_batch_fp;

INSERT INTO src_repl_batch_fp
SELECT number, number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16, 16);

SYSTEM SYNC REFLECTION mi_repl_batch_fp;

SELECT ann_index_part_count >= 2 AS has_two_input_parts
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_batch_fp';

SYSTEM ENABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

SYSTEM START MERGES src_repl_batch_fp;
OPTIMIZE TABLE src_repl_batch_fp FINAL;

SYSTEM SYNC REFLECTION mi_repl_batch_fp;

SELECT count() AS batch_failpoint_fired_and_disabled
FROM system.fail_points
WHERE name = 'replicated_merge_tree_commit_zk_fail_after_op' AND enabled = 1;

SYSTEM DISABLE FAILPOINT replicated_merge_tree_commit_zk_fail_after_op;

SELECT
    ann_index_part_count = 2 AS batch_outputs_committed,
    total_rows = 32 AS batch_rows_match
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_batch_fp';

DROP TABLE mi_repl_batch_fp SYNC;
DROP TABLE src_repl_batch_fp SYNC;
