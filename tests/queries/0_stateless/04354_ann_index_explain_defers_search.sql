-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- The Reflection ANN graph search is deferred from plan optimization to
-- pipeline-build time (ReadFromMergeTree::initializePipeline). A real SELECT
-- therefore runs the DiskANN search, while EXPLAIN must not: EXPLAIN PLAN never
-- builds a pipeline, and EXPLAIN PIPELINE builds one but the rewrite is bypassed
-- under is_explain so no deferred search is attached. This pins that EXPLAIN
-- (including PIPELINE, which does call initializePipeline) pays no search cost.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_explain_defer SYNC;
DROP TABLE IF EXISTS src_explain_defer;

CREATE TABLE src_explain_defer (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_explain_defer
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(4096);

CREATE REFLECTION mi_explain_defer
ON src_explain_defer (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_explain_defer;

CREATE TEMPORARY TABLE mi_explain_defer_start AS SELECT now64(6) AS ts;

SET log_comment = 'ann_defer_real';
SELECT k FROM src_explain_defer
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
FORMAT Null;

SET log_comment = 'ann_defer_explain_plan';
EXPLAIN PLAN SELECT k FROM src_explain_defer
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
FORMAT Null;

SET log_comment = 'ann_defer_explain_pipeline';
EXPLAIN PIPELINE SELECT k FROM src_explain_defer
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
FORMAT Null;

SET log_comment = '';
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_explain_defer_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment IN ('ann_defer_real', 'ann_defer_explain_plan', 'ann_defer_explain_pipeline')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_explain_defer SYNC;
DROP TABLE src_explain_defer;
