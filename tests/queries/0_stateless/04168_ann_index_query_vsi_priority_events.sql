-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- A source-side vector_similarity_index wins by default, but
-- force_using_ann_index makes the ANNIndex optimizer
-- preempt it and execute the DiskANN-backed fast path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_vsi_priority SYNC;
DROP TABLE IF EXISTS src_query_vsi_priority;

CREATE TABLE src_query_vsi_priority
(
    k UInt64,
    embedding Array(Float32),
    INDEX vsi embedding TYPE vector_similarity('hnsw', 'L2Distance', 4)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_vsi_priority
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_query_vsi_priority
ON src_query_vsi_priority (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_query_vsi_priority;

CREATE TEMPORARY TABLE mi_query_vsi_priority_start AS SELECT now64(6) AS ts;

SELECT k FROM src_query_vsi_priority
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_using_ann_index = 0, log_comment = 'mi_vsi_yields';

SELECT k FROM src_query_vsi_priority
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_using_ann_index = 1, log_comment = 'mi_vsi_preempts';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_vsi_priority_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_vsi_yields', 'mi_vsi_preempts')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_vsi_priority SYNC;
DROP TABLE src_query_vsi_priority;
