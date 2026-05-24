-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- Invalid overfetch factors disable the AuxiliaryIndex fast path even when
-- a fully covering DiskANN part is ready. A valid forced query is included as
-- a positive control for the same table and index.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_overfetch_invalid SYNC;
DROP TABLE IF EXISTS src_query_overfetch_invalid;

CREATE TABLE src_query_overfetch_invalid (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_overfetch_invalid
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE AUXILIARY INDEX mi_query_overfetch_invalid
ON src_query_overfetch_invalid (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC AUXILIARY INDEX mi_query_overfetch_invalid;

CREATE TEMPORARY TABLE mi_query_overfetch_invalid_start AS SELECT now64(6) AS ts;

SELECT k FROM src_query_overfetch_invalid
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_auxiliary_index = 'mi_query_overfetch_invalid', log_comment = 'mi_overfetch_valid_control';

SELECT k FROM src_query_overfetch_invalid
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_auxiliary_index = 'mi_query_overfetch_invalid', log_comment = 'mi_overfetch_oversize', auxiliary_index_overfetch_factor = 1025;

SELECT k FROM src_query_overfetch_invalid
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_auxiliary_index = 'mi_query_overfetch_invalid', log_comment = 'mi_overfetch_zero', auxiliary_index_overfetch_factor = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_overfetch_invalid_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_overfetch_valid_control', 'mi_overfetch_oversize', 'mi_overfetch_zero')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_overfetch_invalid SYNC;
DROP TABLE src_query_overfetch_invalid;
