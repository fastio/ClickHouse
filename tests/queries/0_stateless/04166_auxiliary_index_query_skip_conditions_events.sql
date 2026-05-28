-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- Query-level guards must skip unsupported AuxiliaryIndex fast paths. The first
-- query is a positive control that proves the main table has a ready
-- DiskANN-backed index; the dim-mismatch case is separate because it declines
-- during algorithm matching before a usable index is required. A non-vector
-- PREWHERE is expected to stay on the fast path.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_skip SYNC;
DROP TABLE IF EXISTS mi_query_skip_dim SYNC;
DROP TABLE IF EXISTS src_query_skip;
DROP TABLE IF EXISTS src_query_skip_dim;

CREATE TABLE src_query_skip (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_skip
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_query_skip
ON src_query_skip (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_query_skip;

CREATE TABLE src_query_skip_dim (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_skip_dim
SELECT number, [number * 1.0, 0]
FROM numbers(32);

CREATE REFLECTION mi_query_skip_dim
ON src_query_skip_dim (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

CREATE TEMPORARY TABLE mi_query_skip_start AS SELECT now64(6) AS ts;

SELECT k FROM src_query_skip
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_positive_control';

SELECT k FROM src_query_skip
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS enable_auxiliary_index = 0, log_comment = 'mi_skip_feature_off';

SELECT k FROM src_query_skip
PREWHERE k > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_prewhere';

SELECT k FROM src_query_skip_dim
ORDER BY L2Distance(embedding, [3.7, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_dim_mismatch';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_skip_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_skip_positive_control', 'mi_skip_feature_off', 'mi_skip_prewhere', 'mi_skip_dim_mismatch')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_skip_dim SYNC;
DROP TABLE mi_query_skip SYNC;
DROP TABLE src_query_skip_dim;
DROP TABLE src_query_skip;
