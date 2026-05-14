-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- If the final projection still needs the original vector column, the
-- MaterializedIndex optimizer must keep reading it alongside virtual `_distance`.
-- A normal projection is included as a positive control.

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_expr_depends SYNC;
DROP TABLE IF EXISTS src_query_expr_depends;

CREATE TABLE src_query_expr_depends (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_expr_depends
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE MATERIALIZED INDEX mi_query_expr_depends
ON src_query_expr_depends (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 20;

SYSTEM SYNC MATERIALIZED INDEX mi_query_expr_depends;

CREATE TEMPORARY TABLE mi_query_expr_depends_start AS SELECT now64(6) AS ts;

SELECT k FROM src_query_expr_depends
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_expr_fast_control';

SELECT embedding FROM src_query_expr_depends
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_expr_depends_on_vector';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['MaterializedIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_expr_depends_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_expr_fast_control', 'mi_expr_depends_on_vector')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_expr_depends SYNC;
DROP TABLE src_query_expr_depends;
