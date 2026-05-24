-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- A regular WHERE filter is represented as FilterStep between the distance
-- ExpressionStep and ReadFromMergeTree; AuxiliaryIndex should still match it.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET force_auxiliary_index = 'mi_where_filter';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_where_filter SYNC;
DROP TABLE IF EXISTS src_where_filter;

CREATE TABLE src_where_filter (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_where_filter
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(4096);

CREATE AUXILIARY INDEX mi_where_filter
ON src_where_filter (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC AUXILIARY INDEX mi_where_filter;

CREATE TEMPORARY TABLE mi_where_filter_start AS SELECT now64(6) AS ts;

SELECT k FROM src_where_filter
WHERE (k % 2) = 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
FORMAT Null
SETTINGS log_comment = 'mi_where_filter_positive';

SELECT k FROM src_where_filter
WHERE length(embedding) > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS auxiliary_index_require_match = 1, log_comment = 'mi_where_filter_depends_on_vector'; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_where_filter_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_where_filter_positive'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_where_filter SYNC;
DROP TABLE src_where_filter;
