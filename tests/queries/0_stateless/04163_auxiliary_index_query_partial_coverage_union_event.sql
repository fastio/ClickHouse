-- Tags: no-fasttest, no-parallel
-- no-parallel because this test asserts per-query profile events via query_log.
-- Partial coverage must combine a DiskANN-covered branch with an uncovered
-- source scan branch. The final result is intentionally closest to the
-- uncovered part so a pure covered-branch rewrite would be wrong.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_partial_union SYNC;
DROP TABLE IF EXISTS src_query_partial_union;

CREATE TABLE src_query_partial_union (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_partial_union
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(5000);

CREATE REFLECTION mi_query_partial_union
ON src_query_partial_union (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_query_partial_union;

INSERT INTO src_query_partial_union
SELECT 100000 + number, [(100000 + number) * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE TEMPORARY TABLE mi_query_partial_union_start AS SELECT now64(6) AS ts;

SELECT k FROM src_query_partial_union
ORDER BY L2Distance(embedding, [100002.3, 0, 0, 0])
LIMIT 5
SETTINGS log_comment = 'mi_query_partial_union';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_partial_union_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_query_partial_union'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_partial_union SYNC;
DROP TABLE src_query_partial_union;
