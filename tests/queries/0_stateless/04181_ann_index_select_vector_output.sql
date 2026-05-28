-- Tags: no-fasttest, no-parallel, no-parallel-replicas
-- no-parallel because this test asserts per-query profile events via query_log.
-- no-parallel-replicas because the test sets `ann_index_require_match = 1`
-- and the MI rewriter intentionally skips when parallel-replicas is on (cross-replica
-- coordination of MI ranks is not implemented). Random-settings injection of
-- `enable_parallel_replicas = 1` would otherwise turn require_match into BAD_ARGUMENTS.
-- ANNIndex should still match when SELECT outputs need the vector column.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_select_vector_output';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_select_vector_output SYNC;
DROP TABLE IF EXISTS src_select_vector_output;

CREATE TABLE src_select_vector_output (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_select_vector_output
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_select_vector_output
ON src_select_vector_output (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_select_vector_output;

CREATE TEMPORARY TABLE mi_select_vector_output_start AS SELECT now64(6) AS ts;

WITH (SELECT embedding FROM src_select_vector_output WHERE k = 1000) AS q
SELECT k, embedding = q AS same_embedding, round(L2Distance(embedding, q), 6) AS d
FROM src_select_vector_output
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_output';

WITH (SELECT embedding FROM src_select_vector_output WHERE k = 1000) AS q
SELECT k, embedding
FROM src_select_vector_output
WHERE (k % 2) = 0
ORDER BY L2Distance(embedding, q)
LIMIT 1
FORMAT Null
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_where';

WITH (SELECT embedding FROM src_select_vector_output WHERE k = 1000) AS q
SELECT k, embedding
FROM src_select_vector_output
WHERE length(embedding) > 0
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_filter_depends'; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexDiskANNSearchStarted'] > 0) AS used_diskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_select_vector_output_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_select_vector_output', 'mi_select_vector_where')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_select_vector_output SYNC;
DROP TABLE src_select_vector_output;
