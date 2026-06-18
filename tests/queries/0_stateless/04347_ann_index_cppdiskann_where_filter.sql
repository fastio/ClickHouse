-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- A regular WHERE filter is represented as FilterStep between the distance
-- ExpressionStep and ReadFromMergeTree; ANNIndex should still match it.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_where_filter_cppdiskann';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_where_filter_cppdiskann SYNC;
DROP TABLE IF EXISTS src_where_filter_cppdiskann;

CREATE TABLE src_where_filter_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_where_filter_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(4096);

CREATE REFLECTION mi_where_filter_cppdiskann
ON src_where_filter_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_where_filter_cppdiskann;

CREATE TEMPORARY TABLE mi_where_filter_start_cppdiskann AS SELECT now64(6) AS ts;

SELECT k FROM src_where_filter_cppdiskann
WHERE (k % 2) = 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
FORMAT Null
SETTINGS log_comment = 'mi_where_filter_positive_cppdiskann';

SELECT k FROM src_where_filter_cppdiskann
WHERE length(embedding) > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS ann_index_require_match = 1, log_comment = 'mi_where_filter_depends_on_vector_cppdiskann'; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_where_filter_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_where_filter_positive_cppdiskann'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_where_filter_cppdiskann SYNC;
DROP TABLE src_where_filter_cppdiskann;
