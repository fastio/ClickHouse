-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- Query-level guards must skip unsupported ANNIndex fast paths. The first
-- query is a positive control that proves the main table has a ready
-- cppdiskann-backed index; the dim-mismatch case is separate because it declines
-- during algorithm matching before a usable index is required. A non-vector
-- PREWHERE is expected to stay on the fast path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_skip_cppdiskann SYNC;
DROP TABLE IF EXISTS mi_query_skip_dim_cppdiskann SYNC;
DROP TABLE IF EXISTS src_query_skip_cppdiskann;
DROP TABLE IF EXISTS src_query_skip_dim_cppdiskann;

CREATE TABLE src_query_skip_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_skip_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_query_skip_cppdiskann
ON src_query_skip_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_query_skip_cppdiskann;

CREATE TABLE src_query_skip_dim_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_skip_dim_cppdiskann
SELECT number, [number * 1.0, 0]
FROM numbers(32);

CREATE REFLECTION mi_query_skip_dim_cppdiskann
ON src_query_skip_dim_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

CREATE TEMPORARY TABLE mi_query_skip_start_cppdiskann AS SELECT now64(6) AS ts;

SELECT k FROM src_query_skip_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_positive_control_cppdiskann';

SELECT k FROM src_query_skip_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS enable_ann_index = 0, log_comment = 'mi_skip_feature_off_cppdiskann';

SELECT k FROM src_query_skip_cppdiskann
PREWHERE k > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_prewhere_cppdiskann';

SELECT k FROM src_query_skip_dim_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_skip_dim_mismatch_cppdiskann';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_skip_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_skip_positive_control_cppdiskann', 'mi_skip_feature_off_cppdiskann', 'mi_skip_prewhere_cppdiskann', 'mi_skip_dim_mismatch_cppdiskann')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_skip_dim_cppdiskann SYNC;
DROP TABLE mi_query_skip_cppdiskann SYNC;
DROP TABLE src_query_skip_dim_cppdiskann;
DROP TABLE src_query_skip_cppdiskann;
