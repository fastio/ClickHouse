-- Tags: no-fasttest, no-parallel, no-parallel-replicas, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- no-parallel-replicas because the test sets `ann_index_require_match = 1`
-- and the MI rewriter intentionally skips when parallel-replicas is on (cross-replica
-- coordination of MI ranks is not implemented). Random-settings injection of
-- `enable_parallel_replicas = 1` would otherwise turn require_match into BAD_ARGUMENTS.
-- ANNIndex should still match when SELECT outputs need the vector column.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_select_vector_output_cppdiskann';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_select_vector_output_cppdiskann SYNC;
DROP TABLE IF EXISTS src_select_vector_output_cppdiskann;

CREATE TABLE src_select_vector_output_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_select_vector_output_cppdiskann
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_select_vector_output_cppdiskann
ON src_select_vector_output_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_select_vector_output_cppdiskann;

CREATE TEMPORARY TABLE mi_select_vector_output_start_cppdiskann AS SELECT now64(6) AS ts;

WITH (SELECT embedding FROM src_select_vector_output_cppdiskann WHERE k = 1000) AS q
SELECT k, embedding = q AS same_embedding, round(L2Distance(embedding, q), 6) AS d
FROM src_select_vector_output_cppdiskann
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_output_cppdiskann';

WITH (SELECT embedding FROM src_select_vector_output_cppdiskann WHERE k = 1000) AS q
SELECT k, embedding
FROM src_select_vector_output_cppdiskann
WHERE (k % 2) = 0
ORDER BY L2Distance(embedding, q)
LIMIT 1
FORMAT Null
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_where_cppdiskann';

WITH (SELECT embedding FROM src_select_vector_output_cppdiskann WHERE k = 1000) AS q
SELECT k, embedding
FROM src_select_vector_output_cppdiskann
WHERE length(embedding) > 0
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS ann_index_require_match = 1, log_comment = 'mi_select_vector_filter_depends_cppdiskann'; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_select_vector_output_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_select_vector_output_cppdiskann', 'mi_select_vector_where_cppdiskann')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_select_vector_output_cppdiskann SYNC;
DROP TABLE src_select_vector_output_cppdiskann;
