-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- Partial coverage must keep the vector column in both Union branches when
-- SELECT outputs depend on it.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_partial_select_vector_cppdiskann';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_partial_select_vector_cppdiskann SYNC;
DROP TABLE IF EXISTS src_partial_select_vector_cppdiskann;

CREATE TABLE src_partial_select_vector_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_partial_select_vector_cppdiskann
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_partial_select_vector_cppdiskann
ON src_partial_select_vector_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_partial_select_vector_cppdiskann;

INSERT INTO src_partial_select_vector_cppdiskann
SELECT 100000 + number, arrayMap(d -> toFloat32(cityHash64(100000 + number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(16);

CREATE TEMPORARY TABLE mi_partial_select_vector_start_cppdiskann AS SELECT now64(6) AS ts;

WITH (SELECT embedding FROM src_partial_select_vector_cppdiskann WHERE k = 100000) AS q
SELECT length(embedding) AS dim, isFinite(L2Distance(embedding, q)) AS finite_distance
FROM src_partial_select_vector_cppdiskann
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_partial_select_vector_cppdiskann';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_partial_select_vector_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_partial_select_vector_cppdiskann'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_partial_select_vector_cppdiskann SYNC;
DROP TABLE src_partial_select_vector_cppdiskann;
