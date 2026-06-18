-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- A source-side vector_similarity_index wins by default, but
-- force_using_ann_index makes the ANNIndex optimizer
-- preempt it and execute the cppdiskann-backed fast path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_query_vsi_priority_cppdiskann SYNC;
DROP TABLE IF EXISTS src_query_vsi_priority_cppdiskann;

CREATE TABLE src_query_vsi_priority_cppdiskann
(
    k UInt64,
    embedding Array(Float32),
    INDEX vsi embedding TYPE vector_similarity('hnsw', 'L2Distance', 4)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_query_vsi_priority_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_query_vsi_priority_cppdiskann
ON src_query_vsi_priority_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_query_vsi_priority_cppdiskann;

CREATE TEMPORARY TABLE mi_query_vsi_priority_start_cppdiskann AS SELECT now64(6) AS ts;

SELECT k FROM src_query_vsi_priority_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_using_ann_index = 0, log_comment = 'mi_vsi_yields_cppdiskann';

SELECT k FROM src_query_vsi_priority_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS force_using_ann_index = 1, log_comment = 'mi_vsi_preempts_cppdiskann';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_query_vsi_priority_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_vsi_yields_cppdiskann', 'mi_vsi_preempts_cppdiskann')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_query_vsi_priority_cppdiskann SYNC;
DROP TABLE src_query_vsi_priority_cppdiskann;
