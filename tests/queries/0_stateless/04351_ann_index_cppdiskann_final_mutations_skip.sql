-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts per-query profile events via query_log.
-- ANNIndex fast path must not rewrite reads that need FINAL or on-the-fly mutations.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_final_mutations_skip_cppdiskann SYNC;
DROP TABLE IF EXISTS src_final_mutations_skip_cppdiskann;

CREATE TABLE src_final_mutations_skip_cppdiskann (k UInt64, ver UInt64, embedding Array(Float32))
ENGINE = ReplacingMergeTree(ver)
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_final_mutations_skip_cppdiskann
SELECT number, 1, [number * 1.0, 0, 0, 0]
FROM numbers(2048);

CREATE REFLECTION mi_final_mutations_skip_cppdiskann
ON src_final_mutations_skip_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 20;

SYSTEM SYNC REFLECTION mi_final_mutations_skip_cppdiskann;

CREATE TEMPORARY TABLE mi_final_mutations_skip_start_cppdiskann AS SELECT now64(6) AS ts;

SELECT k FROM src_final_mutations_skip_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_positive_control_cppdiskann';

SELECT k FROM src_final_mutations_skip_cppdiskann FINAL
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS log_comment = 'mi_final_skip_cppdiskann';

SYSTEM STOP MERGES src_final_mutations_skip_cppdiskann;
ALTER TABLE src_final_mutations_skip_cppdiskann
    UPDATE embedding = [10000., 0., 0., 0.]
    WHERE k = 3
    SETTINGS mutations_sync = 0;

SELECT k FROM src_final_mutations_skip_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 1
FORMAT Null
SETTINGS apply_mutations_on_fly = 1, log_comment = 'mi_mutation_skip_cppdiskann';

SYSTEM START MERGES src_final_mutations_skip_cppdiskann;
SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_final_mutations_skip_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment IN ('mi_positive_control_cppdiskann', 'mi_final_skip_cppdiskann', 'mi_mutation_skip_cppdiskann')
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_final_mutations_skip_cppdiskann SYNC;
DROP TABLE src_final_mutations_skip_cppdiskann;
