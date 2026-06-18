-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- With parallel replicas active the MI optimizer must early-return so the
-- ReadHints channel is not used (it cannot reach the followers).

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_pr_cppdiskann SYNC;
DROP TABLE IF EXISTS src_pr_cppdiskann;

CREATE TABLE src_pr_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pr_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_pr_cppdiskann
ON src_pr_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_pr_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
SETTINGS parallel_replicas_local_plan = 1, max_parallel_replicas = 2,
         parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE mi_pr_cppdiskann SYNC;
DROP TABLE src_pr_cppdiskann;
