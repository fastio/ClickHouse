-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- force_ann_index pins the optimizer to a specific MI even when its
-- cost is worse than another candidate (and even when its cost would lose to
-- the fallback full-scan). The result is still correct because both paths
-- produce the same top-K.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_force_a_cppdiskann SYNC;
DROP TABLE IF EXISTS mi_force_b_cppdiskann SYNC;
DROP TABLE IF EXISTS src_force_cppdiskann;

CREATE TABLE src_force_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_force_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_force_a_cppdiskann
ON src_force_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

CREATE REFLECTION mi_force_b_cppdiskann
ON src_force_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SET force_ann_index = 'mi_force_b_cppdiskann';

SELECT k FROM src_force_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_force_a_cppdiskann SYNC;
DROP TABLE mi_force_b_cppdiskann SYNC;
DROP TABLE src_force_cppdiskann;
