-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- Two MIs on the same source: both match the query (same algorithm, same k)
-- so their algorithm-side costs tie; the framework breaks the tie by MI name
-- lexicographic order. The query path is identical under either MI, so this
-- test asserts result correctness; the tie-break rule itself is covered by
-- the CostTie.StableSelection gtest.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_multi_a_cppdiskann SYNC;
DROP TABLE IF EXISTS mi_multi_b_cppdiskann SYNC;
DROP TABLE IF EXISTS src_multi_cppdiskann;

CREATE TABLE src_multi_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_multi_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_multi_a_cppdiskann
ON src_multi_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

CREATE REFLECTION mi_multi_b_cppdiskann
ON src_multi_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_multi_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_multi_a_cppdiskann SYNC;
DROP TABLE mi_multi_b_cppdiskann SYNC;
DROP TABLE src_multi_cppdiskann;
