-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- 10-row source: the algorithm cost (100*k = 500) plus the verify cost is
-- larger than the full-scan cost (10 rows). The cost block returns nullopt
-- and the plan stays untouched (no Union step). The query result is still
-- correct because the fallback scan computes the same ranking. This case is
-- distinct from 04143 (match declined): here the algorithm matches but the
-- framework prefers the full scan.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_small_cppdiskann SYNC;
DROP TABLE IF EXISTS src_small_cppdiskann;

CREATE TABLE src_small_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_small_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_small_cppdiskann
ON src_small_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_small_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_small_cppdiskann SYNC;
DROP TABLE src_small_cppdiskann;
