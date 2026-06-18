-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- Single MI on a large source: algorithm cost (100*k = 500) plus verify cost
-- (k = 5) is well below the fallback full-scan (10000 rows). The optimizer
-- should choose the MI path whenever the background build has committed,
-- and the fallback scan otherwise. Both paths produce the same top-5 result.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_one_cppdiskann SYNC;
DROP TABLE IF EXISTS src_one_cppdiskann;

CREATE TABLE src_one_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_one_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_one_cppdiskann
ON src_one_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_one_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_one_cppdiskann SYNC;
DROP TABLE src_one_cppdiskann;
