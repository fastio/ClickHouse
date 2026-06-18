-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- An explicit PREWHERE clause sets PrewhereInfo on the RFMT step; a predicate
-- that does not depend on the vector column can compose with the MI hint path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_pw_cppdiskann SYNC;
DROP TABLE IF EXISTS src_pw_cppdiskann;

CREATE TABLE src_pw_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pw_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_pw_cppdiskann
ON src_pw_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_pw_cppdiskann
PREWHERE k > 0
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pw_cppdiskann SYNC;
DROP TABLE src_pw_cppdiskann;
