-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- force_ann_index pointing at a non-existent MI must not throw. The
-- optimizer logs a warning and falls back to cost-based selection over the
-- candidates that survived `disable_ann_index`. Result remains the
-- correct top-K.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_real_cppdiskann SYNC;
DROP TABLE IF EXISTS src_invalid_cppdiskann;

CREATE TABLE src_invalid_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_invalid_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10000);

CREATE REFLECTION mi_real_cppdiskann
ON src_invalid_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SET force_ann_index = 'mi_does_not_exist_cppdiskann';

SELECT k FROM src_invalid_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_real_cppdiskann SYNC;
DROP TABLE src_invalid_cppdiskann;
