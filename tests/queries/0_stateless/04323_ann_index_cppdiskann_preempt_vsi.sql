-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- Result smoke test for the vector_similarity_index + ANNIndex
-- coexistence case with force_using_ann_index = 1. Event-based tests
-- pin the actual yield/preempt behavior.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_using_ann_index = 1;

DROP TABLE IF EXISTS mi_pre_cppdiskann SYNC;
DROP TABLE IF EXISTS src_pre_cppdiskann;

CREATE TABLE src_pre_cppdiskann (
    k UInt64,
    embedding Array(Float32),
    INDEX vsi embedding TYPE vector_similarity('hnsw', 'L2Distance', 4))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pre_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(20);

CREATE REFLECTION mi_pre_cppdiskann
ON src_pre_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_pre_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pre_cppdiskann SYNC;
DROP TABLE src_pre_cppdiskann;
