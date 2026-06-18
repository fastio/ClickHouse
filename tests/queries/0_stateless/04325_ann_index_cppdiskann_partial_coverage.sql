-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- Result smoke test for a multi-part source table with a ANNIndex
-- definition. This test intentionally accepts either a partial rewrite or a
-- fallback scan; event-based tests pin the actual partial-coverage fast path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_pc_cppdiskann SYNC;
DROP TABLE IF EXISTS src_pc_cppdiskann;

CREATE TABLE src_pc_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pc_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

INSERT INTO src_pc_cppdiskann
SELECT 100 + number, [(100 + number) * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE REFLECTION mi_pc_cppdiskann
ON src_pc_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_pc_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pc_cppdiskann SYNC;
DROP TABLE src_pc_cppdiskann;
