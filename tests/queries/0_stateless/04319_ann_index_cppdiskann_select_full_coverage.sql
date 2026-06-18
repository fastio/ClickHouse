-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- TopK result smoke test for a source table that has a ANNIndex
-- definition. This test intentionally accepts either the indexed path or the
-- fallback scan; event-based tests pin the actual cppdiskann fast path.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_full_cppdiskann SYNC;
DROP TABLE IF EXISTS src_full_cppdiskann;

CREATE TABLE src_full_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_full_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(256);

CREATE REFLECTION mi_full_cppdiskann
ON src_full_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

-- The result must match the brute-force ranking regardless of whether the
-- background MI build has committed yet: if it has, the optimizer attaches
-- hints and the reader pulls rows by `_part_offset`; if not, the fallback
-- scan produces the same answer.
SELECT k FROM src_full_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_full_cppdiskann SYNC;
DROP TABLE src_full_cppdiskann;
