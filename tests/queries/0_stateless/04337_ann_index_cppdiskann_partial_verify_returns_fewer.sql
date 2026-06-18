-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- Locks the "no refetch on partial verify" contract: WHERE filtering after
-- the MI search may shrink the result below LIMIT, and the engine must not
-- top it up by re-running the search. The query vector places the smallest
-- distances near k = 42, so WHERE k > 50 keeps only the ~5 candidates with
-- k = 51..56 inside the default overfetch window. A regression that adds a
-- silent refetch would change this row count and fail against .reference.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_pv_cppdiskann SYNC;
DROP TABLE IF EXISTS src_pv_cppdiskann;

CREATE TABLE src_pv_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pv_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE REFLECTION mi_pv_cppdiskann
ON src_pv_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_pv_cppdiskann
WHERE k > 50
ORDER BY L2Distance(embedding, [42.0, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_pv_cppdiskann SYNC;
DROP TABLE src_pv_cppdiskann;
