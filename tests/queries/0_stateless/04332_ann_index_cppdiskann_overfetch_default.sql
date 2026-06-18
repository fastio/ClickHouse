-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- ann_index_overfetch_factor defaults to 4: candidate_limit = top_k * 4.
-- The overfetched candidates still produce the correct top-10 ranking regardless
-- of whether the MI fast path or the fallback scan executes the query.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_of_def_cppdiskann SYNC;
DROP TABLE IF EXISTS src_of_def_cppdiskann;

CREATE TABLE src_of_def_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_of_def_cppdiskann
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(100);

CREATE REFLECTION mi_of_def_cppdiskann
ON src_of_def_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

SELECT k FROM src_of_def_cppdiskann
ORDER BY L2Distance(embedding, [41.7, 0, 0, 0])
LIMIT 10;

DROP TABLE mi_of_def_cppdiskann SYNC;
DROP TABLE src_of_def_cppdiskann;
