-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- When the query's reference vector dimension does not match the MI dim, the
-- algorithm's match() returns std::nullopt and the optimizer must yield to
-- the fallback scan instead of attaching empty hints.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_decl_cppdiskann SYNC;
DROP TABLE IF EXISTS src_decl_cppdiskann;

CREATE TABLE src_decl_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_decl_cppdiskann
SELECT number, [number * 1.0, 0]
FROM numbers(10);

CREATE REFLECTION mi_decl_cppdiskann
ON src_decl_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 1;

-- Reference vector is 2-dim while the MI was built for dim = 4.
SELECT k FROM src_decl_cppdiskann
ORDER BY L2Distance(embedding, [3.7, 0])
LIMIT 5;

DROP TABLE mi_decl_cppdiskann SYNC;
DROP TABLE src_decl_cppdiskann;
