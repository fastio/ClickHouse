-- Tags: no-fasttest, no-cpu-aarch64, use-cppdiskann

SET allow_experimental_ann_index = 1;
SET ann_index_require_match = 1;
SET force_ann_index = 'mi_cppdiskann';

DROP TABLE IF EXISTS mi_cppdiskann SYNC;
DROP TABLE IF EXISTS src_cppdiskann;

CREATE TABLE src_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_cppdiskann
ON src_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 16,
         diskann_max_degree = 16,
         diskann_l_build = 32,
         diskann_num_threads = 2,
         diskann_pq_chunks = 2,
         diskann_build_quantization = 'PQ_2',
         diskann_pq_code_budget_gb_ratio = 0.125,
         diskann_build_ram_limit_gb = 1,
         diskann_search_cache_budget_gb_ratio = 0.1,
         diskann_disk_pq_dims = 2,
         ann_index_build_min_rows = 1;

SELECT
    impl,
    algorithm_params['dimension'],
    algorithm_params['pruned_degree'],
    algorithm_params['build_quantization'],
    algorithm_params['pq_code_budget_gb_ratio'],
    algorithm_params['search_cache_budget_gb_ratio'],
    algorithm_params['disk_pq_dims']
FROM system.reflection_ann_indexes
WHERE database = currentDatabase() AND reflection_name = 'mi_cppdiskann';

INSERT INTO src_cppdiskann
SELECT number, [toFloat32(number), toFloat32(0), toFloat32(0), toFloat32(0)]
FROM numbers(64);

SYSTEM SYNC REFLECTION mi_cppdiskann;

SELECT k
FROM src_cppdiskann
ORDER BY L2Distance(embedding, [toFloat32(0), toFloat32(0), toFloat32(0), toFloat32(0)])
LIMIT 1;

DROP TABLE mi_cppdiskann SYNC;
DROP TABLE src_cppdiskann;
