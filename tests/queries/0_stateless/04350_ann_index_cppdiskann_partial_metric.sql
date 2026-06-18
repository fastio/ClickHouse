-- Tags: no-fasttest, no-cpu-aarch64, use-cppdiskann

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_partial_diskann_metric_cppdiskann';

DROP TABLE IF EXISTS mi_partial_diskann_metric_cppdiskann SYNC;
DROP TABLE IF EXISTS src_partial_diskann_metric_cppdiskann;

CREATE TABLE src_partial_diskann_metric_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_partial_diskann_metric_cppdiskann
SELECT number, [toFloat32(100 + number), 0, 0, 0]
FROM numbers(256);

CREATE REFLECTION mi_partial_diskann_metric_cppdiskann
ON src_partial_diskann_metric_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_partial_diskann_metric_cppdiskann;

INSERT INTO src_partial_diskann_metric_cppdiskann VALUES (100000, [3, 4, 0, 0]);

SELECT k, toUInt64(d) AS cppdiskann_metric_distance
FROM
(
    SELECT k, L2Distance(embedding, [0., 0., 0., 0.]) AS d
    FROM src_partial_diskann_metric_cppdiskann
    ORDER BY d
    LIMIT 1
);

DROP TABLE mi_partial_diskann_metric_cppdiskann SYNC;
DROP TABLE src_partial_diskann_metric_cppdiskann;
