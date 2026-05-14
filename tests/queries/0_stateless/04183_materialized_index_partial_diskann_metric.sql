-- Tags: no-fasttest

SET allow_experimental_materialized_index = 1;
SET enable_materialized_index = 1;
SET force_materialized_index = 'mi_partial_diskann_metric';

DROP TABLE IF EXISTS mi_partial_diskann_metric SYNC;
DROP TABLE IF EXISTS src_partial_diskann_metric;

CREATE TABLE src_partial_diskann_metric (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_partial_diskann_metric
SELECT number, [toFloat32(100 + number), 0, 0, 0]
FROM numbers(256);

CREATE MATERIALIZED INDEX mi_partial_diskann_metric
ON src_partial_diskann_metric (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 60;

SYSTEM SYNC MATERIALIZED INDEX mi_partial_diskann_metric;

INSERT INTO src_partial_diskann_metric VALUES (100000, [3, 4, 0, 0]);

SELECT k, toUInt64(d) AS diskann_metric_distance
FROM
(
    SELECT k, L2Distance(embedding, [0., 0., 0., 0.]) AS d
    FROM src_partial_diskann_metric
    ORDER BY d
    LIMIT 1
);

DROP TABLE mi_partial_diskann_metric SYNC;
DROP TABLE src_partial_diskann_metric;
