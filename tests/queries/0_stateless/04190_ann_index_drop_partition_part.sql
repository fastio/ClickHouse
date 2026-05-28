-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_drop_partition_part SYNC;
DROP TABLE IF EXISTS src_drop_partition_part SYNC;

CREATE TABLE src_drop_partition_part
(
    p UInt64,
    k UInt64,
    embedding Array(Float32)
)
ENGINE = MergeTree
PARTITION BY p
ORDER BY (p, k)
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_drop_partition_part
SELECT 0, number, [number * 1.0, 0, 0, 0]
FROM numbers(16);

INSERT INTO src_drop_partition_part
SELECT 1, number, [1000.0 + number, 0, 0, 0]
FROM numbers(16);

CREATE REFLECTION mi_drop_partition_part
ON src_drop_partition_part (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1;

SYSTEM SYNC REFLECTION mi_drop_partition_part;

SELECT
    ann_index_part_count >= 2 AS has_parts_before_drop,
    total_rows = 32 AS rows_before_drop
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_drop_partition_part';

SYSTEM STOP REFLECTION BUILDS mi_drop_partition_part;

ALTER TABLE mi_drop_partition_part DROP PARTITION 0;

SELECT
    countIf(rows > 0) = 1 AS one_part_after_partition_drop,
    sum(rows) = 16 AS rows_after_partition_drop
FROM system.ann_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_partition_part' AND active;

DROP TABLE mi_drop_partition_part SYNC;
DROP TABLE src_drop_partition_part SYNC;
