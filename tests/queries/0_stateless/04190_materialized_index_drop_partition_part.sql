-- Tags: no-fasttest

SET allow_experimental_materialized_index = 1;

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

CREATE MATERIALIZED INDEX mi_drop_partition_part
ON src_drop_partition_part (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 60,
         materialized_index_build_min_rows = 1,
         materialized_index_build_min_parts = 1;

SYSTEM SYNC MATERIALIZED INDEX mi_drop_partition_part;

SELECT
    materialized_index_part_count >= 2 AS has_parts_before_drop,
    total_rows = 32 AS rows_before_drop
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_drop_partition_part';

SYSTEM STOP MATERIALIZED INDEX BUILDS mi_drop_partition_part;

ALTER TABLE mi_drop_partition_part DROP PARTITION 0;

SELECT
    materialized_index_part_count = 1 AS one_part_after_partition_drop,
    total_rows = 16 AS rows_after_partition_drop
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_drop_partition_part';

DROP TABLE mi_drop_partition_part SYNC;
DROP TABLE src_drop_partition_part SYNC;
