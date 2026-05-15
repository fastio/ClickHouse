SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_build_success SYNC;
DROP TABLE IF EXISTS src_build_success;

CREATE TABLE src_build_success (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_build_success
ON src_build_success (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex;

INSERT INTO src_build_success
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(8);

SELECT source_database = database AS source_database_match, source_table = 'src_build_success' AS source_table_match
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_build_success';

SYSTEM SYNC MATERIALIZED INDEX mi_build_success;

SELECT
    materialized_index_part_count > 0 AS has_part,
    total_rows = 8 AS rows_match,
    total_bytes_on_disk > 0 AS bytes_counted
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_build_success';

SELECT
    count() > 0 AS has_active_part,
    countIf(active) = count() AS all_active,
    countIf(match(name, '^materialized-index-build_[0-9]+_[0-9]+_0$')) = count() AS legal_names
FROM system.parts
WHERE database = currentDatabase() AND match(name, '^materialized-index-build_[0-9]+_[0-9]+_0$');

DROP TABLE mi_build_success SYNC;
DROP TABLE src_build_success;
