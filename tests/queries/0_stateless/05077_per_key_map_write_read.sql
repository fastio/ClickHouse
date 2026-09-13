-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_wr;
DROP TABLE IF EXISTS t_per_key_limit;

CREATE TABLE t_per_key_wr
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    index_granularity = 1;

SYSTEM STOP MERGES t_per_key_wr;

SELECT 'cross-block write';
INSERT INTO t_per_key_wr
SELECT
    number + 1,
    if(number = 0, map('a', toUInt64(1)), map('a', toUInt64(2), 'b', toUInt64(10)))
FROM numbers(2)
SETTINGS
    max_insert_block_size = 1,
    min_insert_block_size_rows = 1,
    min_insert_block_size_bytes = 0;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_wr' AND active;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_wr' AND active;

SELECT m FROM t_per_key_wr ORDER BY id;
SELECT toTypeName(m['a']), toTypeName(m['b']) FROM t_per_key_wr LIMIT 1;
SELECT m['a'], m['b'], m['c'] FROM t_per_key_wr ORDER BY id;

SELECT 'empty map and missing key';
INSERT INTO t_per_key_wr VALUES (3, map());
SELECT m, m['a'], m['b'] FROM t_per_key_wr WHERE id = 3;

SELECT 'check table';
CHECK TABLE t_per_key_wr SETTINGS check_query_single_value_result = 1;

SELECT 'max_keys_in_map';
CREATE TABLE t_per_key_limit
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    max_keys_in_map = 1;

INSERT INTO t_per_key_limit VALUES (1, map('a', 1, 'b', 2)); -- { serverError LIMIT_EXCEEDED }

DROP TABLE t_per_key_wr;
DROP TABLE t_per_key_limit;
