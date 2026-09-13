-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_bulk_lc;
DROP TABLE IF EXISTS t_per_key_bulk_write;

SELECT 'lc missing key merge';
CREATE TABLE t_per_key_bulk_lc
(
    id UInt64,
    m Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

SYSTEM STOP MERGES t_per_key_bulk_lc;
INSERT INTO t_per_key_bulk_lc VALUES (1, {'a': 'x', 'b': ''});
INSERT INTO t_per_key_bulk_lc VALUES (2, {'b': 'y', 'c': 'z'});

SYSTEM START MERGES t_per_key_bulk_lc;
OPTIMIZE TABLE t_per_key_bulk_lc FINAL;

SELECT
    id,
    m,
    m['a'],
    isNull(m['a']),
    m['b'],
    isNull(m['b']),
    m['c'],
    isNull(m['c'])
FROM t_per_key_bulk_lc
ORDER BY id;

SELECT toTypeName(m['a']) FROM t_per_key_bulk_lc LIMIT 1;
CHECK TABLE t_per_key_bulk_lc SETTINGS check_query_single_value_result = 1;

SELECT 'cross block write pivot';
CREATE TABLE t_per_key_bulk_write
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
    index_granularity = 2,
    index_granularity_bytes = 0;

INSERT INTO t_per_key_bulk_write SELECT
    number,
    if(
        number < 2,
        map('a', number, 'a', 99),
        map('a', number, 'b', number + 10))
FROM numbers(4)
SETTINGS max_block_size = 2, max_insert_block_size = 2, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

SELECT id, m, m['a'], isNull(m['a']), m['b'], isNull(m['b']) FROM t_per_key_bulk_write ORDER BY id;
CHECK TABLE t_per_key_bulk_write SETTINGS check_query_single_value_result = 1;

DROP TABLE t_per_key_bulk_lc;
DROP TABLE t_per_key_bulk_write;
