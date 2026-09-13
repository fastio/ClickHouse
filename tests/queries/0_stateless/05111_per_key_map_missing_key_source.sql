-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_missing_src;
DROP TABLE IF EXISTS t_per_key_missing_src_off;
DROP TABLE IF EXISTS t_per_key_missing_src_lc;

SELECT 'skip missing key readers on';
CREATE TABLE t_per_key_missing_src
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
    index_granularity_bytes = 0,
    map_key_columns_merge_skip_missing_key_readers = 1;

SYSTEM STOP MERGES t_per_key_missing_src;
INSERT INTO t_per_key_missing_src VALUES (1, {'a': 0}), (2, {'a': 1, 'b': 2});
INSERT INTO t_per_key_missing_src VALUES (3, {'b': 3, 'c': 4}), (4, {'c': 5});
INSERT INTO t_per_key_missing_src VALUES (5, {'a': 6});

SYSTEM START MERGES t_per_key_missing_src;
OPTIMIZE TABLE t_per_key_missing_src FINAL;

SELECT id, m, m['a'], isNull(m['a']), m['b'], isNull(m['b']), m['c'], isNull(m['c'])
FROM t_per_key_missing_src
ORDER BY id;
CHECK TABLE t_per_key_missing_src SETTINGS check_query_single_value_result = 1;

SELECT 'skip missing key readers off';
CREATE TABLE t_per_key_missing_src_off
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
    index_granularity_bytes = 0,
    map_key_columns_merge_skip_missing_key_readers = 0;

SYSTEM STOP MERGES t_per_key_missing_src_off;
INSERT INTO t_per_key_missing_src_off VALUES (1, {'a': 0}), (2, {'a': 1, 'b': 2});
INSERT INTO t_per_key_missing_src_off VALUES (3, {'b': 3, 'c': 4}), (4, {'c': 5});
INSERT INTO t_per_key_missing_src_off VALUES (5, {'a': 6});
SYSTEM START MERGES t_per_key_missing_src_off;
OPTIMIZE TABLE t_per_key_missing_src_off FINAL;

SELECT id, m, m['a'], isNull(m['a']), m['b'], isNull(m['b']), m['c'], isNull(m['c'])
FROM t_per_key_missing_src_off
ORDER BY id;
CHECK TABLE t_per_key_missing_src_off SETTINGS check_query_single_value_result = 1;

SELECT 'lc missing key';
CREATE TABLE t_per_key_missing_src_lc
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
    min_rows_for_wide_part = 0,
    map_key_columns_merge_skip_missing_key_readers = 1;

SYSTEM STOP MERGES t_per_key_missing_src_lc;
INSERT INTO t_per_key_missing_src_lc VALUES (1, {'a': 'x', 'b': ''});
INSERT INTO t_per_key_missing_src_lc VALUES (2, {'b': 'y', 'c': 'z'});
SYSTEM START MERGES t_per_key_missing_src_lc;
OPTIMIZE TABLE t_per_key_missing_src_lc FINAL;

SELECT
    id,
    m,
    m['a'],
    isNull(m['a']),
    m['b'],
    isNull(m['b']),
    m['c'],
    isNull(m['c'])
FROM t_per_key_missing_src_lc
ORDER BY id;
CHECK TABLE t_per_key_missing_src_lc SETTINGS check_query_single_value_result = 1;

DROP TABLE t_per_key_missing_src;
DROP TABLE t_per_key_missing_src_off;
DROP TABLE t_per_key_missing_src_lc;
