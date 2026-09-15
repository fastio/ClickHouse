-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_pk_compact;
DROP TABLE IF EXISTS t_pk_compact_lc;
DROP TABLE IF EXISTS t_pk_compact_thresh;
DROP TABLE IF EXISTS t_pk_compact_mix;

SELECT 'compact part type';
CREATE TABLE t_pk_compact
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    max_bytes_for_compact_map_key_columns = 67108864;

-- Two blocks: key 'b' first appears in the second insert (cross-block new key).
INSERT INTO t_pk_compact VALUES (1, {'a': 1});
INSERT INTO t_pk_compact VALUES (2, {'a': 2, 'b': 10}), (3, {'b': 0}), (4, map());

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact' AND active ORDER BY name;

SELECT 'full and single key, zero vs missing';
SELECT id, m, m['a'], m['b'], isNull(m['a']), isNull(m['b']) FROM t_pk_compact ORDER BY id;

SELECT 'map functions';
SELECT id, mapKeys(m), mapValues(m), mapContains(m, 'a'), mapContains(m, 'b') FROM t_pk_compact ORDER BY id;

SELECT 'check table';
CHECK TABLE t_pk_compact SETTINGS check_query_single_value_result = 1;

SELECT 'optimize final -> wide';
OPTIMIZE TABLE t_pk_compact FINAL;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact' AND active;
SELECT id, m, m['a'], m['b'] FROM t_pk_compact ORDER BY id;

SELECT 'low cardinality compact';
CREATE TABLE t_pk_compact_lc
(
    id UInt64,
    m Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000;
INSERT INTO t_pk_compact_lc VALUES (1, map('a', 'x', 'b', 'y')), (2, map('a', 'z'));
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_lc' AND active;
SELECT id, toTypeName(m['a']), m['a'], m['b'], m['c'] FROM t_pk_compact_lc ORDER BY id;

SELECT 'byte threshold forces wide';
CREATE TABLE t_pk_compact_thresh
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    max_bytes_for_compact_map_key_columns = 0;
INSERT INTO t_pk_compact_thresh VALUES (1, {'a': 1});
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_pk_compact_thresh' AND active;

SELECT 'max keys overflow';
CREATE TABLE t_pk_compact_mix
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    max_keys_in_map = 2;
INSERT INTO t_pk_compact_mix VALUES (1, {'a': 1, 'b': 2, 'c': 3}); -- { serverError LIMIT_EXCEEDED }
SELECT count() FROM t_pk_compact_mix;

SELECT 'write marks required';
CREATE TABLE t_pk_compact_nomarks
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    write_marks_for_substreams_in_compact_parts = 0; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE t_pk_compact;
DROP TABLE t_pk_compact_lc;
DROP TABLE t_pk_compact_thresh;
DROP TABLE t_pk_compact_mix;
