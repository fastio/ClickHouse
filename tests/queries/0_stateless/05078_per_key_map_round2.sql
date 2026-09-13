-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_wide;
DROP TABLE IF EXISTS t_per_key_edges;
DROP TABLE IF EXISTS t_per_key_lc;
DROP TABLE IF EXISTS t_per_key_attach_src;
DROP TABLE IF EXISTS t_per_key_attach_dst;
DROP TABLE IF EXISTS t_basic_attach_src;
DROP TABLE IF EXISTS t_basic_attach_dst;

SELECT 'force wide';
CREATE TABLE t_per_key_wide
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '200G',
    min_rows_for_wide_part = 1000000000;

INSERT INTO t_per_key_wide VALUES (1, {'a': 1});
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_per_key_wide' AND active;

SELECT 'duplicate key first match';
CREATE TABLE t_per_key_edges
(
    id UInt64,
    m Map(String, UInt64),
    s Map(String, String),
    arr Map(String, Array(String))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO t_per_key_edges VALUES (1, map('a', 1, 'a', 2), map('a', 'x', 'a', 'y'), map('a', ['x'], 'a', ['y']));
SELECT m, m['a'] FROM t_per_key_edges WHERE id = 1;
SELECT s, s['a'] FROM t_per_key_edges WHERE id = 1;
SELECT arr, arr['a'] FROM t_per_key_edges WHERE id = 1;

SELECT 'zero empty-string empty-array vs missing';
INSERT INTO t_per_key_edges VALUES (2, {'a': 0, 'b': 1}, {'a': '', 'b': 'x'}, {'a': [], 'b': ['x']});
INSERT INTO t_per_key_edges VALUES (3, map(), map(), map());
SELECT
    id,
    m,
    m['a'],
    isNull(m['a']),
    s['a'],
    isNull(s['a']),
    arr['a'],
    isNull(arr['a'])
FROM t_per_key_edges
WHERE id IN (2, 3)
ORDER BY id;

SELECT 'mapKeys mapValues and select both';
SELECT id, m, m['a'], mapKeys(m), mapValues(m) FROM t_per_key_edges WHERE id = 2;

SELECT 'low cardinality first-block keys';
CREATE TABLE t_per_key_lc
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

INSERT INTO t_per_key_lc VALUES (1, map('a', 'x', 'b', 'y')), (2, map('a', 'z'));
SELECT toTypeName(m['a']), m['a'], m['b'], m['c'] FROM t_per_key_lc ORDER BY id;

SELECT 'attach basic to with_key_columns';
CREATE TABLE t_basic_attach_src (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_basic_attach_src VALUES (1, {'a': 1});
CREATE TABLE t_per_key_attach_dst (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns';
ALTER TABLE t_per_key_attach_dst ATTACH PARTITION tuple() FROM t_basic_attach_src; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT 'attach with_key_columns to basic';
CREATE TABLE t_per_key_attach_src (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns';
INSERT INTO t_per_key_attach_src VALUES (1, {'a': 1});
CREATE TABLE t_basic_attach_dst (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_basic_attach_dst ATTACH PARTITION tuple() FROM t_per_key_attach_src; -- { serverError INCOMPATIBLE_COLUMNS }

SELECT 'attach same with_key_columns format';
ALTER TABLE t_per_key_attach_dst ATTACH PARTITION tuple() FROM t_per_key_attach_src;
SELECT m['a'] FROM t_per_key_attach_dst;

DROP TABLE t_per_key_wide;
DROP TABLE t_per_key_edges;
DROP TABLE t_per_key_lc;
DROP TABLE t_per_key_attach_src;
DROP TABLE t_per_key_attach_dst;
DROP TABLE t_basic_attach_src;
DROP TABLE t_basic_attach_dst;
