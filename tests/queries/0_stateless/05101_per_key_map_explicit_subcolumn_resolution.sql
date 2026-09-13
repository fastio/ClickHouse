SET enable_analyzer = 1;

CREATE TABLE scoped_keys
(
    id UInt64,
    m Map(String, UInt64),
    a Nullable(UInt64) ALIAS m.key_a,
    expr Map(String, UInt64) ALIAS map('a', id),
    stored Map(String, UInt64) MATERIALIZED m
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO scoped_keys (id, m) VALUES (1, {'a': 11, 'a.null': 99}), (2, {}), (3, {'a': 0});

SELECT 'qualified and alias columns';
SELECT toTypeName(t.m.key_a), sum(t.m.key_a), count(t.m.key_a) FROM scoped_keys AS t GROUP BY toTypeName(t.m.key_a);
SELECT id, a, stored.key_a FROM scoped_keys ORDER BY id;
SELECT toTypeName(expr.key_a), sum(expr.key_a) FROM scoped_keys GROUP BY toTypeName(expr.key_a);

SELECT 'subquery scalar projection';
SELECT toTypeName(a), sum(a), count(a) FROM (SELECT m.key_a AS a FROM scoped_keys) GROUP BY toTypeName(a);
SELECT 'subquery map projection';
SELECT toTypeName(x.key_a), sum(x.key_a), count(x.key_a)
FROM (SELECT m AS x FROM scoped_keys) GROUP BY toTypeName(x.key_a);
SELECT toTypeName(x.key_a), sum(x.key_a), count(x.key_a)
FROM (SELECT map('a', id) AS x FROM scoped_keys) GROUP BY toTypeName(x.key_a);

SELECT 'dotted key';
SELECT id, m.key_a, `m.key_a.null`, isNull(m.key_a) FROM scoped_keys ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 1;

SELECT 'different storage sources';
CREATE TABLE scoped_basic (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
INSERT INTO scoped_basic VALUES (1, {}), (2, {'a': 5}), (3, {'a': 0});
SELECT p.id, toTypeName(p.m.key_a), toTypeName(b.m.key_a), p.m.key_a, b.m.key_a
FROM scoped_keys AS p INNER JOIN scoped_basic AS b ON p.id = b.id ORDER BY p.id;
DROP TABLE scoped_basic;
DROP TABLE scoped_keys;

SELECT 'full column name wins';
CREATE TABLE named_keys (m Map(String, UInt64), `m.key_a` String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO named_keys VALUES ({'a': 11}, 'literal');
SELECT toTypeName(`m.key_a`), `m.key_a` FROM named_keys;
DROP TABLE named_keys;

SELECT 'other subcolumns';
CREATE TABLE ordinary_subcolumns (t Tuple(a UInt64), arr Array(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ordinary_subcolumns VALUES ((7), [1, 2]);
SELECT toTypeName(t.a), t.a, arr.size0 FROM ordinary_subcolumns;
DROP TABLE ordinary_subcolumns;

SELECT 'other value types';
CREATE TABLE typed_keys
(
    id UInt64,
    s Map(String, String),
    a Map(String, Array(UInt64)),
    lc Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO typed_keys VALUES (1, {'a': 'value'}, {'a': [1, 2]}, {'a': 'value'}), (2, {}, {}, {});
SELECT toTypeName(s.key_a), toTypeName(a.key_a), toTypeName(lc.key_a) FROM typed_keys LIMIT 1;
SELECT id, s.key_a, a.key_a, lc.key_a FROM typed_keys ORDER BY id;
DROP TABLE typed_keys;
