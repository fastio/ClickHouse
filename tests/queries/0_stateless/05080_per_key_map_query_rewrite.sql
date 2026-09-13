-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_query;
DROP TABLE IF EXISTS t_basic_query;

CREATE TABLE t_per_key_query
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
    min_rows_for_wide_part = 0;

CREATE TABLE t_basic_query
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO t_per_key_query VALUES (1, {'a': 1, 'b': 2}), (2, {'b': 0}), (3, map());
INSERT INTO t_basic_query VALUES (1, {'a': 1, 'b': 2}), (2, {'b': 0}), (3, map());

SELECT 'with_key_columns types';
SELECT toTypeName(m['a']), toTypeName(m['missing']) FROM t_per_key_query LIMIT 1;
SELECT toTypeName(m[materialize('a')]) FROM t_per_key_query LIMIT 1;

SELECT 'basic types';
SELECT toTypeName(m['a']) FROM t_basic_query LIMIT 1;

SELECT 'with_key_columns values';
SELECT id, m['a'], isNull(m['a']), m['b'], isNull(m['b']) FROM t_per_key_query ORDER BY id;

SELECT 'mapContains matches isNotNull';
SELECT
    id,
    mapContains(m, 'a'),
    isNotNull(m['a']),
    mapContains(m, 'b'),
    isNotNull(m['b']),
    mapContains(m, 'c')
FROM t_per_key_query
ORDER BY id;

SELECT 'dynamic key';
SELECT id, m[materialize('a')], isNull(m[materialize('a')]) FROM t_per_key_query ORDER BY id;

SELECT 'optimize_functions_to_subcolumns = 0';
SELECT
    toTypeName(m['a']),
    id,
    m['a'],
    isNull(m['a']),
    mapContains(m, 'a')
FROM t_per_key_query
ORDER BY id
SETTINGS optimize_functions_to_subcolumns = 0;

SELECT 'basic missing is default';
SELECT id, m['a'], mapContains(m, 'a') FROM t_basic_query ORDER BY id;

DROP TABLE t_per_key_query;
DROP TABLE t_basic_query;
