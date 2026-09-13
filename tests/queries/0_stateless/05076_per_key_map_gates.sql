-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS t_per_key_ok;
DROP TABLE IF EXISTS t_per_key_empty;
DROP TABLE IF EXISTS t_per_key_data;
DROP TABLE IF EXISTS t_basic_map;

SELECT 'create allowed types';
CREATE TABLE t_per_key_ok
(
    id UInt64,
    m_scalar Map(String, UInt64),
    m_array Map(String, Array(String)),
    m_lc Map(String, LowCardinality(Nullable(String)))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns';

SELECT engine, position(engine_full, 'with_key_columns') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_per_key_ok';

SELECT 'reject add projection';
ALTER TABLE t_per_key_ok ADD PROJECTION p (SELECT id, m_scalar ORDER BY id); -- { serverError SUPPORT_IS_DISABLED }

SELECT 'insert allowed types';
INSERT INTO t_per_key_ok VALUES (1, {'a': 1}, {'a': ['x']}, {'a': 'x'});
SELECT m_scalar['a'], m_array['a'], m_lc['a'] FROM t_per_key_ok;
SELECT toTypeName(m_array['a']) FROM t_per_key_ok;

SELECT 'reject stored Nullable(Array)';
CREATE TABLE t_nullable_array (x Nullable(Array(UInt8))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }
CREATE TABLE t_nullable_array_mem (x Nullable(Array(UInt8))) ENGINE = Memory; -- { serverError DATA_TYPE_CANNOT_BE_USED_IN_TABLES }

SELECT 'cast Nullable(Array) still parses';
SELECT toTypeName(CAST([1, 2] AS Nullable(Array(UInt8))));

SELECT 'default Map is unchanged';
CREATE TABLE t_basic_map (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_basic_map VALUES (1, {'a': 1});
SELECT m['a'] FROM t_basic_map;

SELECT 'reject nullable value';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, Nullable(UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

SELECT 'reject nested map value';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, Map(String, UInt64)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

SELECT 'reject tuple value';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, Tuple(UInt8, UInt8)))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError ILLEGAL_COLUMN }

SELECT 'reject summing engine';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, UInt64))
ENGINE = SummingMergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject mismatched versions';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns'; -- { serverError INVALID_SETTING_VALUE }

SELECT 'reject projection';
CREATE TABLE t_per_key_bad
(
    id UInt64,
    m Map(String, UInt64),
    PROJECTION p (SELECT id, m ORDER BY id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'alter empty table to with_key_columns';
CREATE TABLE t_per_key_empty (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
ALTER TABLE t_per_key_empty
    MODIFY SETTING map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns';
SELECT position(engine_full, 'with_key_columns') > 0
FROM system.tables
WHERE database = currentDatabase() AND name = 't_per_key_empty';

SELECT 'reject alter to with_key_columns when table has data';
CREATE TABLE t_per_key_data (id UInt64, m Map(String, UInt64)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_per_key_data VALUES (1, {'a': 1});
ALTER TABLE t_per_key_data
    MODIFY SETTING map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject order by map key';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY m['a']
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject partition by map key';
CREATE TABLE t_per_key_bad (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree PARTITION BY m['a'] ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject alter delete';
ALTER TABLE t_per_key_ok DELETE WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject alter update map';
ALTER TABLE t_per_key_ok UPDATE m_scalar = map('a', 2) WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'reject alter update map key';
ALTER TABLE t_per_key_ok UPDATE `m_scalar.key_a` = 2 WHERE id = 1; -- { serverError SUPPORT_IS_DISABLED }

SELECT 'lightweight delete and update other column';
CREATE TABLE t_per_key_mut
(
    id UInt64,
    v UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0;

INSERT INTO t_per_key_mut VALUES (1, 1, {'a': 1}), (2, 1, {'a': 2});
ALTER TABLE t_per_key_mut UPDATE v = 2 WHERE m['a'] = 1 SETTINGS mutations_sync = 2;
DELETE FROM t_per_key_mut WHERE id = 2;
SELECT id, v, m['a'] FROM t_per_key_mut ORDER BY id;

DROP TABLE t_per_key_ok;
DROP TABLE t_per_key_empty;
DROP TABLE t_per_key_data;
DROP TABLE t_per_key_mut;
DROP TABLE t_basic_map;
