-- Adapted from ByConity tests/queries/4_cnch_stateless/02014_map_different_keys.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Duplicate queries ported from brace access are omitted.
-- Missing keys return defaults for constant and `basic` maps, and `NULL` for `with_key_columns` maps.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
SELECT '...test const maps...';

WITH map(1, 2, 3, 4) AS m SELECT m[number] FROM numbers(5);
WITH map('1', 2, '3', 4) AS m SELECT m[toString(number)] FROM numbers(5);

WITH map(1, 2, 3, 4) AS m SELECT m[3];
WITH map('1', 2, '3', 4) AS m SELECT m['3'];

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(i1 UInt64, i2 Int32, m1 Map(UInt32, String), m2 Map(Int8, String), m3 Map(Int128, String)) ENGINE = MergeTree order by tuple() SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map_02014 VALUES (1, -1, map(1, 'foo', 2, 'bar'), map(-1, 'foo', 1, 'bar'), map(-1, 'foo', 1, 'bar'));

SELECT '...test int keys for KV map...';

SELECT m1[i1], m2[i1], m3[i1] FROM t_map_02014;
SELECT m1[i2], m2[i2], m3[i2] FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(i1 UInt64, i2 Int32, m1 Map(UInt32, String), m2 Map(Int8, String), m3 Map(Int128, String)) ENGINE = MergeTree order by tuple() SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map_02014 VALUES (1, -1, map(1, 'foo', 2, 'bar'), map(-1, 'foo', 1, 'bar'), map(-1, 'foo', 1, 'bar'));
SELECT '...test int keys for BYTE map...';

SELECT m1[i1], m2[i1], m3[i1] FROM t_map_02014;
SELECT m1[i2], m2[i2], m3[i2] FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(s String, fs FixedString(3), m1 Map(String, String), m2 Map(FixedString(3), String)) ENGINE = MergeTree order by tuple() SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map_02014 VALUES ('aaa', 'bbb', map('aaa', 'foo', 'bbb', 'bar'), map('aaa', 'foo', 'bbb', 'bar'));

SELECT '...test string keys for KV map...';

SELECT m1['aaa'], m2['aaa'] FROM t_map_02014;
SELECT m1['aaa'::FixedString(3)], m2['aaa'::FixedString(3)] FROM t_map_02014;
SELECT m1['ccc'::FixedString(3)], m2['ccc'::FixedString(3)] FROM t_map_02014;
SELECT m1[s], m2[s] FROM t_map_02014;
SELECT m1[fs], m2[fs] FROM t_map_02014;
SELECT length(m2['aaa'::FixedString(4)]) FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;

CREATE TABLE t_map_02014(s String, fs FixedString(3), m1 Map(String, String), m2 Map(FixedString(3), String)) ENGINE = MergeTree order by tuple() SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_map_02014 VALUES ('aaa', 'bbb', map('aaa', 'foo', 'bbb', 'bar'), map('aaa', 'foo', 'bbb', 'bar'));

SELECT '...test string keys for BYTE map...';

SELECT m1['aaa'], m2['aaa'] FROM t_map_02014;
SELECT m1['aaa'::FixedString(3)], m2['aaa'::FixedString(3)] FROM t_map_02014;
SELECT m1['ccc'::FixedString(3)], m2['ccc'::FixedString(3)] FROM t_map_02014;
SELECT m1[s], m2[s] FROM t_map_02014;
SELECT m1[fs], m2[fs] FROM t_map_02014;
SELECT length(m2['aaa'::FixedString(4)]) FROM t_map_02014;

DROP TABLE IF EXISTS t_map_02014;
