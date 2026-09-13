-- Adapted from ByConity tests/queries/4_cnch_stateless/10027_map_files_and_extract_map_functions.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Verify actual `with_key_columns` files and logical column/key metadata; file names differ from ByConity.
-- Adapt `currentDatabase` to its local zero-argument signature.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
DROP TABLE IF EXISTS t_10027_map_files_and_extract_map_functions;
CREATE TABLE t_10027_map_files_and_extract_map_functions ( i Int, d Date, ms Map(String, String), mi Map(Int, Int), mf Map(Float, Float), md Map(Date, Date)) ENGINE = MergeTree ORDER BY i PARTITION BY d SETTINGS compress_marks = 1, string_serialization_version = 'with_size_stream', map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

SELECT 'CASE 0';
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {}, {}, {});
SELECT extractMapColumn(''), extractMapColumn('0'), extractMapColumn('__M__1.bin'), extractMapColumn('__M__1.bin1');
SELECT arrayJoin(_part_map_files as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f;
SELECT arrayJoin(_map_column_keys as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f SETTINGS max_threads = 1;

SELECT 'CASE 1';
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, 0, {}, {}, {}, {});
SELECT DISTINCT arrayJoin(_part_map_files as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f SETTINGS max_threads = 1;
SELECT DISTINCT arrayJoin(_map_column_keys as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f SETTINGS max_threads = 1;

SELECT 'CASE 2';
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {1: 1}, {1.: 1.0}, {'2020-10-10': '2020-10-10'});
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, 0, {'k1': 'v1'}, {1: 1}, {1.23456789: 1.0}, {});
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, 0, {'k''2': 'v2'}, {2: 2}, {1.0: 1.0}, {});
SELECT DISTINCT arrayJoin(_part_map_files as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f SETTINGS max_threads = 1;
SELECT DISTINCT arrayJoin(_map_column_keys as fs) as f FROM t_10027_map_files_and_extract_map_functions ORDER BY f SETTINGS max_threads = 1;

-- File-to-key parsing is specific to the ByConity layout. Enumerate logical keys directly.
SELECT 'CASE 3';
SELECT DISTINCT t.1 AS column_name, t.2 AS key_name
FROM (SELECT arrayJoin(_map_column_keys) AS t FROM t_10027_map_files_and_extract_map_functions)
ORDER BY column_name, key_name;

SELECT 'CASE 4';
SELECT t.1 AS column_name, arraySort(groupUniqArray(t.2)) AS keys
FROM (SELECT arrayJoin(_map_column_keys) AS t FROM t_10027_map_files_and_extract_map_functions)
GROUP BY column_name ORDER BY column_name;

SELECT 'CASE 5';
SELECT DISTINCT t.1 as c, t.2 as k FROM
(
SELECT arrayJoin(_map_column_keys as ts) as t FROM t_10027_map_files_and_extract_map_functions SETTINGS early_limit_for_map_virtual_columns = 1
)
ORDER BY c, k;

SELECT 'CASE 6';
SELECT getMapKeys(currentDatabase(), 't_10027_map_files_and_extract_map_functions', 'ms');
SELECT getMapKeys(currentDatabase(), 't_10027_map_files_and_extract_map_functions', 'ms', '');

SELECT 'CASE 7';
SELECT getMapKeys(currentDatabase(), 't_10027_map_files_and_extract_map_functions', 'ms', '20000101');

SELECT 'CASE 8';
INSERT INTO t_10027_map_files_and_extract_map_functions VALUES (0, toDate('2023-03-03'), {'k3': 'v3'}, {}, {}, {});
SELECT getMapKeys(currentDatabase(), 't_10027_map_files_and_extract_map_functions', 'ms', '20230303');

DROP TABLE t_10027_map_files_and_extract_map_functions;
