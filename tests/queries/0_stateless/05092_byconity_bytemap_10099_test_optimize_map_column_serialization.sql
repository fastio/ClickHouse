-- Adapted from ByConity tests/queries/4_cnch_stateless/10099_test_optimize_map_column_serialization.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
-- ByConity toggles `optimize_map_column_serialization`; this setting has no local equivalent.
-- Retain both data repetitions, without claiming coverage of two local writer strategies.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;


-- value type will automatically adding nullable
DROP TABLE IF EXISTS t_10099_test_optimize_map_column_serialization;
CREATE TABLE t_10099_test_optimize_map_column_serialization (s String, m Map(String, Int64)) engine = MergeTree order by s settings index_granularity = 2, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m['k1'], m['k2'] FROM t_10099_test_optimize_map_column_serialization;

-- value type low cardinality
DROP TABLE IF EXISTS t_10099_test_optimize_map_column_serialization;
CREATE TABLE t_10099_test_optimize_map_column_serialization (s String, m Map(String, LowCardinality(Nullable(Int64)))) engine = MergeTree order by s settings index_granularity = 2, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m['k1'], m['k2'] FROM t_10099_test_optimize_map_column_serialization;


-- value type will automatically adding nullable
DROP TABLE IF EXISTS t_10099_test_optimize_map_column_serialization;
CREATE TABLE t_10099_test_optimize_map_column_serialization (s String, m Map(String, Int64)) engine = MergeTree order by s settings index_granularity = 2, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m['k1'], m['k2'] FROM t_10099_test_optimize_map_column_serialization;

-- value type low cardinality
DROP TABLE IF EXISTS t_10099_test_optimize_map_column_serialization;
CREATE TABLE t_10099_test_optimize_map_column_serialization (s String, m Map(String, LowCardinality(Nullable(Int64)))) engine = MergeTree order by s settings index_granularity = 2, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_10099_test_optimize_map_column_serialization VALUES ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2}) ('s1', {'k1': 0, 'k2': 1, 'k1': 2});
SELECT m['k1'], m['k2'] FROM t_10099_test_optimize_map_column_serialization;

DROP TABLE IF EXISTS t_10099_test_optimize_map_column_serialization;
