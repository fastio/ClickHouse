-- Adapted from ByConity tests/queries/4_cnch_stateless/00970_test_get_map_keys.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
-- Adapt `currentDatabase` to its local zero-argument signature.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
drop table if exists test_map;

CREATE TABLE test_map (`event_date` Date, `int_map` Map(UInt32, String), `string_map` Map(String, String), `float_map` Map(Float64, String), `date_map` Map(Date, String), `date_time_map` Map(DateTime, String)) ENGINE = MergeTree PARTITION BY event_date ORDER BY event_date SETTINGS index_granularity = 8192, map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

insert into test_map values('2001-01-01', {1:'1'}, {'1':'1'},{1.1:'1'},{'2001-01-01':'1'},{'2001-01-01 00:00:00':'1'});

select getMapKeys(currentDatabase(), 'test_map', 'int_map');
select getMapKeys(currentDatabase(), 'test_map', 'string_map');
select getMapKeys(currentDatabase(), 'test_map', 'float_map');
select getMapKeys(currentDatabase(), 'test_map', 'date_map');
select getMapKeys(currentDatabase(), 'test_map', 'date_time_map');

select getMapKeys(currentDatabase(), 'test_map', 'int_map', '2001.*01.*01');
select getMapKeys(currentDatabase(), 'test_map', 'int_map', '2001.*01.*02');

drop table test_map;
