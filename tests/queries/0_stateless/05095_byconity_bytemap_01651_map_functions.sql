-- Adapted from ByConity tests/queries/4_cnch_stateless/01651_map_functions.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
-- Port only the first two stored-map blocks through `mapValues`; retain expected type errors with local codes.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;


select 'String type';
drop table if exists table_map;
create table table_map (a Map(String, String), b String) engine = MergeTree order by tuple() SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
insert into table_map values ({'name':'zhangsan', 'age':'10'}, 'name'), ({'name':'lisi', 'gender':'female'},'age');
select mapContains(a, 'name') from table_map;
select mapContains(a, 'gender') from table_map;
select mapContains(a, 'abc') from table_map;
select mapContains(a, b) from table_map;
select mapContains(a, 10) from table_map; -- { serverError NO_COMMON_TYPE }
select arraySort(mapKeys(a)) from table_map;
drop table if exists table_map;

CREATE TABLE table_map (a Map(UInt8, Int), b UInt8, c UInt32) engine = MergeTree order by tuple() SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
insert into table_map select map(number, number), number, number from numbers(1000, 3);
select mapContains(a, b), mapContains(a, c), mapContains(a, 233) from table_map;
select mapContains(a, 'aaa') from table_map; -- { serverError NO_COMMON_TYPE }
select mapContains(b, 'aaa') from table_map; -- { serverError 43 }
select arraySort(mapKeys(a)) from table_map;
select arraySort(mapValues(a)) from table_map;
drop table if exists table_map;


