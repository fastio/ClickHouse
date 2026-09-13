-- Adapted from ByConity tests/queries/4_cnch_stateless/10123_merge_disable_prefetcher_for_map.sql
-- Source revision: 0ec8e61acc287afe34f4700fe510661ae28415fa
-- `CnchMergeTree` becomes `MergeTree`; BYTE/default maps use `with_key_columns`, KV maps use `basic`.
-- Brace access uses bracket access. Original reference results are retained to expose semantic differences.
-- Use `VALUES` for the two original JSON rows so the client cannot consume subsequent SQL as input.
SET optimize_functions_to_subcolumns = 1;
SET allow_suspicious_low_cardinality_types = 1;
SET max_threads = 1;
SET mutations_sync = 1;
DROP TABLE IF EXISTS only_map_col;
CREATE TABLE only_map_col (m Map(String, String)) Engine=MergeTree() ORDER BY tuple() SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO only_map_col VALUES ({'name': 'cnch'});
INSERT INTO only_map_col VALUES ({'name': 'bytehouse'});
SELECT * FROM only_map_col ORDER BY m['name'];

SYSTEM START MERGES only_map_col;

OPTIMIZE TABLE only_map_col FINAL;


SELECT '-- AFTER MERGE --';

SELECT * FROM only_map_col ORDER BY m['name'];

DROP TABLE only_map_col;
