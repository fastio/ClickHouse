-- Tags: no-fasttest

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_backup_explicit_idx SYNC;
DROP TABLE IF EXISTS mi_backup_explicit_src SYNC;

CREATE TABLE mi_backup_explicit_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_backup_explicit_idx
ON mi_backup_explicit_src (v)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex;

BACKUP TABLE mi_backup_explicit_src TO Null WITH MATERIALIZED INDEXES; -- { serverError NOT_IMPLEMENTED }

DROP TABLE mi_backup_explicit_idx SYNC;
DROP TABLE mi_backup_explicit_src SYNC;
