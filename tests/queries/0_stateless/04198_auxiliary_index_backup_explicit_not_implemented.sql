-- Tags: no-fasttest

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_backup_explicit_idx SYNC;
DROP TABLE IF EXISTS mi_backup_explicit_src SYNC;

CREATE TABLE mi_backup_explicit_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_backup_explicit_idx
ON mi_backup_explicit_src (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

BACKUP TABLE mi_backup_explicit_src TO Null WITH AUXILIARY INDEXES; -- { serverError NOT_IMPLEMENTED }

DROP TABLE mi_backup_explicit_idx SYNC;
DROP TABLE mi_backup_explicit_src SYNC;
