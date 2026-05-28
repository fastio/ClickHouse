-- Tags: no-fasttest, no-parallel
-- Exercises DROP / RENAME on a AuxiliaryIndex while a Build task is paused in
-- `finish` (part written, Keeper commit not started). The storage must not hang
-- and catalog changes must remain consistent after the failpoint is released.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_drop_fp SYNC;
DROP TABLE IF EXISTS mi_rename_fp SYNC;
DROP TABLE IF EXISTS src_drop_fp;
DROP TABLE IF EXISTS src_rename_fp;

CREATE TABLE src_drop_fp (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE TABLE src_rename_fp (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_drop_fp
ON src_drop_fp (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 60,
         auxiliary_index_build_min_rows = 1,
         auxiliary_index_build_min_parts = 1;

CREATE REFLECTION mi_rename_fp
ON src_rename_fp (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 60,
         auxiliary_index_build_min_rows = 1,
         auxiliary_index_build_min_parts = 1;

SYSTEM ENABLE FAILPOINT auxiliary_index_build_pause_in_finish;

INSERT INTO src_drop_fp
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

SYSTEM WAIT FAILPOINT auxiliary_index_build_pause_in_finish PAUSE;

DROP REFLECTION mi_drop_fp SYNC;

SELECT count() AS drop_removed_catalog_entry
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_drop_fp';

SYSTEM DISABLE FAILPOINT auxiliary_index_build_pause_in_finish;

SYSTEM ENABLE FAILPOINT auxiliary_index_build_pause_in_finish;

INSERT INTO src_rename_fp
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

SYSTEM WAIT FAILPOINT auxiliary_index_build_pause_in_finish PAUSE;

RENAME TABLE mi_rename_fp TO mi_rename_fp_new;

SELECT count() AS rename_visible_under_new_name
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_rename_fp_new';

SELECT count() AS rename_old_name_gone
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_rename_fp';

SYSTEM DISABLE FAILPOINT auxiliary_index_build_pause_in_finish;

DROP TABLE mi_rename_fp_new SYNC;
DROP TABLE src_drop_fp;
DROP TABLE src_rename_fp;
