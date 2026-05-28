SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_rename_source_idx SYNC;
DROP TABLE IF EXISTS mi_rename_source_src;
DROP TABLE IF EXISTS mi_rename_source_new;

CREATE TABLE mi_rename_source_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_rename_source_idx
ON mi_rename_source_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SET check_referential_table_dependencies = 1;
RENAME TABLE mi_rename_source_src TO mi_rename_source_new; -- { serverError HAVE_DEPENDENT_OBJECTS }

SELECT
    countIf(name = 'mi_rename_source_src') AS old_name_exists,
    countIf(name = 'mi_rename_source_new') AS new_name_exists
FROM system.tables
WHERE database = currentDatabase()
    AND name IN ('mi_rename_source_src', 'mi_rename_source_new');

DROP TABLE mi_rename_source_idx SYNC;
DROP TABLE mi_rename_source_src;
