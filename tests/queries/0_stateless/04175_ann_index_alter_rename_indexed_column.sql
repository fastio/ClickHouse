SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_alter_rename_idx SYNC;
DROP TABLE IF EXISTS mi_alter_rename_src;

CREATE TABLE mi_alter_rename_src (k UInt64, embedding Array(Float32), extra String)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_alter_rename_idx
ON mi_alter_rename_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

ALTER TABLE mi_alter_rename_src RENAME COLUMN embedding TO embedding2; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE mi_alter_rename_idx SYNC;
ALTER TABLE mi_alter_rename_src RENAME COLUMN embedding TO embedding2;

SELECT
    countIf(name = 'embedding') AS old_name_exists,
    countIf(name = 'embedding2') AS new_name_exists
FROM system.columns
WHERE database = currentDatabase()
    AND table = 'mi_alter_rename_src'
    AND name IN ('embedding', 'embedding2');

DROP TABLE mi_alter_rename_src;
