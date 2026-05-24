SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_alter_drop_idx SYNC;
DROP TABLE IF EXISTS mi_alter_drop_src;

CREATE TABLE mi_alter_drop_src (k UInt64, embedding Array(Float32), extra String)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_alter_drop_idx
ON mi_alter_drop_src (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

ALTER TABLE mi_alter_drop_src DROP COLUMN embedding; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE mi_alter_drop_idx SYNC;
ALTER TABLE mi_alter_drop_src DROP COLUMN embedding;

SELECT count()
FROM system.columns
WHERE database = currentDatabase() AND table = 'mi_alter_drop_src' AND name = 'embedding';

DROP TABLE mi_alter_drop_src;
