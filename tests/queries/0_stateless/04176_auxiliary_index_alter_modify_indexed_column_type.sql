SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_alter_modify_idx SYNC;
DROP TABLE IF EXISTS mi_alter_modify_src;

CREATE TABLE mi_alter_modify_src (k UInt64, embedding Array(Float32), extra String)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_alter_modify_idx
ON mi_alter_modify_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

ALTER TABLE mi_alter_modify_src MODIFY COLUMN embedding Array(Float64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE mi_alter_modify_idx SYNC;
ALTER TABLE mi_alter_modify_src MODIFY COLUMN embedding Array(Float64);

SELECT type
FROM system.columns
WHERE database = currentDatabase() AND table = 'mi_alter_modify_src' AND name = 'embedding';

DROP TABLE mi_alter_modify_src;
