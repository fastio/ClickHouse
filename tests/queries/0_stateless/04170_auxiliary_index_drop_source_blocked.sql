SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_drop_source_idx SYNC;
DROP TABLE IF EXISTS mi_drop_source_src;

CREATE TABLE mi_drop_source_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_drop_source_idx
ON mi_drop_source_src (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SET check_referential_table_dependencies = 1;
DROP TABLE mi_drop_source_src; -- { serverError HAVE_DEPENDENT_OBJECTS }

SELECT count()
FROM system.tables
WHERE database = currentDatabase() AND name = 'mi_drop_source_src';

DROP TABLE mi_drop_source_idx SYNC;
DROP TABLE mi_drop_source_src;

SELECT 'drop_after_index_ok';
