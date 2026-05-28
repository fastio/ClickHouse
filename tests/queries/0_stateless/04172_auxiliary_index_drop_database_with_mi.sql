SET allow_experimental_auxiliary_index = 1;

DROP DATABASE IF EXISTS mi_drop_database_with_mi;
CREATE DATABASE mi_drop_database_with_mi;

CREATE TABLE mi_drop_database_with_mi.src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_drop_database_with_mi.idx
ON mi_drop_database_with_mi.src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

DROP DATABASE mi_drop_database_with_mi;

SELECT count()
FROM system.databases
WHERE name = 'mi_drop_database_with_mi';
