-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_alter_setting_idx SYNC;
DROP TABLE IF EXISTS mi_alter_setting_src;

CREATE TABLE mi_alter_setting_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_alter_setting_idx
ON mi_alter_setting_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

ALTER TABLE mi_alter_setting_idx MODIFY SETTING ann_metric = 'cosine'; -- { serverError SUPPORT_IS_DISABLED }
ALTER TABLE mi_alter_setting_idx RESET SETTING ann_dimension; -- { serverError SUPPORT_IS_DISABLED }

ALTER TABLE mi_alter_setting_idx MODIFY SETTING ann_index_task_max_input_rows = 1000000;

SELECT 'alter setting checks completed';

DROP TABLE mi_alter_setting_idx SYNC;
DROP TABLE mi_alter_setting_src;
