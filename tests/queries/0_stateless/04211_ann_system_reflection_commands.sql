-- Tags: no-fasttest

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_system_cmd_idx SYNC;
DROP TABLE IF EXISTS mi_system_cmd_src;

CREATE TABLE mi_system_cmd_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_system_cmd_idx
ON mi_system_cmd_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4, ann_index_sync_timeout = 1;

SYSTEM STOP REFLECTION BUILDS mi_system_cmd_idx;
SYSTEM REFRESH REFLECTION mi_system_cmd_idx;
SYSTEM START REFLECTION BUILDS mi_system_cmd_idx;
SYSTEM STOP REFLECTION REMAPS mi_system_cmd_idx;
SYSTEM START REFLECTION REMAPS mi_system_cmd_idx;
SYSTEM SYNC REFLECTION mi_system_cmd_idx;

SELECT 'system reflection commands completed';

DROP TABLE mi_system_cmd_idx SYNC;
DROP TABLE mi_system_cmd_src;
