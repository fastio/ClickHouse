-- Tags: no-fasttest
-- Focused contract coverage for the REFLECTION DDL introduced by the
-- ANNIndex / ReplicatedANNIndex refactor. The test avoids waiting for background builds:
-- it validates syntax, metadata, engine argument shape, and immutable build
-- setting rejection at catalog/DDL level.

SET allow_experimental_auxiliary_index = 1;
SET allow_auxiliary_index_engine_mismatch = 1;

DROP REFLECTION IF EXISTS ai_ddl_idx SYNC;
DROP TABLE IF EXISTS ai_ddl_src;

CREATE TABLE ai_ddl_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE REFLECTION ai_ddl_idx
ON ai_ddl_src (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2',
         ann_dimension = 4,
         diskann_pruned_degree = 32,
         diskann_max_degree = 64;

SELECT family, impl, engine
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'ai_ddl_idx';

SELECT engine
FROM system.tables
WHERE database = currentDatabase() AND name = 'ai_ddl_idx';

SELECT create_table_query LIKE '%CREATE REFLECTION%' AS has_create_reflection,
       create_table_query LIKE '%ENGINE = ANNIndex(diskann)%' AS has_ann_engine,
       create_table_query LIKE '%ann_dimension = 4%' AS has_dimension_setting
FROM system.tables
WHERE database = currentDatabase() AND name = 'ai_ddl_idx';

SHOW CREATE TABLE ai_ddl_idx FORMAT Null;
DESCRIBE REFLECTION ai_ddl_idx FORMAT Null;

CREATE AUXILIARY INDEX ai_ddl_old ON ai_ddl_src (v) ENGINE = ANN(diskann); -- { clientError SYNTAX_ERROR }
DROP AUXILIARY INDEX ai_ddl_idx; -- { clientError SYNTAX_ERROR }
ALTER AUXILIARY INDEX ai_ddl_idx MODIFY SETTING ann_dimension = 8; -- { clientError SYNTAX_ERROR }
BACKUP TABLE ai_ddl_src TO Disk('backups', 'ai_ddl_src.zip') WITH AUXILIARY INDEXES; -- { clientError SYNTAX_ERROR }

DROP REFLECTION ai_ddl_idx SYNC;

CREATE REFLECTION ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANNIndex('diskann') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }
CREATE REFLECTION ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANNIndex(unknown_ann) SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE REFLECTION ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANNIndex(diskann, extra) SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE REFLECTION ai_ddl_idx ON ai_ddl_src (v) ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/ai_ddl_idx') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE REFLECTION ai_ddl_idx ON ai_ddl_src (v) ENGINE = ReplicatedANNIndex('diskann', '/clickhouse/tables/{database}/ai_ddl_idx', 'r1') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

DROP TABLE ai_ddl_src;
