-- Tags: no-fasttest
-- Focused contract coverage for the AUXILIARY INDEX DDL introduced by the
-- ANN / ReplicatedANN refactor. The test avoids waiting for background builds:
-- it validates syntax, metadata, engine argument shape, and immutable build
-- setting rejection at catalog/DDL level.

SET allow_experimental_auxiliary_index = 1;
SET allow_auxiliary_index_engine_mismatch = 1;

DROP TABLE IF EXISTS ai_ddl_idx SYNC;
DROP TABLE IF EXISTS ai_ddl_src;

CREATE TABLE ai_ddl_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE AUXILIARY INDEX ai_ddl_idx
ON ai_ddl_src (v)
ENGINE = ANN(diskann)
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

SELECT create_table_query LIKE '%CREATE AUXILIARY INDEX%' AS has_create_auxiliary_index,
       create_table_query LIKE '%ENGINE = ANN(diskann)%' AS has_ann_engine,
       create_table_query LIKE '%ann_dimension = 4%' AS has_dimension_setting
FROM system.tables
WHERE database = currentDatabase() AND name = 'ai_ddl_idx';

SHOW CREATE TABLE ai_ddl_idx FORMAT Null;

ALTER AUXILIARY INDEX ai_ddl_idx MODIFY SETTING ann_dimension = 8; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }
ALTER AUXILIARY INDEX ai_ddl_idx MODIFY SETTING diskann_pruned_degree = 64; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }
ALTER AUXILIARY INDEX ai_ddl_idx MODIFY SETTING spann_posting_page_limit = 16; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }

DROP TABLE ai_ddl_idx SYNC;

CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) TYPE ann('diskann', metric = 'L2', dim = 4) ENGINE = ANN(diskann); -- { clientError SYNTAX_ERROR }
CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANN('diskann') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }
CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANN(unknown_ann) SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) ENGINE = ANN(diskann, extra) SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) ENGINE = ReplicatedANN(diskann, '/clickhouse/tables/{database}/ai_ddl_idx') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError BAD_ARGUMENTS }
CREATE AUXILIARY INDEX ai_ddl_idx ON ai_ddl_src (v) ENGINE = ReplicatedANN('diskann', '/clickhouse/tables/{database}/ai_ddl_idx', 'r1') SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

DROP TABLE ai_ddl_src;
