-- Tags: no-fasttest
-- Exercises ALTER AUXILIARY INDEX MODIFY SETTING /
-- RESET SETTING / MODIFY COMMENT. At this stage the commands parse and
-- dispatch cleanly but the storage does not yet execute them, so they
-- surface as LOGICAL_ERROR or NOT_IMPLEMENTED — either is acceptable.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE AUXILIARY INDEX mi_idx
ON mi_src_ok (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4
COMMENT 'initial';

SELECT family, impl, comment FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

ALTER AUXILIARY INDEX mi_idx MODIFY TYPE ann('diskann', metric = 'L2', dim = 4); -- { clientError SYNTAX_ERROR }
ALTER AUXILIARY INDEX mi_idx MODIFY SETTING index_granularity = 2048; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }
ALTER AUXILIARY INDEX mi_idx RESET SETTING index_granularity; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }
ALTER AUXILIARY INDEX mi_idx MODIFY COMMENT 'updated'; -- { serverError LOGICAL_ERROR, NOT_IMPLEMENTED }

-- Confirm the index is still catalog-visible after the rejected alters.
SELECT family, impl, comment FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP TABLE mi_idx SYNC;
DROP TABLE mi_src_ok;
