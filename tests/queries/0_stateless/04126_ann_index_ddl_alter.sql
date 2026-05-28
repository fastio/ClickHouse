-- Tags: no-fasttest
-- Confirms ALTER REFLECTION is not part of the public DDL surface.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4
COMMENT 'initial';

SELECT family, impl, comment FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

ALTER REFLECTION mi_idx MODIFY TYPE ann('diskann', metric = 'L2', dim = 4); -- { clientError SYNTAX_ERROR }
ALTER REFLECTION mi_idx MODIFY SETTING index_granularity = 2048; -- { clientError SYNTAX_ERROR }
ALTER REFLECTION mi_idx RESET SETTING index_granularity; -- { clientError SYNTAX_ERROR }
ALTER REFLECTION mi_idx MODIFY COMMENT 'updated'; -- { clientError SYNTAX_ERROR }

-- Confirm the index is still catalog-visible after the rejected alters.
SELECT family, impl, comment FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP TABLE mi_idx SYNC;
DROP TABLE mi_src_ok;
