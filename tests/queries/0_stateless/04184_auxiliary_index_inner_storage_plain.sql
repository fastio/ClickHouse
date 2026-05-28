SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_inner_storage_idx SYNC;
DROP TABLE IF EXISTS mi_inner_storage_src;

CREATE TABLE mi_inner_storage_src (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_inner_storage_idx
ON mi_inner_storage_src (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

-- The inner storage uses the `.inner_id.<uuid>` (or `.inner.<name>`) convention
-- shared with MaterializedView, WindowView and TimeSeries. It is registered in
-- the catalog and visible in system.tables so users can introspect it.
SELECT count() = 1 AS inner_present
FROM system.tables
WHERE database = currentDatabase() AND startsWith(name, '.inner');

DROP TABLE mi_inner_storage_idx SYNC;

SELECT count() = 0 AS inner_dropped
FROM system.tables
WHERE database = currentDatabase() AND startsWith(name, '.inner');

DROP TABLE mi_inner_storage_src;
