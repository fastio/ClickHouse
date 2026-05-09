-- Tags: no-replicated-database, no-shared-merge-tree, no-parallel
-- End-to-end: with an existing index, a second source-side INSERT must not
-- destabilize the catalog and must not crash the server during the Remap
-- cycle. Mock algorithm is no-op (Pack 6 deviation).

DROP TABLE IF EXISTS mi_remap_src;
DROP TABLE IF EXISTS mi_remap_idx SYNC;

CREATE TABLE mi_remap_src (k UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

INSERT INTO mi_remap_src VALUES (1, [1.0, 2.0]);

CREATE MATERIALIZED INDEX mi_remap_idx
ON mi_remap_src (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex;

-- Let the initial Build cycle settle. max_block_size = 1 keeps each
-- sleepEachRow chunk under the per-block sleep cap.
SELECT sleepEachRow(1) FROM numbers(15) SETTINGS max_block_size = 1 FORMAT Null;

INSERT INTO mi_remap_src VALUES (2, [3.0, 4.0]);

-- Give the Remap cycle time to fire.
SELECT sleepEachRow(1) FROM numbers(15) SETTINGS max_block_size = 1 FORMAT Null;

SELECT 'remap done';

-- The catalog row survives both cycles and is reachable for queries.
SELECT count() FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_remap_idx';

DROP TABLE mi_remap_idx SYNC;
DROP TABLE mi_remap_src;

