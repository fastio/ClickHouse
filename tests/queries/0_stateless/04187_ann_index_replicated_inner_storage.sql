-- Tags: no-fasttest, zookeeper
-- Single-replica coverage for `ReplicatedANN`: creation must
-- keep the public index engine replicated and create a replicated inner
-- `MergeTree` storage for materialized-index parts.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_repl_inner_idx SYNC;
DROP TABLE IF EXISTS mi_repl_inner_src SYNC;

CREATE TABLE mi_repl_inner_src (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/mi_repl_inner_src_04187', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_repl_inner_idx
ON mi_repl_inner_src (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_repl_inner_idx_04187', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT engine
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_inner_idx';

-- The inner table name is `.inner_id.<outer_uuid>`. We resolve it inline
-- with a scalar subquery rather than via `CREATE TEMPORARY TABLE AS SELECT
-- FROM system.tables`, which observes a stale snapshot in this code path.
SELECT engine, startsWith(engine_full, 'ReplicatedMergeTree') AS replicated_inner
FROM system.tables
WHERE database = currentDatabase()
  AND name = (SELECT concat('.inner_id.', toString(uuid))
              FROM system.tables
              WHERE database = currentDatabase() AND name = 'mi_repl_inner_idx');

DROP TABLE mi_repl_inner_idx SYNC;

-- After dropping the outer index its inner storage must be cleaned up too.
SELECT count()
FROM system.tables
WHERE database = currentDatabase() AND startsWith(name, '.inner_id.');

DROP TABLE mi_repl_inner_src SYNC;
