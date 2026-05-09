-- Tags: no-replicated-database, no-shared-merge-tree, no-parallel
-- End-to-end: CREATE MATERIALIZED INDEX must drive a Build cycle without
-- crashing the server. Mock algorithm is no-op (see Pack 6 deviation note
-- in chain-doc), so this test only asserts the framework path is wired
-- up: the index appears in `system.materialized_indexes` and survives a
-- background tick.

DROP TABLE IF EXISTS mi_cycle_src;
DROP TABLE IF EXISTS mi_cycle SYNC;

CREATE TABLE mi_cycle_src (k UInt64, v Array(Float32))
ENGINE = MergeTree ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

INSERT INTO mi_cycle_src VALUES (1, [1.0, 2.0]);

CREATE MATERIALIZED INDEX mi_cycle
ON mi_cycle_src (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex;

-- Wait for the background_schedule_pool to fire one cycle. Default tick
-- interval is roughly 10 seconds. max_block_size = 1 keeps each
-- sleepEachRow chunk under the per-block sleep cap.
SELECT sleepEachRow(1) FROM numbers(15) SETTINGS max_block_size = 1 FORMAT Null;

SELECT 'cycle started';

-- The catalog row exists.
SELECT count() FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_cycle';

-- DROP must complete cleanly even with an in-flight cycle.
DROP TABLE mi_cycle SYNC;
DROP TABLE mi_cycle_src;

