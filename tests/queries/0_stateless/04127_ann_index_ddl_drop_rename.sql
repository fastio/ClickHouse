-- Tags: no-fasttest
-- Exercises DROP / DETACH / ATTACH / RENAME on a REFLECTION.
-- The index-typed guards must reject non-MI tables; SYNC / ASYNC and
-- DETACH / ATTACH must preserve the catalog entry across the round-trip.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;
DROP TABLE IF EXISTS mi_idx2 SYNC;
DROP TABLE IF EXISTS mi_plain_table;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

-- Guard: DROP REFLECTION on a non-reflection table must fail.
CREATE TABLE mi_plain_table (x UInt64) ENGINE = Memory;
DROP REFLECTION mi_plain_table; -- { serverError INCORRECT_QUERY }
DROP TABLE mi_plain_table;

-- DROP REFLECTION ... SYNC.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT count() FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP REFLECTION mi_idx SYNC;

SELECT count() FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

-- DROP REFLECTION ... (async default).
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

DROP REFLECTION mi_idx;

-- RENAME TABLE keeps the reflection visible under the new name.
CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

RENAME TABLE mi_idx TO mi_idx2;

SELECT name FROM system.ann_indexes
WHERE database = currentDatabase() AND name IN ('mi_idx', 'mi_idx2');

-- DETACH / ATTACH round-trip must preserve the reflection.
DETACH TABLE mi_idx2;

SELECT count() FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx2';

ATTACH TABLE mi_idx2;

SELECT name, family, impl FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_idx2';

DROP TABLE mi_idx2 SYNC;
DROP TABLE mi_src_ok;
