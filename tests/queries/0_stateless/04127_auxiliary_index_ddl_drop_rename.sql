-- Tags: no-fasttest
-- Exercises DROP / DETACH / ATTACH / RENAME on a AUXILIARY INDEX.
-- The index-typed guards must reject non-MI tables; SYNC / ASYNC and
-- DETACH / ATTACH must preserve the catalog entry across the round-trip.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;
DROP TABLE IF EXISTS mi_idx2 SYNC;
DROP TABLE IF EXISTS mi_plain_table;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, assign_part_uuids = 1;

-- Guard: DROP AUXILIARY INDEX on a non-MI table must fail.
CREATE TABLE mi_plain_table (x UInt64) ENGINE = Memory;
DROP AUXILIARY INDEX mi_plain_table; -- { serverError INCORRECT_QUERY }
DROP TABLE mi_plain_table;

-- DROP AUXILIARY INDEX ... SYNC.
CREATE AUXILIARY INDEX mi_idx
ON mi_src_ok (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT count() FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP AUXILIARY INDEX mi_idx SYNC;

SELECT count() FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

-- DROP AUXILIARY INDEX ... (async default).
CREATE AUXILIARY INDEX mi_idx
ON mi_src_ok (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

DROP AUXILIARY INDEX mi_idx;

-- RENAME TABLE keeps the index visible under the new name.
CREATE AUXILIARY INDEX mi_idx
ON mi_src_ok (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

RENAME TABLE mi_idx TO mi_idx2;

SELECT name FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name IN ('mi_idx', 'mi_idx2');

-- DETACH / ATTACH round-trip must preserve the index.
DETACH TABLE mi_idx2;

SELECT count() FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx2';

ATTACH TABLE mi_idx2;

SELECT name, family, impl FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx2';

DROP TABLE mi_idx2 SYNC;
DROP TABLE mi_src_ok;
