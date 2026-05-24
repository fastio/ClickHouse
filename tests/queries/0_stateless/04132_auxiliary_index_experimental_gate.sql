-- Tags: no-fasttest, no-parallel
-- Verifies the experimental gate around `CREATE AUXILIARY INDEX`:
-- a fresh `CREATE` without the setting must be rejected, the setting flips
-- the gate open, and `DETACH` / `ATTACH` on an already-created index must be
-- rejected again with the gate closed.

DROP TABLE IF EXISTS mi_gate_src;
DROP TABLE IF EXISTS mi_gate SYNC;

CREATE TABLE mi_gate_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- Default-off: `CREATE` must be rejected with `SUPPORT_IS_DISABLED`.
CREATE AUXILIARY INDEX mi_gate
ON mi_gate_src (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError SUPPORT_IS_DISABLED }

-- Setting opens the gate.
SET allow_experimental_auxiliary_index = 1;

CREATE AUXILIARY INDEX mi_gate
ON mi_gate_src (v)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT count() FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_gate';

-- `ATTACH` must be rejected with the gate closed; otherwise existing metadata
-- would load on server restart after the administrator disables the feature.
SET allow_experimental_auxiliary_index = 0;

DETACH TABLE mi_gate SYNC;
ATTACH TABLE mi_gate; -- { serverError SUPPORT_IS_DISABLED }

SET allow_experimental_auxiliary_index = 1;
ATTACH TABLE mi_gate;

SELECT count() FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_gate';

DROP TABLE mi_gate SYNC;
DROP TABLE mi_gate_src;
