-- Tags: no-fasttest, no-parallel
-- Verifies the experimental gate around CREATE MATERIALIZED INDEX:
-- a fresh CREATE without the setting must be rejected, the setting flips
-- the gate open, and DETACH / ATTACH on an already-created index must keep
-- working with the gate closed (otherwise existing `.sql` metadata would
-- fail to load on restart).

DROP TABLE IF EXISTS mi_gate_src;
DROP TABLE IF EXISTS mi_gate SYNC;

CREATE TABLE mi_gate_src (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

-- Default-off: CREATE must be rejected with SUPPORT_IS_DISABLED.
CREATE MATERIALIZED INDEX mi_gate
ON mi_gate_src (v)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex; -- { serverError SUPPORT_IS_DISABLED }

-- Setting opens the gate.
SET allow_experimental_materialized_index = 1;

CREATE MATERIALIZED INDEX mi_gate
ON mi_gate_src (v)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex;

SELECT count() FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_gate';

-- ATTACH must succeed even with the gate closed; otherwise existing
-- metadata would fail to load on server restart.
SET allow_experimental_materialized_index = 0;

DETACH TABLE mi_gate SYNC;
ATTACH TABLE mi_gate;

SELECT count() FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_gate';

DROP TABLE mi_gate SYNC;
DROP TABLE mi_gate_src;
