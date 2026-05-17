-- Tags: no-fasttest, zookeeper
-- Verifies the source/index engine reconcile check in
-- `validateMaterializedIndexPrerequisites`:
--
--   * Replicated source + ENGINE = MaterializedIndex            -> INCORRECT_QUERY
--   * Non-replicated source + ENGINE = ReplicatedMaterializedIndex -> INCORRECT_QUERY
--   * `allow_materialized_index_engine_mismatch = 1` escapes both checks
--     (recovery only).
--
-- See: src/Interpreters/validateMaterializedIndexPrerequisites.cpp:114-132.

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS src_rep_emm SYNC;
DROP TABLE IF EXISTS src_plain_emm SYNC;
DROP TABLE IF EXISTS mi_emm SYNC;

-- A1: Replicated source + plain `MaterializedIndex` engine is rejected.
CREATE TABLE src_rep_emm (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_rep_emm_04196', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_emm
ON src_rep_emm (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex; -- { serverError INCORRECT_QUERY }

-- A2: plain source + `ReplicatedMaterializedIndex` engine is rejected.
CREATE TABLE src_plain_emm (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_emm
ON src_plain_emm (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = ReplicatedMaterializedIndex('/clickhouse/tables/{database}/mi_emm_04196_a2', 'r1'); -- { serverError INCORRECT_QUERY }

-- A3: escape hatch lets a forbidden combination through. The recovery-only
-- override must be explicit; we exercise the replicated-source / plain-engine
-- direction so the surviving index is single-node and easy to clean up.
SET allow_materialized_index_engine_mismatch = 1;

CREATE MATERIALIZED INDEX mi_emm
ON src_rep_emm (embedding)
TYPE ann('diskann', metric = 'L2', dim = 4)
ENGINE = MaterializedIndex;

SELECT engine
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_emm';

DROP TABLE mi_emm SYNC;
DROP TABLE src_rep_emm SYNC;
DROP TABLE src_plain_emm SYNC;
