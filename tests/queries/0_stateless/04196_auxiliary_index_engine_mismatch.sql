-- Tags: no-fasttest, zookeeper
-- Verifies the source/index engine reconcile check in
-- `validateAuxiliaryIndexPrerequisites`:
--
--   * Replicated source + ENGINE = ANN(diskann)            -> INCORRECT_QUERY
--   * Non-replicated source + ENGINE = ReplicatedANN -> INCORRECT_QUERY
--   * `allow_auxiliary_index_engine_mismatch = 1` escapes both checks
--     (recovery only).
--
-- See: src/Interpreters/validateAuxiliaryIndexPrerequisites.cpp:114-132.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS src_rep_emm SYNC;
DROP TABLE IF EXISTS src_plain_emm SYNC;
DROP TABLE IF EXISTS mi_emm SYNC;

-- A1: Replicated source + plain `ANN` engine is rejected.
CREATE TABLE src_rep_emm (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_rep_emm_04196', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_emm
ON src_rep_emm (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- A2: plain source + `ReplicatedANN` engine is rejected.
CREATE TABLE src_plain_emm (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE AUXILIARY INDEX mi_emm
ON src_plain_emm (embedding)
ENGINE = ReplicatedANN(diskann, '/clickhouse/tables/{database}/mi_emm_04196_a2', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- A3: escape hatch lets a forbidden combination through. The recovery-only
-- override must be explicit; we exercise the replicated-source / plain-engine
-- direction so the surviving index is single-node and easy to clean up.
SET allow_auxiliary_index_engine_mismatch = 1;

CREATE AUXILIARY INDEX mi_emm
ON src_rep_emm (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT engine
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_emm';

DROP TABLE mi_emm SYNC;
DROP TABLE src_rep_emm SYNC;
DROP TABLE src_plain_emm SYNC;
