-- Tags: no-fasttest, zookeeper
-- Verifies the source/index engine reconcile check in
-- `validateANNIndexPrerequisites`:
--
--   * Replicated source + ENGINE = ANNIndex(diskann)            -> INCORRECT_QUERY
--   * Non-replicated source + ENGINE = ReplicatedANNIndex -> INCORRECT_QUERY
--   * `allow_ann_index_engine_mismatch = 1` escapes both checks
--     (recovery only).
--
-- See: src/Interpreters/validateANNIndexPrerequisites.cpp:114-132.

SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS src_rep_emm SYNC;
DROP TABLE IF EXISTS src_plain_emm SYNC;
DROP TABLE IF EXISTS mi_emm SYNC;

-- A1: Replicated source + plain `ANNIndex` engine is rejected.
CREATE TABLE src_rep_emm (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_rep_emm_04196', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_emm
ON src_rep_emm (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- A2: plain source + `ReplicatedANNIndex` engine is rejected.
CREATE TABLE src_plain_emm (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_emm
ON src_plain_emm (embedding)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{database}/mi_emm_04196_a2', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 4; -- { serverError INCORRECT_QUERY }

-- A3: escape hatch lets a forbidden combination through. The recovery-only
-- override must be explicit; we exercise the replicated-source / plain-engine
-- direction so the surviving index is single-node and easy to clean up.
SET allow_ann_index_engine_mismatch = 1;

CREATE REFLECTION mi_emm
ON src_rep_emm (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4;

SELECT engine
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_emm';

DROP TABLE mi_emm SYNC;
DROP TABLE src_rep_emm SYNC;
DROP TABLE src_plain_emm SYNC;
