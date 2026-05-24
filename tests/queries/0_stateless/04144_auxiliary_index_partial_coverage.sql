-- Tags: no-fasttest, no-parallel
-- Result smoke test for a multi-part source table with a AuxiliaryIndex
-- definition. This test intentionally accepts either a partial rewrite or a
-- fallback scan; event-based tests pin the actual partial-coverage fast path.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_pc SYNC;
DROP TABLE IF EXISTS src_pc;

CREATE TABLE src_pc (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_pc
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(10);

INSERT INTO src_pc
SELECT 100 + number, [(100 + number) * 1.0, 0, 0, 0]
FROM numbers(10);

CREATE AUXILIARY INDEX mi_pc
ON src_pc (embedding)
ENGINE = ANN(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

SELECT k FROM src_pc
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_pc SYNC;
DROP TABLE src_pc;
