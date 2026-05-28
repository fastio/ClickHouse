-- Tags: no-fasttest, no-parallel
-- TopK result smoke test for a source table that has a AuxiliaryIndex
-- definition. This test intentionally accepts either the indexed path or the
-- fallback scan; event-based tests pin the actual DiskANN fast path.

SET allow_experimental_auxiliary_index = 1;
SET enable_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_full SYNC;
DROP TABLE IF EXISTS src_full;

CREATE TABLE src_full (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_full
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(256);

CREATE REFLECTION mi_full
ON src_full (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

-- The result must match the brute-force ranking regardless of whether the
-- background MI build has committed yet: if it has, the optimizer attaches
-- hints and the reader pulls rows by `_part_offset`; if not, the fallback
-- scan produces the same answer.
SELECT k FROM src_full
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5;

DROP TABLE mi_full SYNC;
DROP TABLE src_full;
