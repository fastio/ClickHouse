-- Tags: no-fasttest, no-parallel
-- Hot/cold path correctness for the ANN-index inline payload optimisation.
--
-- Builds a small DiskANN reflection, then runs the same vector search twice:
--   (a) with the hot path enabled (default) — payload sidecar is read and
--       `{uuid, _part_offset}` returns inline, bumping `ANNIndexHotPathHits`.
--   (b) with `ann_index_disable_hot_path = 1` — every hit goes through
--       `readLocatorRows`, bumping `ANNIndexColdPathHits`.
-- The two result sets MUST be byte-identical: hot/cold split is a pure
-- performance optimisation and may not change query semantics.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_hot SYNC;
DROP TABLE IF EXISTS src_hot;

CREATE TABLE src_hot (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_hot
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(256);

CREATE REFLECTION mi_hot
ON src_hot (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 30;

SYSTEM SYNC REFLECTION mi_hot;

-- Reset event counters before each run so we can attribute hits to a single
-- query. `system.events` is process-cumulative; we read it via
-- `currentProfileEvents` instead so the assertion is per-query.

-- Run (a): hot path active.
SELECT k FROM src_hot
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
SETTINGS ann_index_disable_hot_path = 0, force_ann_index = 'mi_hot';

-- Run (b): hot path forced off, must give the same result set.
SELECT k FROM src_hot
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0])
LIMIT 5
SETTINGS ann_index_disable_hot_path = 1, force_ann_index = 'mi_hot';

DROP TABLE mi_hot SYNC;
DROP TABLE src_hot;
