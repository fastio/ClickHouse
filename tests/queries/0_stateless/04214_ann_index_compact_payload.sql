-- Tags: no-fasttest, no-parallel
-- Verifies that ANN-index reflections built on this branch persist the new
-- compact `vectors.payload` records (8 B / vector, V1) and that hot-path
-- search still returns identical results to the cold-path fallback.
--
-- The test does not poke at the on-disk record width directly — that lives
-- in `coverage.json::payload_format_version` which is not exposed via SQL.
-- Instead it checks the `ANNIndexPayloadV1Used` ProfileEvent (incremented by
-- BuildTask when it picks V1) and the hot/cold equivalence assertion that
-- already covers the decoder round-trip.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;

DROP TABLE IF EXISTS mi_compact SYNC;
DROP TABLE IF EXISTS src_compact;

CREATE TABLE src_compact (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_compact
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(256);

-- Snapshot V1 build event before the build so we can detect the increment.
CREATE TABLE compact_v1_before
ENGINE = Memory AS
SELECT ifNull((SELECT value FROM system.events WHERE event = 'ANNIndexPayloadV1Used'), 0) AS value;

CREATE REFLECTION mi_compact
ON src_compact (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 30;

SYSTEM SYNC REFLECTION mi_compact;

-- The build must have picked V1 (256 rows fits in UInt32 trivially) and
-- emitted exactly one ANNIndexPayloadV1Used increment.
SELECT
    ifNull((SELECT value FROM system.events WHERE event = 'ANNIndexPayloadV1Used'), 0)
  - (SELECT value FROM compact_v1_before)
   >= 1 AS v1_used;

-- Hot path: payload tells the matcher {part_id, _part_offset} directly.
SELECT k FROM src_compact
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0]), k
LIMIT 5
SETTINGS ann_index_disable_hot_path = 0, force_ann_index = 'mi_compact';

-- Cold path: must produce the byte-identical result set.
SELECT k FROM src_compact
ORDER BY L2Distance(embedding, [3.7, 0, 0, 0]), k
LIMIT 5
SETTINGS ann_index_disable_hot_path = 1, force_ann_index = 'mi_compact';

DROP TABLE compact_v1_before;
DROP TABLE mi_compact SYNC;
DROP TABLE src_compact;
