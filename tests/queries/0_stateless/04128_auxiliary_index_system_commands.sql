-- Tags: no-fasttest
-- Exercises SYSTEM subcommands that target a REFLECTION. None of the
-- pipelines are wired up yet; the commands are expected to parse,
-- type-check, and return without throwing.

SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION mi_idx
ON mi_src_ok (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 1;

-- Every SYSTEM MI subcommand should parse and dispatch without throwing.
-- The empty source table is already fully covered, so `SYSTEM SYNC` returns
-- immediately; build coverage is asserted by the dedicated 04133-04136 tests.
SYSTEM REFRESH REFLECTION mi_idx;
SYSTEM STOP REFLECTION BUILDS mi_idx;
SYSTEM START REFLECTION BUILDS mi_idx;
SYSTEM STOP REFLECTION REMAPS mi_idx;
SYSTEM START REFLECTION REMAPS mi_idx;
SYSTEM SYNC REFLECTION mi_idx;

SELECT 'system commands completed';

-- The placeholder columns must surface as NULL until the engine reports a
-- real lifecycle state, coverage ratio, or creation timestamp.
SELECT state, coverage_ratio, creation_time
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

-- The real-valued counters stay populated even while placeholders are NULL.
-- `source_database` is omitted because the per-run test database name is not
-- reproducible in the reference file; the four counters and the index name
-- are stable inputs that pin the real-valued columns.
SELECT name, auxiliary_index_part_count, total_rows, total_bytes_on_disk, consecutive_remap_count
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

DROP TABLE mi_idx SYNC;
DROP TABLE mi_src_ok;
