-- Exercises SYSTEM subcommands that target a MATERIALIZED INDEX as well
-- as the BACKUP ... WITH MATERIALIZED INDEXES parse surface. None of the
-- pipelines are wired up yet; the commands are expected to parse, type-
-- check, and return without throwing.

SET allow_experimental_materialized_index = 1;

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = 1;

-- Every SYSTEM MI subcommand should parse and dispatch without throwing;
-- the build / remap pipelines only log the recorded intent at this stage.
-- SYSTEM SYNC, however, performs a real bounded wait and will raise
-- TIMEOUT_EXCEEDED here because the source has no rows for the
-- reconciler to schedule against — the success/coverage assertion stays
-- in the dedicated 04133-04136 tests; 04128 just verifies the command
-- is recognised.
SYSTEM REFRESH MATERIALIZED INDEX mi_idx;
SYSTEM STOP MATERIALIZED INDEX BUILDS mi_idx;
SYSTEM START MATERIALIZED INDEX BUILDS mi_idx;
SYSTEM STOP MATERIALIZED INDEX REMAPS mi_idx;
SYSTEM START MATERIALIZED INDEX REMAPS mi_idx;
SYSTEM SYNC MATERIALIZED INDEX mi_idx; -- { serverError TIMEOUT_EXCEEDED }

SELECT 'system commands completed';

-- The placeholder columns must surface as NULL until the engine reports a
-- real lifecycle state, coverage ratio, or creation timestamp.
SELECT state, coverage_ratio, creation_time
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

-- The real-valued counters stay populated even while placeholders are NULL.
-- `source_database` is omitted because the per-run test database name is not
-- reproducible in the reference file; the four counters and the index name
-- are stable inputs that pin the real-valued columns.
SELECT name, mi_part_count, total_rows, total_bytes_on_disk, consecutive_remap_count
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'mi_idx';

-- BACKUP ... WITH MATERIALIZED INDEXES: verify the parser accepts the
-- keyword without executing the backup. formatQuerySingleLine round-trips
-- the parsed AST to SQL, which is enough to confirm the clause stuck.
SELECT formatQuerySingleLine('BACKUP TABLE mi_src_ok TO Disk(''default'', ''unused.zip'') WITH MATERIALIZED INDEXES');

DROP TABLE mi_idx SYNC;
DROP TABLE mi_src_ok;
