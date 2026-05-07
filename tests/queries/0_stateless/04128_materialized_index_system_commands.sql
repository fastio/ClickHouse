-- Exercises SYSTEM subcommands that target a MATERIALIZED INDEX as well
-- as the BACKUP ... WITH MATERIALIZED INDEXES parse surface. None of the
-- pipelines are wired up yet; the commands are expected to parse, type-
-- check, and return without throwing.

DROP TABLE IF EXISTS mi_src_ok;
DROP TABLE IF EXISTS mi_idx SYNC;

CREATE TABLE mi_src_ok (k UInt64, v Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE MATERIALIZED INDEX mi_idx
ON mi_src_ok (v)
TYPE ann('MockAnn')
ENGINE = MaterializedIndex;

-- Every SYSTEM MI subcommand should parse and dispatch without throwing;
-- the background pipelines only log the recorded intent at this stage.
SYSTEM REFRESH MATERIALIZED INDEX mi_idx;
SYSTEM STOP MATERIALIZED INDEX BUILDS mi_idx;
SYSTEM START MATERIALIZED INDEX BUILDS mi_idx;
SYSTEM STOP MATERIALIZED INDEX REMAPS mi_idx;
SYSTEM START MATERIALIZED INDEX REMAPS mi_idx;
SYSTEM SYNC MATERIALIZED INDEX mi_idx;

SELECT 'system commands completed';

-- BACKUP ... WITH MATERIALIZED INDEXES: verify the parser accepts the
-- keyword without executing the backup. formatQuerySingleLine round-trips
-- the parsed AST to SQL, which is enough to confirm the clause stuck.
SELECT formatQuerySingleLine('BACKUP TABLE mi_src_ok TO Disk(''default'', ''unused.zip'') WITH MATERIALIZED INDEXES');

DROP TABLE mi_idx SYNC;
DROP TABLE mi_src_ok;
