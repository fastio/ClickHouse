-- Tags: no-fasttest, zookeeper, no-cpu-aarch64, use-cppdiskann
-- End-to-end correctness for `ReplicatedANN`: the build path
-- routes through `commitReplacingPartFromBackgroundTask` with
-- materialized-index leader-lease and task-lock check ops. This test verifies
-- the full pipeline on a single node: write source → SYSTEM SYNC drains the build
-- queue → optimizer rewrites a self-query → cppdiskann returns the indexed
-- row as top-1 with distance 0.
--
-- Companion to `04187_ann_index_replicated_inner_storage.sql`,
-- which only asserts the DDL shape.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_repl_e2e_cppdiskann';

DROP TABLE IF EXISTS mi_repl_e2e_cppdiskann SYNC;
DROP TABLE IF EXISTS src_repl_e2e_cppdiskann SYNC;

CREATE TABLE src_repl_e2e_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/src_repl_e2e_04188_cppdiskann', 'r1')
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_repl_e2e_cppdiskann
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_repl_e2e_cppdiskann
ON src_repl_e2e_cppdiskann (embedding)
ENGINE = ReplicatedANNIndex(cppdiskann, '/clickhouse/tables/{database}/mi_repl_e2e_04188_cppdiskann', 'r1')
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_repl_e2e_cppdiskann;

-- The build committed at least one part through the replicated commit path.
SELECT
    ann_index_part_count > 0 AS has_part,
    total_rows = 4096 AS rows_match
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'mi_repl_e2e_cppdiskann';

-- The committed part lives on the replicated inner storage and is visible
-- through the replication queue (replicated parts always appear in
-- `system.replicated_fetches`/`system.replicas`; here we just check that
-- the inner table is replicated, mirroring 04187).
SELECT startsWith(engine, 'Replicated') AS inner_is_replicated
FROM system.tables
WHERE database = currentDatabase()
  AND name = (SELECT concat('.inner_id.', toString(uuid))
              FROM system.tables
              WHERE database = currentDatabase() AND name = 'mi_repl_e2e_cppdiskann');

-- Self-query: cppdiskann must return the indexed row as top-1 with distance 0.
-- If the build path silently produced an empty/broken inner part, this query
-- would return either zero rows or a non-zero distance.
WITH (SELECT embedding FROM src_repl_e2e_cppdiskann WHERE k = 1000) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_repl_e2e_cppdiskann
ORDER BY L2Distance(embedding, q)
LIMIT 1;

DROP TABLE mi_repl_e2e_cppdiskann SYNC;
DROP TABLE src_repl_e2e_cppdiskann SYNC;
