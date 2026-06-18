-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-cppdiskann
-- no-parallel because this test asserts a profile event via query_log.
-- End-to-end correctness smoke for the cppdiskann-backed ANNIndex
-- query path: build → SYSTEM SYNC → optimizer rewrite → cppdiskann search →
-- reverse lookup to source row. Uses dim=32 deterministic-hash vectors
-- (non-degenerate, so cppdiskann actually returns the exact neighbour) and
-- self-queries with a stored embedding, which must come back top-1 with
-- distance 0.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET force_ann_index = 'mi_smoke_cppdiskann';
SET log_queries = 1;

DROP TABLE IF EXISTS mi_smoke_cppdiskann SYNC;
DROP TABLE IF EXISTS src_smoke_cppdiskann;

CREATE TABLE src_smoke_cppdiskann (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_smoke_cppdiskann
SELECT number, arrayMap(d -> toFloat32(cityHash64(number, d) % 1000000) / 1000000.0, range(32))
FROM numbers(4096);

CREATE REFLECTION mi_smoke_cppdiskann
ON src_smoke_cppdiskann (embedding)
ENGINE = ANNIndex(cppdiskann)
SETTINGS ann_metric = 'L2', ann_dimension = 32,
         diskann_pruned_degree = 8,
         diskann_max_degree = 8,
         diskann_l_build = 16,
         diskann_num_threads = 2,
         diskann_build_quantization = 'FP',
         diskann_build_ram_limit_gb = 1,
         ann_index_sync_timeout = 60;

SYSTEM SYNC REFLECTION mi_smoke_cppdiskann;

CREATE TEMPORARY TABLE mi_smoke_start_cppdiskann AS SELECT now64(6) AS ts;

-- Self-query: pull k=1000's stored embedding and look it up. cppdiskann on
-- non-degenerate data must return it as the top-1 with distance ~ 0.
WITH (SELECT embedding FROM src_smoke_cppdiskann WHERE k = 1000) AS q
SELECT k, round(L2Distance(embedding, q), 6) AS d
FROM src_smoke_cppdiskann
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_smoke_self_query_cppdiskann';

WITH (SELECT embedding FROM src_smoke_cppdiskann WHERE k = 1000) AS q
SELECT k
FROM src_smoke_cppdiskann
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS diskann_search_num_threads = 65; -- { serverError BAD_ARGUMENTS }

SYSTEM FLUSH LOGS query_log;

-- Assert the fast path actually fired (profile event must be set).
SELECT
    log_comment,
    max(ProfileEvents['ANNIndexCppDiskANNSearchStarted'] > 0) AS used_cppdiskann_search
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_smoke_start_cppdiskann)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment = 'mi_smoke_self_query_cppdiskann'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_smoke_cppdiskann SYNC;
DROP TABLE src_smoke_cppdiskann;
