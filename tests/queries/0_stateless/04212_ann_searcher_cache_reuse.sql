-- Tags: no-fasttest, no-parallel, no-cpu-aarch64, use-sptag
-- The reflection ANN searcher cache is keyed by `(part path, build_params_hash)` and
-- intentionally excludes search-time tunables. This test verifies that changing a
-- search-time setting between two queries does NOT cause the cache to re-open the
-- searcher — i.e. one searcher per part is reused regardless of session settings.
-- no-fasttest: SPTAG build is slow; no-cpu-aarch64: SPTAG is x86_64-only on CI.
-- no-parallel because the test reads `system.query_log` ProfileEvents for the
-- current database.

SET allow_experimental_ann_index = 1;
SET enable_ann_index = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS mi_spann_cache SYNC;
DROP TABLE IF EXISTS src_spann_cache;

CREATE TABLE src_spann_cache
(
    k UInt64,
    embedding Array(Float32)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_spann_cache
SELECT
    number,
    arrayMap(d -> if(d = 0, toFloat32(number * 1000), toFloat32(cityHash64(number, d) % 100) / 100.0), range(16))
FROM numbers(512);

CREATE REFLECTION mi_spann_cache
ON src_spann_cache (embedding)
ENGINE = ANNIndex(spann)
SETTINGS ann_metric = 'L2', ann_dimension = 16,
         ann_index_sync_timeout = 60,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1;

SYSTEM SYNC REFLECTION mi_spann_cache;

CREATE TEMPORARY TABLE mi_spann_cache_start AS SELECT now64(6) AS ts;

SET force_ann_index = 'mi_spann_cache';

-- Q1 (cache miss): first search, opens the SPANN searcher and caches it.
WITH (SELECT embedding FROM src_spann_cache WHERE k = 100) AS q
SELECT k FROM src_spann_cache
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_cache_q1_miss';

-- Q2 (cache hit): same part, same build params — must reuse the searcher.
WITH (SELECT embedding FROM src_spann_cache WHERE k = 200) AS q
SELECT k FROM src_spann_cache
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_cache_q2_hit';

-- Q3 (cache hit despite changed search-time setting): different
-- `spann_search_internal_result_num` — this used to live in the cache key
-- before the refactor; now it must NOT cause a miss.
WITH (SELECT embedding FROM src_spann_cache WHERE k = 300) AS q
SELECT k FROM src_spann_cache
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_cache_q3_diff_search_setting',
         spann_search_internal_result_num = 128;

-- Q4 (still hit, another setting changed).
WITH (SELECT embedding FROM src_spann_cache WHERE k = 400) AS q
SELECT k FROM src_spann_cache
ORDER BY L2Distance(embedding, q)
LIMIT 1
SETTINGS log_comment = 'mi_spann_cache_q4_diff_search_setting',
         spann_search_max_check = 8192;

SYSTEM FLUSH LOGS query_log;

-- Expected: q1 has at least one miss, q2/q3/q4 have only hits and zero misses.
-- Pre-refactor, q3/q4 would have shown up as additional misses because the
-- search-time tunables were part of the cache key.
SELECT
    log_comment,
    sum(ProfileEvents['AnnSearcherCacheMisses']) > 0 AS had_miss,
    sum(ProfileEvents['AnnSearcherCacheHits']) > 0 AS had_hit
FROM system.query_log
WHERE event_date >= yesterday()
    AND event_time_microseconds >= (SELECT ts FROM mi_spann_cache_start)
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_kind = 'Select'
    AND log_comment LIKE 'mi_spann_cache_%'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE mi_spann_cache SYNC;
DROP TABLE src_spann_cache;
