-- Test: text_index_lazy_vs_materialize_correctness
-- Verifies that lazy and materialize modes produce identical row sets (not just counts).
-- Uses groupBitXor(id), cityHash64 checksums, and exact row-sequence comparison.

SET allow_experimental_text_index_lazy_apply = 1;
SET query_plan_direct_read_from_text_index = 1;

-- ============================================================================
-- Test 1: Row-set fingerprint comparison (groupBitXor)
-- groupBitXor(id) detects cases where count() matches but the actual rows differ.
-- ============================================================================
DROP TABLE IF EXISTS t_lazy_fingerprint;

CREATE TABLE t_lazy_fingerprint
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_lazy_fingerprint
SELECT
    number,
    concat(
        if(number % 3 = 0, 'alpha ', ''),
        if(number % 5 = 0, 'beta ', ''),
        if(number % 7 = 0, 'gamma ', ''),
        if(number % 11 = 0, 'delta ', ''),
        'end'
    )
FROM numbers(20000);

OPTIMIZE TABLE t_lazy_fingerprint FINAL;

SELECT 'T1 hasToken';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasToken(text, 'alpha') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasToken(text, 'alpha') SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T1 hasAllTokens';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T1 hasAnyTokens';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAnyTokens(text, ['alpha', 'beta', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T1 mixed AND+OR';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta']) AND hasAnyTokens(text, ['gamma', 'delta']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta']) AND hasAnyTokens(text, ['gamma', 'delta']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T1 3-token AND';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_fingerprint WHERE hasAllTokens(text, ['alpha', 'beta', 'gamma']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

DROP TABLE t_lazy_fingerprint;


-- ============================================================================
-- Test 2: LIMIT + ORDER BY
-- Verifies that row ordering and early termination produce identical results.
-- ============================================================================
DROP TABLE IF EXISTS t_lazy_limit;

CREATE TABLE t_lazy_limit
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_lazy_limit
SELECT
    number,
    concat(
        if(number % 3 = 0, 'alpha ', ''),
        if(number % 7 = 0, 'beta ', ''),
        if(number % 11 = 0, 'gamma ', ''),
        'end'
    )
FROM numbers(10000);

OPTIMIZE TABLE t_lazy_limit FINAL;

SELECT 'T2 LIMIT 10 hasToken';
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasToken(text, 'alpha') ORDER BY id LIMIT 10) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasToken(text, 'alpha') ORDER BY id LIMIT 10) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T2 LIMIT 100 hasAllTokens';
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasAllTokens(text, ['alpha', 'beta']) ORDER BY id LIMIT 100) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasAllTokens(text, ['alpha', 'beta']) ORDER BY id LIMIT 100) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T2 LIMIT 50 OFFSET 20 hasAnyTokens';
SELECT
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasAnyTokens(text, ['alpha', 'beta']) ORDER BY id LIMIT 50 OFFSET 20) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupArray(id) FROM (SELECT id FROM t_lazy_limit WHERE hasAnyTokens(text, ['alpha', 'beta']) ORDER BY id LIMIT 50 OFFSET 20) SETTINGS text_index_posting_list_apply_mode = 'lazy');

DROP TABLE t_lazy_limit;


-- ============================================================================
-- Test 3: Non-aggregate SELECT (actual row content via checksum)
-- Computes a hash over the full (id, text) result set to verify byte-level equality.
-- ============================================================================
DROP TABLE IF EXISTS t_lazy_rows;

CREATE TABLE t_lazy_rows
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_lazy_rows
SELECT
    number,
    concat(
        if(number % 4 = 0, 'foo ', ''),
        if(number % 6 = 0, 'bar ', ''),
        if(number % 10 = 0, 'baz ', ''),
        'base'
    )
FROM numbers(8000);

OPTIMIZE TABLE t_lazy_rows FINAL;

SELECT 'T3 row content hasToken';
SELECT
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasToken(text, 'foo') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasToken(text, 'foo') SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T3 row content hasAllTokens';
SELECT
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasAllTokens(text, ['foo', 'bar']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasAllTokens(text, ['foo', 'bar']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

SELECT 'T3 row content hasAnyTokens';
SELECT
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasAnyTokens(text, ['foo', 'baz']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT sum(cityHash64(id, text)) FROM t_lazy_rows WHERE hasAnyTokens(text, ['foo', 'baz']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

DROP TABLE t_lazy_rows;


-- ============================================================================
-- Test 4: Sparse granule edge cases
-- Large granules (8192) where most granules have zero matches for a rare token.
-- Also covers RawPostings path (cardinality 7-12, stored as VarUInt in .pst).
-- ============================================================================
DROP TABLE IF EXISTS t_lazy_sparse;

CREATE TABLE t_lazy_sparse
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- 100,000 rows with 8192-row granules -> ~12 granules.
-- 'needle' appears only every 10,000 rows -> 10 hits total, most granules empty.
INSERT INTO t_lazy_sparse
SELECT
    number,
    concat(
        if(number % 3 = 0, 'common ', ''),
        if(number % 10000 = 0, 'needle ', ''),
        'filler'
    )
FROM numbers(100000);

OPTIMIZE TABLE t_lazy_sparse FINAL;

SELECT 'T4 sparse single token';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasToken(text, 'needle') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasToken(text, 'needle') SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- Extreme selectivity difference: common (33%) AND needle (0.01%)
SELECT 'T4 sparse AND extreme';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasAllTokens(text, ['common', 'needle']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasAllTokens(text, ['common', 'needle']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- OR with very sparse token
SELECT 'T4 sparse OR';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasAnyTokens(text, ['needle', 'common']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_sparse WHERE hasAnyTokens(text, ['needle', 'common']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- Zero results: token not present at all
SELECT 'T4 no match';
SELECT
    (SELECT count() FROM t_lazy_sparse WHERE hasToken(text, 'nonexistent') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT count() FROM t_lazy_sparse WHERE hasToken(text, 'nonexistent') SETTINGS text_index_posting_list_apply_mode = 'lazy');

DROP TABLE t_lazy_sparse;


-- ============================================================================
-- Test 5: RawPostings boundary (cardinality near MAX_CARDINALITY_FOR_RAW_POSTINGS=12)
-- Verifies correct handling at the boundary between RawPostings and compressed.
-- ============================================================================
DROP TABLE IF EXISTS t_lazy_rawpostings;

CREATE TABLE t_lazy_rawpostings
(
    id UInt64,
    text String,
    INDEX idx_text text TYPE text(tokenizer = 'splitByNonAlpha', posting_list_codec = 'bitpacking') GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 128;

INSERT INTO t_lazy_rawpostings
SELECT
    number,
    concat(
        if(number % 500 = 0, 'sparse500 ', ''),   -- 20 rows (compressed)
        if(number % 900 = 0, 'sparse900 ', ''),   -- 12 rows (RawPostings boundary)
        if(number % 1000 = 0, 'sparse1000 ', ''),  -- 10 rows (RawPostings)
        if(number % 2000 = 0, 'sparse2000 ', ''),  -- 5 rows  (embedded)
        'filler'
    )
FROM numbers(10000);

OPTIMIZE TABLE t_lazy_rawpostings FINAL;

-- Compressed (cardinality > 12)
SELECT 'T5 compressed hasToken';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse500') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse500') SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- RawPostings boundary (cardinality = 12)
SELECT 'T5 raw boundary hasToken';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse900') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse900') SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- RawPostings (cardinality = 10)
SELECT 'T5 raw hasToken';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse1000') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse1000') SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- Embedded (cardinality = 5)
SELECT 'T5 embedded hasToken';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse2000') SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasToken(text, 'sparse2000') SETTINGS text_index_posting_list_apply_mode = 'lazy');

-- AND across all encoding types: compressed × raw × embedded
SELECT 'T5 cross-encoding AND';
SELECT
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasAllTokens(text, ['sparse500', 'sparse1000', 'sparse2000']) SETTINGS text_index_posting_list_apply_mode = 'materialize')
    =
    (SELECT groupBitXor(id) FROM t_lazy_rawpostings WHERE hasAllTokens(text, ['sparse500', 'sparse1000', 'sparse2000']) SETTINGS text_index_posting_list_apply_mode = 'lazy');

DROP TABLE t_lazy_rawpostings;
