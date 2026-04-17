-- Tests for `diskann` skip index DDL (Phase 1: validator-only, no creator).
-- The index type is registered as a validator-only DDL entry point; accepting
-- the DDL does not change the query / write / merge pipelines, so queries keep
-- using the brute-force path.

DROP TABLE IF EXISTS t_diskann_ddl;

-- ---------------------------------------------------------------------------
-- Positive cases
-- ---------------------------------------------------------------------------

-- Case 1: all arguments provided
SELECT '-- 1: all arguments';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=768, R=64, L=100, alpha=1.2, pq_bytes=32) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DROP TABLE t_diskann_ddl;

-- Case 2: only required arguments (defaults applied)
SELECT '-- 2: required only';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='cosine', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DROP TABLE t_diskann_ddl;

-- Case 3: metric='mips' accepted at DDL layer (build-time rejection belongs to task-8)
SELECT '-- 3: metric=mips accepted';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='mips', dim=256) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DROP TABLE t_diskann_ddl;

-- Case 4: Array(Float64) column
SELECT '-- 4: Array(Float64)';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float64),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=64) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DROP TABLE t_diskann_ddl;

-- Case 5: Array(BFloat16) column
SELECT '-- 5: Array(BFloat16)';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(BFloat16),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=64) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
DROP TABLE t_diskann_ddl;

-- ---------------------------------------------------------------------------
-- Negative cases (parameter validation, BAD_ARGUMENTS)
-- ---------------------------------------------------------------------------

SELECT '-- 6: missing metric';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 7: missing dim';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2') GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 8: invalid metric';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='xyz', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 9: dim=0';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=0) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 10: R out of range (too small)';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128, R=8) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 11: L < R';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128, R=64, L=10) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 12: alpha out of range';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128, alpha=0.5) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 13: pq_bytes not in allowed set';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128, pq_bytes=7) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 14: unknown key';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128, foo=1) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

-- ---------------------------------------------------------------------------
-- Negative cases (column shape)
-- ---------------------------------------------------------------------------

SELECT '-- 15: non-array column';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec UInt64,
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError ILLEGAL_COLUMN }

SELECT '-- 16: Array(String) column';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(String),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1; -- { serverError ILLEGAL_COLUMN }

-- ---------------------------------------------------------------------------
-- Negative cases (table-level cross-cutting checks)
-- ---------------------------------------------------------------------------

SELECT '-- 17: enable_block_number_column disabled';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 0, enable_block_offset_column = 1; -- { serverError BAD_ARGUMENTS }

SELECT '-- 18: enable_block_offset_column disabled';
CREATE TABLE t_diskann_ddl
(
    id  UInt64,
    vec Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 0; -- { serverError BAD_ARGUMENTS }

SELECT '-- 19: ALTER ADD a second diskann index is rejected';
CREATE TABLE t_diskann_ddl
(
    id   UInt64,
    vec  Array(Float32),
    vec2 Array(Float32),
    INDEX idx_vec vec TYPE diskann(metric='l2', dim=128) GRANULARITY 1
) ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;
ALTER TABLE t_diskann_ddl ADD INDEX idx_vec2 vec2 TYPE diskann(metric='l2', dim=128) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_diskann_ddl;
