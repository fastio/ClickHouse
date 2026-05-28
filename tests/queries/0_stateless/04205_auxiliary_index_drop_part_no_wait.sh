#!/usr/bin/env bash
# Tags: no-fasttest
# Covers AuxiliaryIndex part removal paths:
#   * `dropPart` / `ALTER TABLE ... DROP PART` — user-facing; runs the inner
#     partition command and refreshes coverage (see `04191_auxiliary_index_drop_part.sh`).
#   * `dropPartNoWaitNoThrow` — `MergeTreeData::clearEmptyParts` hook; the catalog
#     shell delegates `clearEmptyParts` to the inner MergeTree and forwards no-wait
#     drops for active inner part names (`StorageANN.cpp`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
SET allow_experimental_auxiliary_index = 1;

DROP TABLE IF EXISTS mi_drop_no_wait SYNC;
DROP TABLE IF EXISTS src_drop_no_wait;

CREATE TABLE src_drop_no_wait (k UInt64, embedding Array(Float32))
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_drop_no_wait
SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0]
FROM numbers(16);

CREATE REFLECTION mi_drop_no_wait
ON src_drop_no_wait (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         auxiliary_index_sync_timeout = 60,
         auxiliary_index_build_min_rows = 1,
         auxiliary_index_build_min_parts = 1,
         remove_empty_parts = 1,
         merge_tree_clear_old_parts_interval_seconds = 1;

SYSTEM SYNC REFLECTION mi_drop_no_wait;

SELECT
    countIf(rows > 0) = 1 AS has_one_part_before_drop,
    sum(rows) = 16 AS rows_before_drop
FROM system.auxiliary_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_no_wait' AND active;
"

PART_NAME=$(${CLICKHOUSE_CLIENT} --query "
SELECT part_name
FROM system.auxiliary_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_no_wait' AND active
ORDER BY part_name
LIMIT 1
")

if [[ -z "${PART_NAME}" ]]; then
    echo "No active materialized-index part found for mi_drop_no_wait" >&2
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "ALTER TABLE mi_drop_no_wait DROP PART '${PART_NAME}'"

${CLICKHOUSE_CLIENT} --query "
SELECT
    countIf(rows > 0) = 0 AS no_active_parts_after_drop_part,
    sum(rows) = 0 AS no_rows_after_drop_part
FROM system.auxiliary_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_no_wait' AND active;

DROP TABLE mi_drop_no_wait SYNC;
DROP TABLE src_drop_no_wait;
"
