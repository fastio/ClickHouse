#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
SET allow_experimental_ann_index = 1;

DROP TABLE IF EXISTS mi_drop_part_shell SYNC;
DROP TABLE IF EXISTS src_drop_part_shell SYNC;

CREATE TABLE src_drop_part_shell
(
    k UInt64,
    embedding Array(Float32)
)
ENGINE = MergeTree
ORDER BY k
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO src_drop_part_shell
SELECT number, [number * 1.0, 0, 0, 0]
FROM numbers(16);

CREATE REFLECTION mi_drop_part_shell
ON src_drop_part_shell (embedding)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 4,
         ann_index_sync_timeout = 60,
         ann_index_build_min_rows = 1,
         ann_index_build_min_parts = 1;

SYSTEM SYNC REFLECTION mi_drop_part_shell;

SELECT
    countIf(rows > 0) = 1 AS has_one_part_before_drop,
    sum(rows) = 16 AS rows_before_drop
FROM system.ann_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_part_shell' AND active;

SYSTEM STOP REFLECTION BUILDS mi_drop_part_shell;
"

INNER_TABLE=$(${CLICKHOUSE_CLIENT} --query "
SELECT concat('.inner_id.', toString(uuid))
FROM system.tables
WHERE database = currentDatabase() AND name = 'mi_drop_part_shell'
")

PART_NAME=$(${CLICKHOUSE_CLIENT} --query "
SELECT name
FROM system.parts
WHERE database = currentDatabase() AND table = '${INNER_TABLE}' AND active
ORDER BY name
LIMIT 1
")

if [[ -z "${PART_NAME}" ]]; then
    echo "No active materialized-index part found in ${INNER_TABLE}" >&2
    exit 1
fi

${CLICKHOUSE_CLIENT} --query "ALTER TABLE mi_drop_part_shell DROP PART '${PART_NAME}'"

${CLICKHOUSE_CLIENT} --query "
SELECT
    countIf(rows > 0) = 0 AS no_parts_after_drop,
    sum(rows) = 0 AS no_rows_after_drop
FROM system.ann_index_parts
WHERE database = currentDatabase() AND index_name = 'mi_drop_part_shell' AND active;

DROP TABLE mi_drop_part_shell SYNC;
DROP TABLE src_drop_part_shell SYNC;
"
