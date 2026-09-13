#!/usr/bin/env bash
# Tags: no-object-storage, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE map_paths_drop (id UInt64, m Map(String, Array(UInt64)), keep Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO map_paths_drop VALUES (1, {'a': [1,2], 'b': []}, {'a': 5});
"
part_dir=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'map_paths_drop' AND active")
find "$part_dir" -maxdepth 1 -name 'm.key_*' | grep -q .
${CLICKHOUSE_CLIENT} --multiquery --query "
ALTER TABLE map_paths_drop DROP COLUMN m SETTINGS mutations_sync = 2;
SELECT id, keep FROM map_paths_drop;
CHECK TABLE map_paths_drop SETTINGS check_query_single_value_result = 1;
"
part_dir=$(${CLICKHOUSE_CLIENT} --query "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 'map_paths_drop' AND active")
if find "$part_dir" -maxdepth 1 -name 'm.*' | grep -q .; then
    echo 'Unexpected dropped column files'
    exit 1
fi
echo 'Dropped column files removed'
${CLICKHOUSE_CLIENT} --query "DROP TABLE map_paths_drop"
