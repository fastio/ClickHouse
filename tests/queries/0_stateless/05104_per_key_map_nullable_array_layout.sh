#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail
CLICKHOUSE_CLIENT+=" --enable_parallel_replicas 0"

${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE nullable_layout (id UInt64, m Map(String, Array(Nullable(Int32))))
ENGINE = MergeTree ORDER BY id SETTINGS serialization_info_version = 'with_types', map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0,
    min_bytes_for_wide_part = 0, replace_long_file_name_to_hash = 0;
INSERT INTO nullable_layout VALUES (1, {}), (2, {'k':[]}), (3, {'k':[NULL]}), (4, {'k':[1,NULL,-2,3]});
"
${CLICKHOUSE_CLIENT} --query "SELECT arraySort(substreams) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'nullable_layout' AND column = 'm' AND active"
${CLICKHOUSE_CLIENT} --query "SELECT m FROM nullable_layout ORDER BY id FORMAT Native" > "${CLICKHOUSE_TMP}/nullable_layout_map.native"
${CLICKHOUSE_CLIENT} --query "CREATE TABLE nullable_native (m Map(String, Array(Nullable(Int32)))) ENGINE = Memory"
${CLICKHOUSE_CLIENT} --query "INSERT INTO nullable_native FORMAT Native" < "${CLICKHOUSE_TMP}/nullable_layout_map.native"
${CLICKHOUSE_CLIENT} --query "SELECT m FROM nullable_native ORDER BY toString(m)"
${CLICKHOUSE_CLIENT} --query "SELECT m['k'] AS v FROM nullable_layout ORDER BY id FORMAT Native SETTINGS optimize_functions_to_subcolumns = 1" \
    | ${CLICKHOUSE_LOCAL} --input-format Native --query "SELECT v, toTypeName(v) FROM table"
${CLICKHOUSE_CLIENT} --multiquery --query "
TRUNCATE TABLE nullable_layout;
INSERT INTO nullable_layout VALUES (1, {'k':[1,NULL], 'k.null':[2,NULL], 'k.null1':[3,NULL], 'a.b':[4,NULL], '%':[5,NULL], '中文':[6,NULL]});
SELECT m['k'], m['k.null'], m['k.null1'], m['a.b'], m['%'], m['中文'] FROM nullable_layout SETTINGS optimize_functions_to_subcolumns = 1;
CHECK TABLE nullable_layout SETTINGS check_query_single_value_result = 1;
DROP TABLE nullable_native;
DROP TABLE nullable_layout;
"
