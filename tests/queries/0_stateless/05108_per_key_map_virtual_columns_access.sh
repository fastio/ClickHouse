#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail
user="${CLICKHOUSE_DATABASE}_map_virtual_user"
cleanup()
{
    ${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY IF EXISTS map_virtual_policy ON map_virtual_access; DROP TABLE IF EXISTS map_virtual_access; DROP USER IF EXISTS ${user}" --multiquery
}
trap cleanup EXIT
cleanup
${CLICKHOUSE_CLIENT} --multiquery --query "
CREATE TABLE map_virtual_access (id UInt64, public_map Map(String, UInt64), secret_map Map(String, UInt64)) ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
INSERT INTO map_virtual_access VALUES (1, {'a': 1}, {'secret': 1});
CREATE USER ${user};
GRANT SELECT(id, public_map) ON ${CLICKHOUSE_DATABASE}.map_virtual_access TO ${user};"
${CLICKHOUSE_CLIENT} --user "${user}" --multiquery --query "
SELECT _map_column_keys FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT _part_map_files FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT count(_map_column_keys) FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT count(_part_map_files) FROM map_virtual_access; -- { serverError ACCESS_DENIED }
"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.map_virtual_access TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --query "SELECT _map_column_keys FROM map_virtual_access"
${CLICKHOUSE_CLIENT} --query "CREATE ROW POLICY map_virtual_policy ON map_virtual_access FOR SELECT USING id > 100 TO ${user}"
${CLICKHOUSE_CLIENT} --user "${user}" --multiquery --query "
SELECT _map_column_keys FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT _part_map_files FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT count(_map_column_keys) FROM map_virtual_access; -- { serverError ACCESS_DENIED }
SELECT count(_part_map_files) FROM map_virtual_access; -- { serverError ACCESS_DENIED }
"
