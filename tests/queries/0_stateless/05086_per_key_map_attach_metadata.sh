#!/usr/bin/env bash
# Tags: no-replicated-database, no-shared-merge-tree

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -euo pipefail

${CLICKHOUSE_CLIENT} --query "
CREATE TABLE map_paths_attach_metadata (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns'"
metadata_path=$(${CLICKHOUSE_CLIENT} --query "SELECT metadata_path FROM system.tables WHERE database = currentDatabase() AND name = 'map_paths_attach_metadata'")
server_path=$(${CLICKHOUSE_EXTRACT_CONFIG} --key path)
metadata_path="${server_path%/}/${metadata_path}"
backup="${CLICKHOUSE_TMP}/map_paths_attach_metadata.sql"
cp "$metadata_path" "$backup"
restore_metadata()
{
    cp "$backup" "$metadata_path"
}
trap restore_metadata EXIT

for invalid in sorting projection; do
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE map_paths_attach_metadata"
    python3 - "$metadata_path" "$backup" "$invalid" <<'PY'
import sys
from pathlib import Path
metadata, backup, invalid = sys.argv[1:]
text = Path(backup).read_text()
if invalid == 'sorting':
    before, after = 'ORDER BY id', 'ORDER BY length(m)'
else:
    before, after = '\n)\nENGINE', ',\n    PROJECTION p (SELECT id ORDER BY id)\n)\nENGINE'
assert text.count(before) == 1, text
Path(metadata).write_text(text.replace(before, after))
PY
    if ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE map_paths_attach_metadata" 2> "${CLICKHOUSE_TMP}/map_paths_attach.err"; then
        echo 'Unexpected successful attach'
        exit 1
    fi
    grep -q 'SUPPORT_IS_DISABLED' "${CLICKHOUSE_TMP}/map_paths_attach.err"
    echo "$invalid rejected"
    restore_metadata
    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE map_paths_attach_metadata"
done
trap - EXIT
${CLICKHOUSE_CLIENT} --query "DROP TABLE map_paths_attach_metadata"
