#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-fasttest
#
# Inspect the Wide part directory of a `with_key_columns` Map: the manifest and written
# key files exist, unwritten keys and the write-time template do not. Deleting a
# listed key file must fail the query instead of returning NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_per_key_layout"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_per_key_layout
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    replace_long_file_name_to_hash = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_per_key_layout"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_per_key_layout VALUES (1, {'a': 1, 'b': 2})"

echo "substreams"
${CLICKHOUSE_CLIENT} -q "
SELECT arraySort(arrayFilter(x -> x LIKE 'm.keys_info' OR x LIKE 'm.key_%' OR x LIKE '%per_key_template%', substreams))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_per_key_layout' AND column = 'm' AND active
"

part_dir=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_per_key_layout' AND active
")

has_pattern()
{
    local pattern=$1
    if find "$part_dir" -maxdepth 1 -name "$pattern" | grep -q .
    then
        echo 1
    else
        echo 0
    fi
}

echo "files"
echo "keys_info=$(has_pattern 'm.keys_info.*')"
echo "key_a=$(has_pattern 'm.key_a*')"
echo "key_b=$(has_pattern 'm.key_b*')"
echo "key_c=$(has_pattern 'm.key_c*')"
echo "template=$(has_pattern '*per_key_template*')"

echo "missing key is null"
${CLICKHOUSE_CLIENT} -q "SELECT m['c'] FROM t_per_key_layout"

echo "deleted listed key file"
rm -f "$part_dir"/m.key_a.bin

echo "query"
# A listed key whose data file is gone must not become NULL.
# shellcheck disable=SC2016
if ${CLICKHOUSE_CLIENT} --query "SELECT m['a'] FROM t_per_key_layout" > /dev/null 2> "${CLICKHOUSE_TMP}/per_key_query.err"
then
    echo UNEXPECTED_SUCCESS
else
    grep -oE 'NO_FILE_IN_DATA_PART|CANNOT_OPEN_FILE|INCORRECT_DATA|CORRUPTED_DATA|NOT_FOUND_EXPECTED_DATA_PART' "${CLICKHOUSE_TMP}/per_key_query.err" | head -n1
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_per_key_layout"
