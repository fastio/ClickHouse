#!/usr/bin/env bash
# Tags: no-random-settings, no-random-merge-tree-settings, no-object-storage, no-shared-merge-tree, no-replicated-database, no-fasttest
#
# Inspect the Compact part directory of a `with_key_columns` Map. The per-key data lives
# as substreams inside data.bin (compact marks), the key manifest is a standalone
# `keys_info` sidecar file, and the write-time template stream must not exist. Deleting
# the sidecar must fail the query instead of returning NULL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_pk_compact_layout"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE t_pk_compact_layout
(
    id UInt64,
    m Map(String, UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    map_serialization_version = 'with_key_columns',
    map_serialization_version_for_zero_level_parts = 'with_key_columns',
    min_bytes_for_wide_part = '10G',
    min_rows_for_wide_part = 1000000000,
    index_granularity = 2,
    replace_long_file_name_to_hash = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES t_pk_compact_layout"
# Key 'b' first appears in the second granule and 'c' only in the third: the whole part
# freezes {a, b, c} before granule 0.
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_pk_compact_layout VALUES (1, map('a', 1)), (2, map('a', 2, 'b', 10)), (3, map('c', 3)), (4, map())"

echo "part type"
${CLICKHOUSE_CLIENT} -q "
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_pk_compact_layout' AND active
"

echo "substreams"
${CLICKHOUSE_CLIENT} -q "
SELECT arraySort(arrayFilter(x -> x LIKE 'm.keys_info' OR x LIKE 'm.key_%' OR x LIKE '%per_key_template%', substreams))
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_pk_compact_layout' AND column = 'm' AND active
"

part_dir=$(${CLICKHOUSE_CLIENT} -q "
    SELECT path FROM system.parts
    WHERE database = currentDatabase() AND table = 't_pk_compact_layout' AND active
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
# Compact keeps all key data in data.bin with compact marks; no per-key .bin files.
echo "data_bin=$(has_pattern 'data.bin')"
echo "compact_marks=$(has_pattern 'data.cmrk4')"
echo "keys_info=$(has_pattern 'm.keys_info.*')"
echo "key_a_bin=$(has_pattern 'm.key_a.bin')"
echo "key_b_bin=$(has_pattern 'm.key_b.bin')"
echo "template=$(has_pattern '*per_key_template*')"

echo "columns_substreams for m"
grep -E '^\s+m\.' "$part_dir/columns_substreams.txt"

echo "missing key is null"
${CLICKHOUSE_CLIENT} -q "SELECT m['missing'] FROM t_pk_compact_layout ORDER BY id"

echo "deleted sidecar manifest"
rm -f "$part_dir"/m.keys_info.bin
${CLICKHOUSE_CLIENT} -q "SYSTEM DROP MARK CACHE"
# A listed key whose manifest is gone must fail the query, not silently return NULL.
# shellcheck disable=SC2016
if ${CLICKHOUSE_CLIENT} --query "SELECT m['a'] FROM t_pk_compact_layout" > /dev/null 2> "${CLICKHOUSE_TMP}/per_key_compact_query.err"
then
    echo UNEXPECTED_SUCCESS
else
    grep -oE 'FILE_DOESNT_EXIST|NO_FILE_IN_DATA_PART|CANNOT_OPEN_FILE|INCORRECT_DATA|CORRUPTED_DATA|NOT_FOUND_EXPECTED_DATA_PART' "${CLICKHOUSE_TMP}/per_key_compact_query.err" | head -n1
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_pk_compact_layout"
