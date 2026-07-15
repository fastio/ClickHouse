#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

REPO_DIR=$(cd "$CUR_DIR/../../.." && pwd)
DATASET_DIR="$REPO_DIR/tmp/${CLICKHOUSE_TEST_UNIQUE_NAME}_lance"
TABLE_NAME="${CLICKHOUSE_TEST_UNIQUE_NAME}_lance"

cleanup()
{
    ${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --query "DROP TABLE IF EXISTS ${TABLE_NAME}" >/dev/null 2>&1 || true
    rm -rf "${DATASET_DIR:?}"
}
trap cleanup EXIT
cleanup

if ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${TABLE_NAME} (id UInt64) ENGINE=Lance('${DATASET_DIR}')" >/dev/null 2>&1
then
    echo "experimental gate: failed"
else
    echo "experimental gate: ok"
fi

${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --multiquery <<EOF
CREATE TABLE ${TABLE_NAME}
(
    id UInt64,
    s String,
    n Nullable(Int32),
    a Array(UInt8)
)
ENGINE=Lance('${DATASET_DIR}');

INSERT INTO ${TABLE_NAME} VALUES (1, 'one', 10, [1, 2]), (2, 'two', NULL, []);
INSERT INTO ${TABLE_NAME} VALUES (3, 'three', 30, [3]);

SELECT id, s, n, a FROM ${TABLE_NAME} ORDER BY id;
SELECT s FROM lance('${DATASET_DIR}') ORDER BY id;
SELECT id FROM lance('${DATASET_DIR}') ORDER BY id SETTINGS lance_version=1;
EOF

if ${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 --lance_version=1 \
    --query "INSERT INTO ${TABLE_NAME} VALUES (4, 'four', 40, [4])" >/dev/null 2>&1
then
    echo "historical write guard: failed"
else
    echo "historical write guard: ok"
fi

if ${CLICKHOUSE_CLIENT} --allow_experimental_lance=1 \
    --query "SELECT * FROM lance('${DATASET_DIR}_missing')" >/dev/null 2>&1
then
    echo "missing dataset error: failed"
else
    echo "missing dataset error: ok"
fi
