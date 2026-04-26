#!/usr/bin/env bash
# Drive a SIFT-1M Recall@10 / QPS sweep against a running ClickHouse server.
#
# Inputs:
#   CLICKHOUSE_BINARY    path to the `clickhouse` binary (defaults to PATH)
#   CLICKHOUSE_PORT_TCP  TCP port (default 9000)
#   CLICKHOUSE_DB        target database (default sift)
#   SEARCH_LIST_SIZES    space-separated list of search_list_size values to sweep
#                        (default: "10 30 50 100 200")
#   QUERIES_FOR_QPS      number of queries used for the QPS measurement
#                        (default: 1000; use 10000 for the published baseline)
#   K                    top-k for recall (default: 10)
#
# Outputs (under ./results/<utc_timestamp>/):
#   sweep.tsv            one row per `search_list_size` with recall, qps, latency
#                        percentiles, ProfileEvents medians, index size, build
#                        seconds and the git commit being benchmarked
#   server_meta.txt      server version, build, ANN ProfileEvents at start
#
# This script is intentionally not wired into CI: it downloads ~167 MB, builds
# an index that takes minutes, and reports user-visible numbers. Run it
# manually after sizeable changes to the ANN code path.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$DIR/data"
RESULT_ROOT="$DIR/results"
RUN_DIR="$RESULT_ROOT/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR"

CLICKHOUSE_BINARY="${CLICKHOUSE_BINARY:-clickhouse}"
PORT="${CLICKHOUSE_PORT_TCP:-9000}"
DB="${CLICKHOUSE_DB:-sift}"
K="${K:-10}"
SEARCH_LIST_SIZES="${SEARCH_LIST_SIZES:-10 30 50 100 200}"
QUERIES_FOR_QPS="${QUERIES_FOR_QPS:-1000}"

# Build-time hyperparameters held constant across the sweep so we isolate the
# Recall-vs-QPS Pareto curve to one knob (`search_list_size`).
MAX_DEGREE="${MAX_DEGREE:-64}"
BUILD_SEARCH_LIST_SIZE="${BUILD_SEARCH_LIST_SIZE:-100}"
ALPHA="${ALPHA:-1.2}"
BEAM_WIDTH="${BEAM_WIDTH:-4}"
SEARCH_IO_LIMIT="${SEARCH_IO_LIMIT:-4}"

CH() { "$CLICKHOUSE_BINARY" client --port="$PORT" --database="$DB" "$@"; }
CHQ() { CH --query "$@"; }

# --- Sanity ---
HDF5="$DATA_DIR/sift-128-euclidean.hdf5"
if [ ! -f "$HDF5" ]; then
    echo "missing $HDF5 - run ./download.sh first" >&2
    exit 1
fi

CHQ "CREATE DATABASE IF NOT EXISTS $DB"

# --- Server metadata ---
{
    echo "git_commit: $(git -C "$DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "server_version: $(CHQ 'SELECT version()')"
    echo "data_dir: $DATA_DIR"
    echo "K: $K"
    echo "queries_for_qps: $QUERIES_FOR_QPS"
    echo "search_list_sizes: $SEARCH_LIST_SIZES"
    echo "build_params: max_degree=$MAX_DEGREE build_search_list_size=$BUILD_SEARCH_LIST_SIZE alpha=$ALPHA beam_width=$BEAM_WIDTH search_io_limit=$SEARCH_IO_LIMIT"
} > "$RUN_DIR/server_meta.txt"

# --- Load query and ground-truth tables once (reused across sweep entries) ---
CHQ "DROP TABLE IF EXISTS sift_query"
CHQ "DROP TABLE IF EXISTS sift_gt"
CHQ "CREATE TABLE sift_query (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id"
CHQ "CREATE TABLE sift_gt    (query_id UInt32, neighbors Array(UInt32)) ENGINE = MergeTree ORDER BY query_id"
"$DIR/hdf5_to_rowbinary.py" "$HDF5" --schema query | CHQ "INSERT INTO sift_query FORMAT RowBinary"
"$DIR/hdf5_to_rowbinary.py" "$HDF5" --schema gt    | CHQ "INSERT INTO sift_gt    FORMAT RowBinary"

wait_for_full_coverage() {
    local table=$1
    local deadline=$((SECONDS + 1800))
    while [ $SECONDS -lt $deadline ]; do
        local row
        row=$(CHQ "SELECT tupleElement(tableANNCoverage('$DB', '$table'), 'total') = tupleElement(tableANNCoverage('$DB', '$table'), 'covered')")
        if [ "$row" = "1" ]; then
            return 0
        fi
        sleep 1
    done
    echo "TIMEOUT waiting for ANN coverage on $DB.$table" >&2
    return 1
}

# --- Sweep ---
SWEEP_TSV="$RUN_DIR/sweep.tsv"
{
    printf 'search_list_size\trecall@%d\tqueries\tqps\tp50_us\tp95_us\tp99_us\tindex_size_mb\tbuild_seconds\tdiskann_search_count_p50\tdiskann_search_us_p50\n' "$K"
} > "$SWEEP_TSV"

for SLS in $SEARCH_LIST_SIZES; do
    echo "=== search_list_size=$SLS ==="

    CHQ "DROP TABLE IF EXISTS sift_base"
    CH --param_max_degree="$MAX_DEGREE" \
       --param_build_search_list_size="$BUILD_SEARCH_LIST_SIZE" \
       --param_alpha="$ALPHA" \
       --param_search_list_size="$SLS" \
       --param_beam_width="$BEAM_WIDTH" \
       --param_search_io_limit="$SEARCH_IO_LIMIT" \
       --queries-file="$DIR/load_base_only.sql" >/dev/null

    "$DIR/hdf5_to_rowbinary.py" "$HDF5" --schema base | CHQ "INSERT INTO sift_base FORMAT RowBinary"

    BUILD_START=$SECONDS
    CHQ "SYSTEM BUILD ANN INDEX sift_base"
    wait_for_full_coverage sift_base
    BUILD_SECONDS=$((SECONDS - BUILD_START))

    INDEX_SIZE_MB=$(CHQ "SELECT round(sum(secondary_indices_compressed_bytes) / 1048576.0, 1) FROM system.parts WHERE database = '$DB' AND table = 'sift_base' AND active")

    # Recall@K: for every query row, run the ANN top-K and compare to the first
    # K ids of the ground truth list. Average length(intersect)/K across queries.
    RECALL=$(CHQ "
        WITH
            (SELECT count() FROM sift_query) AS Q
        SELECT round(sum(matched) / (Q * $K), 4)
        FROM (
            SELECT
                length(arrayIntersect(
                    arraySlice(g.neighbors, 1, $K),
                    arrayMap(t -> t.1,
                        arraySort(t -> t.2,
                            (SELECT groupArray((id, L2Distance(v, q.v))) FROM (
                                SELECT id, v FROM sift_base
                                ORDER BY L2Distance(v, q.v)
                                LIMIT $K
                            ))
                        )
                    )
                )) AS matched
            FROM sift_query AS q
            JOIN sift_gt AS g ON q.id = g.query_id
        )")

    # QPS measurement: clickhouse-benchmark over a stream of one query per row.
    # We render the parameterised query into a temporary file fed via --queries-file.
    BENCH_QUERIES="$RUN_DIR/sls_${SLS}_queries.sql"
    CHQ "
        SELECT 'SELECT id FROM sift_base ORDER BY L2Distance(v, [' ||
               arrayStringConcat(arrayMap(x -> toString(x), v), ',') ||
               ']::Array(Float32)) LIMIT $K FORMAT Null;'
        FROM sift_query LIMIT $QUERIES_FOR_QPS" > "$BENCH_QUERIES"

    BENCH_OUT="$RUN_DIR/sls_${SLS}_benchmark.json"
    "$CLICKHOUSE_BINARY" benchmark --port="$PORT" --database="$DB" \
        --concurrency=1 --iterations="$QUERIES_FOR_QPS" \
        --json="$BENCH_OUT" \
        --queries-file="$BENCH_QUERIES" >/dev/null 2>&1

    QPS=$(python3 -c "import json,sys; d=json.load(open('$BENCH_OUT')); print(round(d['statistics']['QPS'],2))")
    P50=$(python3 -c "import json,sys; d=json.load(open('$BENCH_OUT')); print(int(d['query_time_percentiles']['50']*1e6))")
    P95=$(python3 -c "import json,sys; d=json.load(open('$BENCH_OUT')); print(int(d['query_time_percentiles']['95']*1e6))")
    P99=$(python3 -c "import json,sys; d=json.load(open('$BENCH_OUT')); print(int(d['query_time_percentiles']['99']*1e6))")

    # Per-query ProfileEvents medians: pull from system.query_log filtered to the
    # benchmark window. We tag the queries via `log_comment` for cheap retrieval.
    SEARCH_COUNT_P50=$(CHQ "SELECT quantile(0.5)(toFloat64(ProfileEvents['DiskANNSearchCount'])) FROM system.query_log WHERE event_time > now() - INTERVAL 5 MINUTE AND has(ProfileEvents, 'DiskANNSearchCount') AND type = 'QueryFinish'")
    SEARCH_US_P50=$(CHQ   "SELECT quantile(0.5)(toFloat64(ProfileEvents['DiskANNSearchMicroseconds'])) FROM system.query_log WHERE event_time > now() - INTERVAL 5 MINUTE AND has(ProfileEvents, 'DiskANNSearchMicroseconds') AND type = 'QueryFinish'")

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$SLS" "$RECALL" "$QUERIES_FOR_QPS" "$QPS" "$P50" "$P95" "$P99" "$INDEX_SIZE_MB" "$BUILD_SECONDS" "$SEARCH_COUNT_P50" "$SEARCH_US_P50" \
        >> "$SWEEP_TSV"
done

echo "=== sweep complete ==="
column -t -s "$(printf '\t')" "$SWEEP_TSV"
echo
echo "Results: $RUN_DIR"
