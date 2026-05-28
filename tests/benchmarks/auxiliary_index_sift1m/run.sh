#!/usr/bin/env bash
# Full SIFT-1M AuxiliaryIndex recall benchmark.
#
# Steps:
#   1. init.sql              -- (re)create sift_base / sift_query / sift_gt
#   2. load.sh               -- stream HDF5 into the three tables
#   3. CREATE REFLECTION + SYSTEM SYNC
#   4. Generate 10000 per-query recall SELECTs, run them under
#      force_auxiliary_index = 'mi_sift' (so the MI fast path is taken),
#      sum |MI ∩ ground-truth| across queries.
#   5. recall_at_10 = sum / (10000 * 10); fail if < RECALL_THRESHOLD.
#
# Environment overrides:
#   CH_BIN              path to `clickhouse` binary (default: clickhouse)
#   CH_PORT             TCP port (default: 9000)
#   CH_DB               target database (default: default)
#   ANN_SIFT_DIR        source of HDF5 + converter
#                       (default: /data/test/benchmarks/ann_sift1m)
#   RECALL_THRESHOLD    pass/fail floor (default: 0.95)
#   SYNC_TIMEOUT_SEC    SYSTEM SYNC timeout (default: 3600 = 60 min;
#                       SIFT-1M graph build with default DiskANN params
#                       (num_threads=4, R=64, L_build=128) takes 30-60 min
#                       on a typical workstation; raise if hitting the cap)
#   QUERY_COUNT         queries used for recall (default: 10000 = full set)
#   KEEP_TABLES         1 to skip the drop/re-init step (default: 0)

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

CH_BIN="${CH_BIN:-clickhouse}"
CH_PORT="${CH_PORT:-9000}"
CH_DB="${CH_DB:-default}"
ANN_SIFT_DIR="${ANN_SIFT_DIR:-/data/test/benchmarks/ann_sift1m}"
RECALL_THRESHOLD="${RECALL_THRESHOLD:-0.95}"
SYNC_TIMEOUT_SEC="${SYNC_TIMEOUT_SEC:-3600}"
QUERY_COUNT="${QUERY_COUNT:-10000}"
KEEP_TABLES="${KEEP_TABLES:-0}"

CLIENT=("$CH_BIN" client --port="$CH_PORT" -d "$CH_DB")

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "ClickHouse: $("${CLIENT[@]}" -q 'SELECT version()')"
log "target DB: $CH_DB    threshold: recall@10 >= $RECALL_THRESHOLD    queries: $QUERY_COUNT"

# --- step 1: schema ----------------------------------------------------------
if [ "$KEEP_TABLES" = "0" ]; then
    log "init.sql"
    "${CLIENT[@]}" --multiquery < "$SCRIPT_DIR/init.sql"
else
    log "KEEP_TABLES=1 -> reusing existing tables"
fi

# --- step 2: load ------------------------------------------------------------
NEED_LOAD=1
if [ "$KEEP_TABLES" = "1" ]; then
    EXISTING=$("${CLIENT[@]}" -q "SELECT count() FROM sift_base" 2>/dev/null || echo 0)
    if [ "$EXISTING" -ge 1000000 ]; then
        log "sift_base already has $EXISTING rows -> skipping load"
        NEED_LOAD=0
    fi
fi
if [ "$NEED_LOAD" = "1" ]; then
    log "load.sh"
    ANN_SIFT_DIR="$ANN_SIFT_DIR" CH_BIN="$CH_BIN" CH_PORT="$CH_PORT" CH_DB="$CH_DB" \
        "$SCRIPT_DIR/load.sh"
fi

# --- step 3: build MI --------------------------------------------------------
# Build kwargs tuned for SIFT-1M (128-d L2). The DiskANN-in-code defaults
# (pq_chunks=4, R=32, max=64) are smoke-test sized and give recall@10 = 0 on
# real ANN workloads because the PQ codebook is too coarse and the graph is
# too sparse. The values below match the DiskANN paper SIFT-1M setup and were
# measured to deliver recall@10 ≈ 0.992 (see README "Threshold rationale").
log "DROP IF EXISTS mi_sift + CREATE REFLECTION (tuned: pq_chunks=32, R=64, max=96)"
"${CLIENT[@]}" --multiquery -q "
SET allow_experimental_auxiliary_index = 1;
DROP TABLE IF EXISTS mi_sift SYNC;
CREATE REFLECTION mi_sift
ON sift_base (v)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 128, diskann_pq_chunks = 32, diskann_pruned_degree = 64, diskann_max_degree = 96, diskann_l_build = 128, diskann_alpha = 1.2, diskann_num_threads = 16,
         auxiliary_index_sync_timeout = $SYNC_TIMEOUT_SEC;
"

log "SYSTEM SYNC REFLECTION mi_sift (timeout ${SYNC_TIMEOUT_SEC}s)"
"${CLIENT[@]}" --receive_timeout="$SYNC_TIMEOUT_SEC" \
    -q "SYSTEM SYNC REFLECTION mi_sift"

log "MI status:"
"${CLIENT[@]}" -q "
SELECT name, auxiliary_index_part_count, total_rows, total_bytes_on_disk, backlog_parts, pending_task_count
FROM system.auxiliary_indexes
WHERE database = currentDatabase() AND name = 'mi_sift' FORMAT Vertical
"

# --- step 4: per-query recall ------------------------------------------------
log "generating $QUERY_COUNT recall SELECTs"
TMPDIR_BENCH="$(mktemp -d -t mi_sift_recall.XXXXXX)"
trap 'rm -rf "$TMPDIR_BENCH"' EXIT
QUERIES_FILE="$TMPDIR_BENCH/recall.sql"

{
    echo "SET allow_experimental_auxiliary_index = 1;"
    echo "SET enable_auxiliary_index = 1;"
    echo "SET force_auxiliary_index = 'mi_sift';"
    # Strict mode: error out if any per-query SELECT would silently fall back
    # to a brute-force scan instead of using the MI. The post-hoc query_log
    # check below is a belt; this is the suspenders.
    echo "SET auxiliary_index_require_match = 1;"
    echo "SET log_queries = 1;"
    # CTE-free per-query SELECTs. Each prints a single integer (intersect count).
    # Scalar subquery as the reference vector folds to ColumnConst at planning,
    # which is what optimizeAuxiliaryIndex matches on.
    "${CLIENT[@]}" -q "
SELECT 'SELECT length(arrayIntersect((SELECT groupArray(id) FROM (SELECT id FROM sift_base ORDER BY L2Distance(v, (SELECT v FROM sift_query WHERE id = ' || toString(id) || ')) LIMIT 10)), (SELECT arraySlice(neighbors, 1, 10) FROM sift_gt WHERE query_id = ' || toString(id) || '))) SETTINGS log_comment = ''sift_recall_q' || toString(id) || ''';'
FROM sift_query
WHERE id < $QUERY_COUNT
ORDER BY id
FORMAT TSVRaw
"
} > "$QUERIES_FILE"

log "running $QUERY_COUNT per-query recall SELECTs (this may take a while)"
SUM_FILE="$TMPDIR_BENCH/sum.txt"
"${CLIENT[@]}" --multiquery < "$QUERIES_FILE" | awk '{s+=$1; n++} END {printf "%d %d\n", s, n}' > "$SUM_FILE"
read -r MATCHED RAN < "$SUM_FILE"
if [ "$RAN" -ne "$QUERY_COUNT" ]; then
    log "ERROR: expected $QUERY_COUNT results, got $RAN"
    exit 1
fi

# --- step 5: verify MI path actually fired ----------------------------------
"${CLIENT[@]}" -q "SYSTEM FLUSH LOGS query_log"
EVENTS_TRIGGERED=$("${CLIENT[@]}" -q "
SELECT countIf(ProfileEvents['AuxiliaryIndexDiskANNSearchStarted'] > 0)
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query_kind = 'Select'
  AND startsWith(log_comment, 'sift_recall_q')
  AND event_time > now() - INTERVAL 1 HOUR
")
if [ "$EVENTS_TRIGGERED" -lt "$QUERY_COUNT" ]; then
    log "ERROR: only $EVENTS_TRIGGERED / $QUERY_COUNT queries triggered the MI DiskANN path"
    log "       (the optimizer rejected the rewrite for some queries -> recall would be meaningless)"
    exit 1
fi

# --- step 6: assert ----------------------------------------------------------
RECALL=$(awk -v m="$MATCHED" -v n="$QUERY_COUNT" 'BEGIN { printf "%.6f", m / (n * 10.0) }')
log "matched=$MATCHED total=$((QUERY_COUNT * 10)) recall@10=$RECALL events=$EVENTS_TRIGGERED"

PASS=$(awk -v r="$RECALL" -v t="$RECALL_THRESHOLD" 'BEGIN { print (r >= t) ? 1 : 0 }')
if [ "$PASS" = "1" ]; then
    log "PASS: recall@10 = $RECALL >= $RECALL_THRESHOLD"
    exit 0
else
    log "FAIL: recall@10 = $RECALL < $RECALL_THRESHOLD"
    exit 1
fi
