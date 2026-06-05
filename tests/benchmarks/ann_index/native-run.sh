#!/usr/bin/env bash
# Convenience wrapper for running one ANNIndex benchmark point against an
# externally managed ClickHouse server.
#
# Examples:
#   ./native-run.sh --host 127.0.0.1 --port 9000 sift1m diskann
#   ./native-run.sh --host=127.0.0.1 --port=9000 --database=default --user=default sift1m spann --query-count 10000

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
BENCH_PY="$SCRIPT_DIR/harness/bench.py"

usage()
{
    cat <<'EOF'
Usage:
  ./native-run.sh [connection options] <dataset> <diskann|spann> [bench.py args...]
  ./native-run.sh help

Connection options:
  --host HOST          External ClickHouse host. Default: 127.0.0.1
  --port PORT          External ClickHouse native TCP port. Default: 9000
  --database DATABASE  External ClickHouse database. Default: default
  --user USER          External ClickHouse user. Default: default

Examples:
  ./native-run.sh --host 127.0.0.1 --port 9000 sift1m diskann
  ./native-run.sh --host=127.0.0.1 --port=9000 --database=default --user=default sift1m spann --query-count 10000
  ./native-run.sh --host 10.0.0.2 --port 9000 sift1m diskann --reuse-index --skip-recall

What it does:
  * Connects to an externally managed ClickHouse server; it does not start Docker.
  * Clears the target database if it exists, or creates it if it does not.
  * Runs one dataset/algorithm benchmark point.
  * Prints key paths and the clickhouse-client command before running.
  * Appends one JSON result to RESULTS_DIR/results_<dataset>_<algo>_<timestamp>.jsonl.
  * Prints a compact final summary: build time, recall@10, QPS, index size.

Environment overrides:
  CH_BIN             Host clickhouse binary used for client/benchmark. Default: <repo>/build/programs/clickhouse
  ANN_DATA_DIR       Host ANN data directory. Default: /data/test/benchmarks/ann_sift1m
  RESULTS_DIR        Host result directory. Default: <repo>/tmp/ann_bench/<timestamp>_<dataset>_<algo>
  QUERY_COUNT        Default --query-count if not passed. Default: 1000
  QPS_ITERATIONS     Default --qps-iterations if not passed. Default: 1000
  QPS_CONCURRENCIES  Default --qps-concurrencies if not passed. Default: 1,16

Inspect while running from another terminal:
  <CH_BIN> client --host <host> --port <port> --database <database> --user <user>
EOF
}

has_arg()
{
    local needle="$1"
    shift
    for arg in "$@"; do
        if [[ "$arg" == "$needle" ]]; then
            return 0
        fi
    done
    return 1
}

print_summary()
{
    local result_file="$1"
    python3 - "$result_file" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print(f"Result file not found: {path}", file=sys.stderr)
    sys.exit(1)

records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
if not records:
    print(f"No JSON records in {path}", file=sys.stderr)
    sys.exit(1)

r = records[-1]
qps = r.get("qps", {})
print("\n=== Final result ===")
print(f"algo={r.get('algo')} dataset={r.get('dataset')} query_count={r.get('query_count')}")
print(f"build_seconds={r.get('build_seconds')} recall_at_10={r.get('recall_at_10')} index_bytes={r.get('index_bytes')}")
for name in sorted(qps):
    item = qps[name]
    print(
        f"{name}: qps={item.get('qps')} "
        f"p50_ms={item.get('latency_ms_p50')} "
        f"p95_ms={item.get('latency_ms_p95')} "
        f"p99_ms={item.get('latency_ms_p99')}"
    )
print(f"raw_result={path}")
PY
}

host="${CH_HOST:-127.0.0.1}"
port="${CH_PORT:-9000}"
database="${CH_DATABASE:-default}"
user="${CH_USER:-default}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        help|-h|--help)
            usage
            exit 0
            ;;
        --host)
            host="${2:?--host requires a value}"
            shift 2
            ;;
        --host=*)
            host="${1#--host=}"
            shift
            ;;
        --port)
            port="${2:?--port requires a value}"
            shift 2
            ;;
        --port=*)
            port="${1#--port=}"
            shift
            ;;
        --database)
            database="${2:?--database requires a value}"
            shift 2
            ;;
        --database=*)
            database="${1#--database=}"
            shift
            ;;
        --user)
            user="${2:?--user requires a value}"
            shift 2
            ;;
        --user=*)
            user="${1#--user=}"
            shift
            ;;
        *)
            break
            ;;
    esac
done

if [[ $# -lt 2 ]]; then
    usage >&2
    exit 2
fi

dataset="$1"
algo="$2"
shift 2

case "$algo" in
    diskann|spann) ;;
    *)
        echo "Algorithm must be 'diskann' or 'spann', got: $algo" >&2
        exit 2
        ;;
esac

preset="$SCRIPT_DIR/presets/${algo}_${dataset}.json"
if [[ ! -f "$preset" ]]; then
    echo "Preset not found: $preset" >&2
    echo "Expected file name pattern: presets/${algo}_${dataset}.json" >&2
    exit 2
fi

safe_dataset="${dataset//[^A-Za-z0-9_.-]/_}"
safe_algo="${algo//[^A-Za-z0-9_.-]/_}"
ts="$(date +%Y%m%d_%H%M%S)"

export CH_BIN="${CH_BIN:-$REPO_ROOT/build/programs/clickhouse}"
export ANN_DATA_DIR="${ANN_DATA_DIR:-/data/test/benchmarks/ann_sift1m}"
export RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/tmp/ann_bench/${ts}_${safe_dataset}_${safe_algo}}"

if [[ ! -x "$CH_BIN" ]]; then
    echo "clickhouse binary missing or not executable at $CH_BIN" >&2
    exit 2
fi
if [[ ! -f "$ANN_DATA_DIR/hdf5_to_rowbinary.py" ]]; then
    echo "hdf5_to_rowbinary.py missing under $ANN_DATA_DIR (expected ann_sift1m layout)" >&2
    exit 2
fi

result_file="$RESULTS_DIR/results_${safe_dataset}_${safe_algo}_${ts}.jsonl"
bench_args=(
    --external-server
    --host "$host"
    --port "$port"
    --database "$database"
    --user "$user"
    --binary "$CH_BIN"
    --algo "$algo"
    --dataset "$dataset"
    --preset "$preset"
    --output "$result_file"
)

if ! has_arg --query-count "$@"; then
    bench_args+=(--query-count "${QUERY_COUNT:-1000}")
fi
if ! has_arg --qps-iterations "$@"; then
    bench_args+=(--qps-iterations "${QPS_ITERATIONS:-1000}")
fi
if ! has_arg --qps-concurrencies "$@"; then
    bench_args+=(--qps-concurrencies "${QPS_CONCURRENCIES:-1,16}")
fi
bench_args+=("$@")

mkdir -p "$RESULTS_DIR"

# Read dataset metadata (dim / metric / base_count / query_count / format) from
# harness/datasets.py so the banner shows scale info up-front.
dataset_meta="$(
    ANN_DATA_DIR="$ANN_DATA_DIR" PYTHONPATH="$SCRIPT_DIR/harness" python3 - "$dataset" <<'PY'
import sys
from datasets import DATASETS
ds = DATASETS.get(sys.argv[1])
if ds is None:
    sys.exit(0)
print(f"{ds.dim}\t{ds.metric}\t{ds.base_count}\t{ds.query_count}\t{ds.format}")
PY
)"
if [[ -n "$dataset_meta" ]]; then
    IFS=$'\t' read -r ds_dim ds_metric ds_base_count ds_query_count ds_format <<<"$dataset_meta"
    raw_bytes=$(( ds_base_count * ds_dim * 4 ))
    printf -v raw_gib '%.2f' "$(awk "BEGIN { printf \"%.4f\", $raw_bytes / (1024*1024*1024) }")"
    dataset_info=$(printf 'dim=%s  metric=%s  base=%s  queries=%s  format=%s  raw~%s GiB (fp32)' \
        "$ds_dim" "$ds_metric" "$ds_base_count" "$ds_query_count" "$ds_format" "$raw_gib")
else
    dataset_info="(unknown dataset; metadata not found in datasets.py)"
fi

cat <<EOF
=== ANNIndex benchmark ===
dataset:          $dataset
dataset info:     $dataset_info
algo:             $algo
preset:           $preset
clickhouse:       $CH_BIN
data dir:         $ANN_DATA_DIR
results dir:      $RESULTS_DIR
result file:      $result_file
server:           $host:$port
database:         $database
user:             $user

Inspect while running from another terminal:
  $CH_BIN client --host $host --port $port --database $database --user $user

Starting native benchmark...
EOF

ANN_DATA_DIR="$ANN_DATA_DIR" python3 "$BENCH_PY" "${bench_args[@]}"

print_summary "$result_file"
