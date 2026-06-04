#!/usr/bin/env bash
# Convenience wrapper for running one ANNIndex benchmark point in Docker.
#
# Examples:
#   ./run.sh sift1m diskann
#   ./run.sh sift1m spann --query-count 10000
#   ./run.sh shell mi-ann-bench-sift1m-diskann
#   ./run.sh status mi-ann-bench-sift1m-diskann

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DOCKER_BENCH="$SCRIPT_DIR/docker-bench.sh"

usage()
{
    cat <<'EOF'
Usage:
  ./run.sh <dataset> <diskann|spann> [bench.py args...]
  ./run.sh help
  ./run.sh shell [container_name]
  ./run.sh status [container_name]

Examples:
  ./run.sh sift1m diskann
  ./run.sh sift1m spann --query-count 10000 --qps-iterations 2000 --qps-concurrencies 1,16
  ./run.sh sift1m diskann --reuse-index                  # reuse existing index, run recall + QPS
  ./run.sh sift1m diskann --reuse-index --skip-recall    # reuse existing index, skip recall, QPS only
  ./run.sh shell mi-ann-bench-sift1m-diskann
  ./run.sh status mi-ann-bench-sift1m-diskann

What it does:
  * Builds/uses the benchmark Docker image via docker-bench.sh.
  * Runs one dataset/algorithm benchmark point.
  * Prints key paths, container name, and docker exec command before running.
  * Appends one JSON result to RESULTS_DIR/results_<dataset>_<algo>_<timestamp>.jsonl.
  * Prints a compact final summary: build time, recall@10, QPS, index size.

Environment overrides:
  CH_BIN             Host clickhouse binary. Default: <repo>/build/programs/clickhouse
  ANN_DATA_DIR       Host ANN data directory. Default: /data/test/benchmarks/ann_sift1m
  RESULTS_DIR        Host result directory. Default: <repo>/tmp/ann_bench/<timestamp>_<dataset>_<algo>
  SERVER_DATA_DIR    Host server data directory. Default: $RESULTS_DIR/server-data
  CONTAINER_NAME     Docker container name. Default: mi-ann-bench-<dataset>-<algo>
  QUERY_COUNT        Default --query-count if not passed. Default: 1000
  QPS_ITERATIONS     Default --qps-iterations if not passed. Default: 1000
  QPS_CONCURRENCIES  Default --qps-concurrencies if not passed. Default: 1,16
  KEEP_DATA_DIR      Set to 0 to wipe data dir after run. Default: 1 (passes --keep-data-dir)
  PORT_MODE          Passed to docker-bench.sh. Default: host

Inspect while running from another terminal:
  docker exec -it <container_name> bash
  docker logs -f <container_name>
  cat <SERVER_DATA_DIR>/ports.env
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

command="${1:-help}"
case "$command" in
    help|-h|--help)
        usage
        exit 0
        ;;
    shell)
        container="${2:-${CONTAINER_NAME:-mi-ann-bench}}"
        exec docker exec -it "$container" bash
        ;;
    status)
        container="${2:-${CONTAINER_NAME:-mi-ann-bench}}"
        echo "== docker ps =="
        docker ps --filter "name=$container"
        echo
        echo "== recent logs =="
        docker logs --tail 80 "$container"
        exit 0
        ;;
esac

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
export SERVER_DATA_DIR="${SERVER_DATA_DIR:-$RESULTS_DIR/server-data}"
export CONTAINER_NAME="${CONTAINER_NAME:-mi-ann-bench-${safe_dataset}-${safe_algo}}"
export PORT_MODE="${PORT_MODE:-host}"

result_file="$RESULTS_DIR/results_${safe_dataset}_${safe_algo}_${ts}.jsonl"
bench_args=(
    --algo "$algo"
    --dataset "$dataset"
    --preset "/presets/${algo}_${dataset}.json"
    --output "/out/$(basename "$result_file")"
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
if [[ "${KEEP_DATA_DIR:-1}" != "0" ]] && ! has_arg --keep-data-dir "$@"; then
    bench_args+=(--keep-data-dir)
fi
bench_args+=("$@")

mkdir -p "$RESULTS_DIR" "$SERVER_DATA_DIR"

# Read dataset metadata (dim / metric / base_count / query_count / format) from
# harness/datasets.py so the banner shows scale info up-front.
dataset_meta="$(
    PYTHONPATH="$SCRIPT_DIR/harness" python3 - "$dataset" <<'PY'
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
    # base_count × dim × 4B (float32) is a useful raw-data sanity check; not
    # exact for uint8/int8 datasets but good enough for an order-of-magnitude.
    raw_bytes=$(( ds_base_count * ds_dim * 4 ))
    printf -v raw_gib '%.2f' "$(awk "BEGIN { printf \"%.4f\", $raw_bytes / (1024*1024*1024) }")"
    dataset_info=$(printf 'dim=%s  metric=%s  base=%s  queries=%s  format=%s  raw≈%s GiB (fp32)' \
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
server data dir:  $SERVER_DATA_DIR
result file:      $result_file
container:        $CONTAINER_NAME
port mode:        $PORT_MODE

Inspect while running from another terminal:
  docker exec -it $CONTAINER_NAME bash
  docker logs -f $CONTAINER_NAME
  cat $SERVER_DATA_DIR/ports.env

Starting Docker benchmark...
EOF

"$DOCKER_BENCH" "${bench_args[@]}"

print_summary "$result_file"
