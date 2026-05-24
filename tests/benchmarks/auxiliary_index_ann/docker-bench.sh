#!/usr/bin/env bash
# Build the harness image (cached) and run bench.py inside it.
#
#   CH_BIN          host path to the clickhouse binary (default: ../../../build/programs/clickhouse)
#   ANN_DATA_DIR    host path of the ann_sift1m directory containing data/ + hdf5_to_rowbinary.py
#                   (default: /data/test/benchmarks/ann_sift1m)
#   RESULTS_DIR     host path for the results .jsonl   (default: ./results)
#   SERVER_DATA_DIR host path bind-mounted at /tmp/ch-bench inside the container.
#                   MUST live on a filesystem that supports O_DIRECT (real ext4/xfs/btrfs).
#                   Docker's default overlay2 writable layer does NOT support O_DIRECT and
#                   DiskANN's aligned reader will fail with EPERM if you leave the data dir
#                   inside the container layer.
#                   (default: $RESULTS_DIR/server-data, wiped on every run unless --keep-data-dir)
#   IMAGE           image tag                          (default: mi-ann-bench:latest)
#
# All extra args are forwarded to bench.py:
#   ./docker-bench.sh --algo diskann --dataset sift1m --preset /presets/diskann_sift1m.json
#   ./docker-bench.sh --algo spann   --dataset sift1m --preset /presets/spann_sift1m.json
#
# Notes:
#   * The binary is bind-mounted, not baked into the image. host glibc must
#     be ABI-compatible with the image (ubuntu:22.04, glibc 2.35).
#   * The image is rebuilt only when the harness sources or Dockerfile change.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CH_BIN="${CH_BIN:-$REPO_ROOT/build/programs/clickhouse}"
ANN_DATA_DIR="${ANN_DATA_DIR:-/data/test/benchmarks/ann_sift1m}"
RESULTS_DIR="${RESULTS_DIR:-$SCRIPT_DIR/results}"
SERVER_DATA_DIR="${SERVER_DATA_DIR:-$RESULTS_DIR/server-data}"
IMAGE="${IMAGE:-mi-ann-bench:latest}"

if [ ! -x "$CH_BIN" ]; then
    echo "clickhouse binary missing or not executable at $CH_BIN" >&2
    exit 2
fi
if [ ! -f "$ANN_DATA_DIR/hdf5_to_rowbinary.py" ]; then
    echo "hdf5_to_rowbinary.py missing under $ANN_DATA_DIR (expected ann_sift1m layout)" >&2
    exit 2
fi
mkdir -p "$RESULTS_DIR" "$SERVER_DATA_DIR"

docker build -t "$IMAGE" -f "$SCRIPT_DIR/Dockerfile" "$SCRIPT_DIR" >&2

# Default output goes to /out/results.jsonl unless the caller passed --output.
EXTRA_ARGS=("$@")
HAS_OUTPUT=0
for arg in "$@"; do
    [ "$arg" = "--output" ] && HAS_OUTPUT=1
done
if [ "$HAS_OUTPUT" = "0" ]; then
    EXTRA_ARGS+=("--output" "/out/results.jsonl")
fi

exec docker run --rm -i \
    --network=bridge \
    --security-opt seccomp=unconfined \
    -v "$CH_BIN:/ch/clickhouse:ro" \
    -v "$ANN_DATA_DIR:/data:ro" \
    -v "$RESULTS_DIR:/out" \
    -v "$SERVER_DATA_DIR:/tmp/ch-bench" \
    "$IMAGE" "${EXTRA_ARGS[@]}"
