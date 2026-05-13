#!/usr/bin/env bash
# Load SIFT-1M (sift-128-euclidean) into sift_base / sift_query / sift_gt.
#
# Reuses the existing converter shipped with the ann_sift1m benchmark
# infrastructure: HDF5 is streamed straight through hdf5_to_rowbinary.py
# into ClickHouse RowBinary INSERTs. If the dataset is missing on disk we
# fall back to download.sh from the same directory.
#
# Environment / arguments (with defaults):
#   ANN_SIFT_DIR    /data/test/benchmarks/ann_sift1m   (source of HDF5 + tools)
#   CH_BIN          clickhouse                         (binary on $PATH)
#   CH_PORT         9000                               (TCP port)
#   CH_DB           default                            (target database)

set -euo pipefail

ANN_SIFT_DIR="${ANN_SIFT_DIR:-/data/test/benchmarks/ann_sift1m}"
HDF5_PATH="$ANN_SIFT_DIR/data/sift-128-euclidean.hdf5"
CONVERTER="$ANN_SIFT_DIR/hdf5_to_rowbinary.py"

CH_BIN="${CH_BIN:-clickhouse}"
CH_PORT="${CH_PORT:-9000}"
CH_DB="${CH_DB:-default}"

if [ ! -x "$CONVERTER" ]; then
    echo "[load] converter missing or not executable: $CONVERTER" >&2
    exit 2
fi

if [ ! -f "$HDF5_PATH" ]; then
    echo "[load] SIFT-1M HDF5 missing, invoking $ANN_SIFT_DIR/download.sh"
    "$ANN_SIFT_DIR/download.sh" --dataset sift-128-euclidean
fi

CLIENT=("$CH_BIN" client --port="$CH_PORT" -d "$CH_DB")

# Pair (HDF5 schema selector, target ClickHouse table)
for pair in base:sift_base query:sift_query gt:sift_gt; do
    schema="${pair%%:*}"
    table="${pair##*:}"
    echo "[load] streaming HDF5 '$schema' -> $CH_DB.$table"
    python3 "$CONVERTER" "$HDF5_PATH" --schema "$schema" \
        | "${CLIENT[@]}" -q "INSERT INTO $table FORMAT RowBinary"
done

# Show row counts so the operator can eyeball the load.
"${CLIENT[@]}" -q "
SELECT 'sift_base   ' AS tname, count() AS rows FROM sift_base
UNION ALL SELECT 'sift_query  ', count() FROM sift_query
UNION ALL SELECT 'sift_gt     ', count() FROM sift_gt
FORMAT TSV
"
