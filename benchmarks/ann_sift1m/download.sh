#!/usr/bin/env bash
# Download and verify an ann-benchmarks dataset.
#
# Each dataset is packaged as a single HDF5 file with four datasets:
#   train      (N_base, dim)    Float32  - base vectors
#   test       (N_query, dim)   Float32  - query vectors
#   neighbors  (N_query, k_gt)  Int32    - ground-truth top-k_gt neighbour ids
#   distances  (N_query, k_gt)  Float32  - ground-truth distances (unused here)
#
# Why this mirror instead of the original FTP / S3 corpora: ann-benchmarks.com
# serves all the standard benchmark vectors over HTTPS from Cloudflare, which
# works wherever outbound HTTPS is allowed (no FTP data-channel issues).
#
# Usage:
#   download.sh                                     # default: sift-128-euclidean
#   download.sh --dataset gist-960-euclidean
#   download.sh --dataset deep-image-96-angular
#
# Supported dataset names (must match cmd/sweep/main.go datasetRegistry):
#   sift-128-euclidean       128-d L2,    1M base, 10k query   (~500 MB)
#   gist-960-euclidean       960-d L2,    1M base, 1k query    (~3.6 GB)
#   deep-image-96-angular     96-d cosine, 9.99M base, 10k query (~3.7 GB)

set -euo pipefail

DATASET="sift-128-euclidean"
while [ $# -gt 0 ]; do
    case "$1" in
        --dataset)
            DATASET="$2"; shift 2
            ;;
        --dataset=*)
            DATASET="${1#*=}"; shift
            ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "$0" | sed 's/^# \{0,1\}//' | head -n -1
            exit 0
            ;;
        *)
            echo "unknown arg: $1" >&2
            exit 2
            ;;
    esac
done

case "$DATASET" in
    sift-128-euclidean|gist-960-euclidean|deep-image-96-angular)
        ;;
    *)
        echo "[download] ERROR: unsupported --dataset '$DATASET'" >&2
        echo "[download] supported: sift-128-euclidean, gist-960-euclidean, deep-image-96-angular" >&2
        exit 1
        ;;
esac

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$DIR/data"
HDF5="$DATA_DIR/${DATASET}.hdf5"
URL="${ANN_DATASET_URL:-https://ann-benchmarks.com/${DATASET}.hdf5}"
EXPECTED_SHA256="${ANN_DATASET_SHA256:-}"

mkdir -p "$DATA_DIR"

if [ -f "$HDF5" ] && [ -s "$HDF5" ]; then
    echo "[download] dataset already present at $HDF5, skipping"
else
    echo "[download] fetching $URL"
    if command -v curl >/dev/null 2>&1; then
        curl --fail --location --output "$HDF5.partial" "$URL"
    else
        wget -O "$HDF5.partial" "$URL"
    fi
    mv "$HDF5.partial" "$HDF5"
fi

ACTUAL_SHA256=$(sha256sum "$HDF5" | awk '{print $1}')
echo "[download] sha256: $ACTUAL_SHA256"
if [ -n "$EXPECTED_SHA256" ] && [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "[download] ERROR: sha256 mismatch (expected $EXPECTED_SHA256)" >&2
    exit 1
fi

# Print the dataset shape so a reader can sanity-check what was downloaded.
python3 - "$HDF5" <<'EOF'
import sys, h5py
with h5py.File(sys.argv[1], "r") as f:
    for name in ("train", "test", "neighbors", "distances"):
        if name in f:
            ds = f[name]
            print(f"[download] {name}: shape={ds.shape}, dtype={ds.dtype}")
EOF

echo "[download] done ($DATASET)"
