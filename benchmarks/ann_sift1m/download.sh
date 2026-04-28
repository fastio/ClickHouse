#!/usr/bin/env bash
# Download and verify an ANN benchmark dataset.
#
# Two formats supported:
#
#   "hdf5"  ann-benchmarks.com single-file packaging:
#       train      (N_base, dim)    Float32  - base vectors
#       test       (N_query, dim)   Float32  - query vectors
#       neighbors  (N_query, k_gt)  Int32    - ground-truth top-k_gt neighbour ids
#       distances  (N_query, k_gt)  Float32  - ground-truth distances (unused here)
#     Mirror lives on Cloudflare HTTPS (no FTP data-channel pain).
#
#   "bin"   Big ANN Benchmarks (NeurIPS '21+) three-file packaging:
#       <name>/base.<scale>.<dtype>bin     header [int32 N, int32 D] + N*D body
#       <name>/query.public.<n>K.<dtype>bin  same layout
#       <name>/<scale>.ibin                  GT: [int32 Nq, int32 K] + Nq*K int32 + Nq*K float32
#     File sizes range from ~10s of GB (10M slice) to ~hundreds of GB (1B).
#
# Usage:
#   download.sh                                     # default: sift-128-euclidean
#   download.sh --dataset gist-960-euclidean
#   download.sh --dataset bigann-1B-euclidean
#
# Supported dataset names (must match cmd/sweep/main.go datasetRegistry):
#   sift-128-euclidean       128-d L2,    1M base, 10k query    (~500 MB,  hdf5)
#   gist-960-euclidean       960-d L2,    1M base, 1k query     (~3.6 GB,  hdf5)
#   deep-image-96-angular     96-d cosine, 9.99M base, 10k query (~3.7 GB,  hdf5)
#   bigann-1B-euclidean      128-d L2,    1B base, 10k query    (~135 GB,  bin, uint8)

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

# Each dataset declares (FORMAT, list of "URL|RELATIVE_DEST" entries). Common
# loop downloads them all. SHA verification is opt-in via $ANN_DATASET_SHA256
# (single-file datasets only; multi-file Big ANN datasets are too large to
# checksum from CI in a useful timeframe — sample-based verification belongs
# downstream of this script).
case "$DATASET" in
    sift-128-euclidean|gist-960-euclidean|deep-image-96-angular)
        FORMAT="hdf5"
        FILES=(
            "${ANN_DATASET_URL:-https://ann-benchmarks.com/${DATASET}.hdf5}|${DATASET}.hdf5"
        )
        ;;
    bigann-1B-euclidean)
        # NeurIPS '21 BIGANN-1B (Facebook AI Research mirror).
        # Total ~135 GB:  base 128 GB + query 1.2 MB + GT 8 MB.
        FORMAT="bin"
        BIGANN_BASE_URL="https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann"
        FILES=(
            "${BIGANN_BASE_URL}/base.1B.u8bin|bigann/base.1B.u8bin"
            "${BIGANN_BASE_URL}/query.public.10K.u8bin|bigann/query.public.10K.u8bin"
            "${BIGANN_BASE_URL}/GT.public.1B.ibin|bigann/GT.public.1B.ibin"
        )
        ;;
    *)
        echo "[download] ERROR: unsupported --dataset '$DATASET'" >&2
        echo "[download] supported: sift-128-euclidean, gist-960-euclidean, deep-image-96-angular, bigann-1B-euclidean" >&2
        exit 1
        ;;
esac

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$DIR/data"
EXPECTED_SHA256="${ANN_DATASET_SHA256:-}"
mkdir -p "$DATA_DIR"

# Pick the downloader once.
if command -v curl >/dev/null 2>&1; then
    DL=(curl --fail --location --continue-at - --output)
else
    DL=(wget -c -O)
fi

for entry in "${FILES[@]}"; do
    URL="${entry%%|*}"
    REL="${entry##*|}"
    DEST="$DATA_DIR/$REL"
    mkdir -p "$(dirname "$DEST")"
    if [ -f "$DEST" ] && [ -s "$DEST" ]; then
        echo "[download] skip (exists): $DEST"
        continue
    fi
    echo "[download] fetching $URL"
    echo "[download]       -> $DEST"
    "${DL[@]}" "$DEST.partial" "$URL"
    mv "$DEST.partial" "$DEST"
done

# Optional SHA256 (single-file datasets only).
if [ "$FORMAT" = "hdf5" ] && [ -n "$EXPECTED_SHA256" ]; then
    HDF5_PATH="$DATA_DIR/${FILES[0]##*|}"
    ACTUAL_SHA256=$(sha256sum "$HDF5_PATH" | awk '{print $1}')
    echo "[download] sha256: $ACTUAL_SHA256"
    if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
        echo "[download] ERROR: sha256 mismatch (expected $EXPECTED_SHA256)" >&2
        exit 1
    fi
fi

# Sanity-print the shape so the caller can eyeball the download.
case "$FORMAT" in
    hdf5)
        python3 - "$DATA_DIR/${FILES[0]##*|}" <<'EOF'
import sys, h5py
with h5py.File(sys.argv[1], "r") as f:
    for name in ("train", "test", "neighbors", "distances"):
        if name in f:
            ds = f[name]
            print(f"[download] {name}: shape={ds.shape}, dtype={ds.dtype}")
EOF
        ;;
    bin)
        # Each Big ANN file starts with [int32 N, int32 D] (or [Nq, K] for GT).
        # Print these so the operator can confirm the file is intact (truncated
        # downloads are the most common failure mode at 100GB scale).
        for entry in "${FILES[@]}"; do
            REL="${entry##*|}"
            FULL="$DATA_DIR/$REL"
            python3 - "$FULL" <<'EOF'
import os, struct, sys
p = sys.argv[1]
size = os.path.getsize(p)
with open(p, "rb") as f:
    a, b = struct.unpack("<ii", f.read(8))
print(f"[download] {p}: header=({a}, {b}) size={size:,} bytes")
EOF
        done
        ;;
esac

echo "[download] done ($DATASET, format=$FORMAT)"
