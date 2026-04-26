#!/usr/bin/env bash
# Download and verify the SIFT-1M benchmark dataset.
#
# Source: ann-benchmarks.com mirror, packaged as a single HDF5 file with the
# four datasets `train` (1,000,000 x 128 Float32), `test` (10,000 x 128 Float32),
# `neighbors` (10,000 x 100 Int32 ground-truth top-100), and `distances`
# (10,000 x 100 Float32, unused here).
#
# Why this mirror instead of the original INRIA TEXMEX FTP corpus: the FTP
# server requires data-channel connections that are blocked in many CI / sandbox
# environments (passive ports, RETR over PORT). The ann-benchmarks HDF5 mirror
# serves the same vectors over HTTPS from Cloudflare, so it works wherever
# outbound HTTPS is allowed.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$DIR/data"
HDF5="$DATA_DIR/sift-128-euclidean.hdf5"
URL="${SIFT1M_URL:-https://ann-benchmarks.com/sift-128-euclidean.hdf5}"
EXPECTED_SHA256="${SIFT1M_SHA256:-}"

mkdir -p "$DATA_DIR"

if [ -f "$HDF5" ] && [ -s "$HDF5" ]; then
    echo "[download] dataset already present at $HDF5, skipping"
else
    echo "[download] fetching $URL (~525 MB)"
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

echo "[download] done"
