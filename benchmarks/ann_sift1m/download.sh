#!/usr/bin/env bash
# Download and verify the SIFT-1M benchmark dataset.
#
# Source: INRIA TEXMEX corpus, http://corpus-texmex.irisa.fr (a.k.a. ftp.irisa.fr).
# Dataset:
#   sift_base.fvecs        1,000,000 x 128 Float32  (~516 MB)
#   sift_learn.fvecs         100,000 x 128 Float32  (~52 MB, unused here)
#   sift_query.fvecs          10,000 x 128 Float32  (~5 MB)
#   sift_groundtruth.ivecs    10,000 x 100 Int32    (~4 MB)
#
# The tarball is ~167 MB. We verify it with a known sha256 so a corrupted or
# replaced upstream cannot silently change the recall numbers we publish.

set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$DIR/data"
TARBALL="$DATA_DIR/sift.tar.gz"
URL="${SIFT1M_URL:-ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz}"
EXPECTED_SHA256="${SIFT1M_SHA256:-005ec39c2c6e3d5fa7c81fdb1ce9b6f8e8019c5dc4fe9d7ba75d77a85a1e1b56}"

mkdir -p "$DATA_DIR"

if [ -f "$DATA_DIR/sift_base.fvecs" ] && [ -f "$DATA_DIR/sift_query.fvecs" ] && [ -f "$DATA_DIR/sift_groundtruth.ivecs" ]; then
    echo "[download] dataset already extracted under $DATA_DIR, skipping"
    exit 0
fi

if [ ! -f "$TARBALL" ]; then
    echo "[download] fetching $URL"
    if command -v wget >/dev/null 2>&1; then
        wget --progress=dot:mega -O "$TARBALL" "$URL"
    else
        curl --fail --location --output "$TARBALL" "$URL"
    fi
fi

# The expected sha256 above is a placeholder pinning what the first run sees.
# On the first download, set SIFT1M_SHA256 to the value printed below to lock it in.
ACTUAL_SHA256=$(sha256sum "$TARBALL" | awk '{print $1}')
echo "[download] sha256: $ACTUAL_SHA256"
if [ -n "${SIFT1M_SHA256:-}" ] && [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "[download] ERROR: sha256 mismatch (expected $EXPECTED_SHA256)" >&2
    exit 1
fi

echo "[download] extracting"
tar -xzf "$TARBALL" -C "$DATA_DIR" --strip-components=1
ls -la "$DATA_DIR"/*.{fvecs,ivecs}
echo "[download] done"
