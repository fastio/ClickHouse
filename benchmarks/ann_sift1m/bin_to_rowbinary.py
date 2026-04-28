#!/usr/bin/env python3
"""Stream a Big ANN binary file as ClickHouse RowBinary.

Big ANN file formats (https://big-ann-benchmarks.com/):

  Base / query files: .u8bin (uint8), .i8bin (int8), .fbin (float32)
    Header: int32 N (little-endian), int32 D
    Body:   N rows of D values (dtype as above), row-major

  Ground truth files: .ibin (extension also seen as .bin / .gt100 / no suffix)
    Header: int32 N_queries, int32 K
    Body:   N_queries * K int32 neighbour ids
            N_queries * K float32 distances (we discard distances)

Schemas (mirrors hdf5_to_rowbinary.py for plug-compat with the Go harness):

  --schema base   -> (id UInt64, v Array(Float32))
                     reads base file; uint8/int8 are widened to float32 row-by-row
                     because the ClickHouse ANN index only operates on Array(Float32).
                     uint8 -> float32 and int8 -> float32 are exact (no scaling).

  --schema query  -> (id UInt32, v Array(Float32))
                     same as base, smaller id type to match the existing `queries` table.

  --schema gt     -> (query_id UInt32, neighbors Array(UInt32))
                     reads the ibin GT, drops the distances block, keeps full K per query.

Streaming model: the body of base / query files is mmapped (np.memmap), so peak
RSS stays at chunk-size regardless of the 100GB+ file size. Each chunk is encoded
in a single vectorised pass — no per-row Python loop — which keeps the reader from
becoming the bottleneck at 1B rows.
"""

import argparse
import struct
import sys

import numpy as np


CHUNK_ROWS = 4096

DTYPE_MAP = {
    "uint8": np.uint8,
    "int8": np.int8,
    "float32": np.float32,
}


def encode_varint(value: int) -> bytes:
    out = bytearray()
    while True:
        b = value & 0x7F
        value >>= 7
        if value:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def read_header(path: str) -> tuple[int, int]:
    with open(path, "rb") as f:
        buf = f.read(8)
    if len(buf) != 8:
        raise IOError(f"file too short to contain header: {path}")
    n, d = struct.unpack("<ii", buf)
    return n, d


def stream_vectors(path: str, schema: str, dtype_name: str, rows_limit: int, output) -> None:
    n_total, d = read_header(path)
    elem_dtype_in = DTYPE_MAP[dtype_name]
    elem_dtype_out = np.dtype("<f4")
    item_in = np.dtype(elem_dtype_in).itemsize

    n = n_total
    if rows_limit > 0 and rows_limit < n_total:
        n = rows_limit

    # mmap the body. offset=8 skips the (N, D) header.
    arr = np.memmap(path, dtype=elem_dtype_in, mode="r", offset=8, shape=(n_total, d))

    id_dtype = np.dtype("<u8") if schema == "base" else np.dtype("<u4")
    id_size = id_dtype.itemsize
    varint = encode_varint(int(d))
    varint_size = len(varint)
    payload_size = d * elem_dtype_out.itemsize
    row_size = id_size + varint_size + payload_size

    # Pre-build a (CHUNK_ROWS, varint_size) tile of varint bytes so each chunk's
    # middle column can be assigned in one vectorised broadcast. The last chunk
    # of the stream gets a smaller view.
    varint_tile = np.frombuffer(varint, dtype=np.uint8)

    print(f"[bin_to_rowbinary] schema={schema} path={path} n={n} (of {n_total}) d={d} "
          f"dtype_in={dtype_name} -> Array(Float32)", file=sys.stderr)

    for start in range(0, n, CHUNK_ROWS):
        end = min(start + CHUNK_ROWS, n)
        rows = end - start

        # Read + dtype-widen.
        chunk = np.asarray(arr[start:end]).astype(elem_dtype_out, copy=True)

        ids = np.arange(start, end, dtype=id_dtype)

        # Build the full chunk buffer in one allocation; fill the three columns
        # (id, varint, payload) with vectorised assignments.
        buf = np.empty((rows, row_size), dtype=np.uint8)
        buf[:, 0:id_size] = ids.view(np.uint8).reshape(rows, id_size)
        buf[:, id_size:id_size + varint_size] = varint_tile  # broadcast along axis 0
        buf[:, id_size + varint_size:] = chunk.view(np.uint8).reshape(rows, payload_size)

        output.write(buf.tobytes())


def stream_gt(path: str, output) -> None:
    """Read a Big ANN ground-truth (.ibin) file and emit RowBinary for table
    (query_id UInt32, neighbors Array(UInt32))."""
    with open(path, "rb") as f:
        buf = f.read(8)
        if len(buf) != 8:
            raise IOError(f"GT file too short: {path}")
        n_q, k = struct.unpack("<ii", buf)
        # Body: n_q*k int32 neighbours, then n_q*k float32 distances.
        # Distances are skipped; their offset is implicit since we read sequentially.
        neighbours = np.fromfile(f, dtype=np.int32, count=n_q * k)
        if neighbours.size != n_q * k:
            raise IOError(f"GT file truncated: read {neighbours.size}, expected {n_q * k}")
    neighbours = neighbours.reshape(n_q, k).astype(np.uint32, copy=False)

    print(f"[bin_to_rowbinary] schema=gt path={path} n_q={n_q} k={k}", file=sys.stderr)

    id_dtype = np.dtype("<u4")
    id_size = id_dtype.itemsize
    varint = encode_varint(int(k))
    varint_size = len(varint)
    elem_size = 4  # UInt32
    payload_size = k * elem_size
    row_size = id_size + varint_size + payload_size

    varint_tile = np.frombuffer(varint, dtype=np.uint8)

    for start in range(0, n_q, CHUNK_ROWS):
        end = min(start + CHUNK_ROWS, n_q)
        rows = end - start
        chunk = neighbours[start:end]
        ids = np.arange(start, end, dtype=id_dtype)

        buf = np.empty((rows, row_size), dtype=np.uint8)
        buf[:, 0:id_size] = ids.view(np.uint8).reshape(rows, id_size)
        buf[:, id_size:id_size + varint_size] = varint_tile
        buf[:, id_size + varint_size:] = chunk.view(np.uint8).reshape(rows, payload_size)

        output.write(buf.tobytes())


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("input", help="path to the .u8bin/.i8bin/.fbin/.ibin file")
    p.add_argument("--schema", required=True, choices=("base", "query", "gt"))
    p.add_argument("--dtype", default="float32", choices=tuple(DTYPE_MAP.keys()),
                   help="element dtype for base/query schemas; ignored for gt")
    p.add_argument("--rows-limit", type=int, default=0,
                   help="for base schema only: read first N rows (0 = all). "
                        "Use to slice 1B -> smaller; recall numbers are only meaningful "
                        "if a matching GT file is supplied for the same slice.")
    args = p.parse_args()

    if args.schema == "gt":
        stream_gt(args.input, sys.stdout.buffer)
    else:
        stream_vectors(args.input, args.schema, args.dtype, args.rows_limit, sys.stdout.buffer)


if __name__ == "__main__":
    main()
