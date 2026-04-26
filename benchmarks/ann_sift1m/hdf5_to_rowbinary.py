#!/usr/bin/env python3
"""Stream one dataset from an ann-benchmarks HDF5 file as ClickHouse RowBinary.

The HDF5 file packages four datasets:
  train      (N_base, dim)    Float32  - base vectors
  test       (N_query, dim)   Float32  - query vectors
  neighbors  (N_query, k_gt)  Int32    - ground-truth top-k_gt neighbour ids
  distances  (N_query, k_gt)  Float32  - ground-truth distances (unused here)

The script emits RowBinary for one of three target table shapes:

  --schema base   -> (id UInt64, v Array(Float32))    (from `train`)
  --schema query  -> (id UInt32, v Array(Float32))    (from `test`)
  --schema gt     -> (query_id UInt32, neighbors Array(UInt32))  (from `neighbors`)

ClickHouse RowBinary encodes `Array(T)` as a varint length followed by the
raw element bytes. The vector dimensionality is the same for every row in a
given dataset, so the varint header is encoded once and written verbatim.
Reading happens in chunks of 4096 rows so memory usage stays flat regardless
of N.

Usage:
  hdf5_to_rowbinary.py data/sift-128-euclidean.hdf5 --schema base \
      | clickhouse-client ... --query 'INSERT INTO sift_base FORMAT RowBinary'
"""

import argparse
import struct
import sys

import h5py
import numpy as np

CHUNK_ROWS = 4096


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


def stream(input_path: str, schema: str, output) -> None:
    with h5py.File(input_path, "r") as f:
        if schema == "base":
            ds = f["train"]
            id_dtype = np.dtype("<u8")  # UInt64
            element_dtype = np.dtype("<f4")  # Float32
        elif schema == "query":
            ds = f["test"]
            id_dtype = np.dtype("<u4")  # UInt32
            element_dtype = np.dtype("<f4")  # Float32
        elif schema == "gt":
            ds = f["neighbors"]
            id_dtype = np.dtype("<u4")  # UInt32
            element_dtype = np.dtype("<u4")  # UInt32 (Int32 in source, all >= 0)
        else:
            raise ValueError(f"unknown schema {schema}")

        n_rows, dim = ds.shape
        varint = encode_varint(int(dim))

        for start in range(0, n_rows, CHUNK_ROWS):
            end = min(start + CHUNK_ROWS, n_rows)
            chunk = np.asarray(ds[start:end]).astype(element_dtype, copy=False)
            ids = np.arange(start, end, dtype=id_dtype)
            id_bytes = ids.tobytes()
            row_payload = chunk.tobytes()  # row-major, contiguous
            row_size = id_dtype.itemsize + len(varint) + dim * element_dtype.itemsize
            # Interleave: id, varint, payload, id, varint, payload, ...
            # Done in Python because pre-building the full buffer is faster than
            # one write() per element but small enough to keep simple.
            buf = bytearray(row_size * (end - start))
            offset = 0
            stride = dim * element_dtype.itemsize
            for i in range(end - start):
                buf[offset:offset + id_dtype.itemsize] = id_bytes[i * id_dtype.itemsize:(i + 1) * id_dtype.itemsize]
                offset += id_dtype.itemsize
                buf[offset:offset + len(varint)] = varint
                offset += len(varint)
                buf[offset:offset + stride] = row_payload[i * stride:(i + 1) * stride]
                offset += stride
            output.write(bytes(buf))


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("input", help="path to the ann-benchmarks .hdf5 file")
    p.add_argument("--schema", required=True, choices=("base", "query", "gt"))
    args = p.parse_args()
    stream(args.input, args.schema, sys.stdout.buffer)


if __name__ == "__main__":
    main()
