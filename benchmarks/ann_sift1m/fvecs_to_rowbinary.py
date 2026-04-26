#!/usr/bin/env python3
"""Stream a .fvecs / .ivecs file as ClickHouse RowBinary.

`.fvecs` and `.ivecs` are the INRIA TEXMEX corpus formats: one row per vector,
each row laid out as `<int32 dim><dim x float32>` (`.fvecs`) or
`<int32 dim><dim x int32>` (`.ivecs`). All integers are little-endian, matching
ClickHouse's wire format on x86 / aarch64.

The script reads stdin (or a file) and writes RowBinary for one of three table
shapes:

  --schema base   -> (id UInt64, v Array(Float32))
  --schema query  -> (id UInt32, v Array(Float32))
  --schema gt     -> (query_id UInt32, neighbors Array(UInt32))

ClickHouse RowBinary encodes `Array(T)` as a varint length followed by the raw
elements, so for the SIFT vector dimensions (128 / 100) we emit either two
bytes (varint for 128 = 0x80 0x01) or one byte (varint for 100 = 0x64) per row,
plus the integer id and the raw element bytes. Reading and writing in chunks of
4096 rows keeps memory usage flat regardless of the input size.

Usage:
  fvecs_to_rowbinary.py --schema base data/sift_base.fvecs    | clickhouse-client ... --query 'INSERT INTO sift_base FORMAT RowBinary'
  fvecs_to_rowbinary.py --schema query data/sift_query.fvecs  | clickhouse-client ... --query 'INSERT INTO sift_query FORMAT RowBinary'
  fvecs_to_rowbinary.py --schema gt data/sift_groundtruth.ivecs | clickhouse-client ... --query 'INSERT INTO sift_gt FORMAT RowBinary'
"""

import argparse
import struct
import sys

CHUNK_ROWS = 4096


def write_varint(out, value):
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.write(bytes([byte | 0x80]))
        else:
            out.write(bytes([byte]))
            return


def read_dim(buf):
    if len(buf) < 4:
        return None
    return struct.unpack_from("<i", buf, 0)[0]


def stream(input_path, schema, output):
    if input_path == "-":
        in_file = sys.stdin.buffer
    else:
        in_file = open(input_path, "rb")
    try:
        if schema == "base":
            id_struct = struct.Struct("<Q")
            element_size = 4   # Float32
            id_seed = 0
        elif schema == "query":
            id_struct = struct.Struct("<I")
            element_size = 4   # Float32
            id_seed = 0
        elif schema == "gt":
            id_struct = struct.Struct("<I")
            element_size = 4   # Int32 stored as UInt32 (positive ids)
            id_seed = 0
        else:
            raise ValueError(f"unknown schema {schema}")

        # First row: peek the dimension to compute the row size, then loop in chunks.
        header = in_file.read(4)
        if not header:
            return
        dim = struct.unpack("<i", header)[0]
        if dim <= 0 or dim > 1 << 20:
            raise ValueError(f"implausible vector dimension {dim}")
        row_payload_size = dim * element_size
        row_total_size = 4 + row_payload_size  # header + payload, repeated

        # Encode the array length once: it is identical for every row.
        varint_buf = bytearray()
        v = dim
        while True:
            b = v & 0x7F
            v >>= 7
            if v:
                varint_buf.append(b | 0x80)
            else:
                varint_buf.append(b)
                break
        varint_bytes = bytes(varint_buf)

        # Process the rest of the stream in chunks. We've already consumed the
        # header of the first row, so emit it manually before the loop.
        first_payload = in_file.read(row_payload_size)
        if len(first_payload) != row_payload_size:
            raise ValueError("truncated first row")
        idx = id_seed
        output.write(id_struct.pack(idx))
        output.write(varint_bytes)
        output.write(first_payload)
        idx += 1

        chunk_size = CHUNK_ROWS * row_total_size
        while True:
            chunk = in_file.read(chunk_size)
            if not chunk:
                break
            if len(chunk) % row_total_size != 0:
                raise ValueError(f"trailing partial row of {len(chunk) % row_total_size} bytes")
            n_rows = len(chunk) // row_total_size
            offset = 0
            for _ in range(n_rows):
                row_dim = struct.unpack_from("<i", chunk, offset)[0]
                if row_dim != dim:
                    raise ValueError(f"row {idx} reported dim={row_dim}, expected {dim}")
                output.write(id_struct.pack(idx))
                output.write(varint_bytes)
                output.write(chunk[offset + 4 : offset + row_total_size])
                offset += row_total_size
                idx += 1
    finally:
        if input_path != "-":
            in_file.close()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input", help="path to .fvecs/.ivecs file (or '-' for stdin)")
    p.add_argument("--schema", required=True, choices=("base", "query", "gt"))
    args = p.parse_args()
    stream(args.input, args.schema, sys.stdout.buffer)


if __name__ == "__main__":
    main()
