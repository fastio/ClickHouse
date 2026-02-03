#pragma once

#include <config.h>
#include <cstring>
#include <cstdint>
#include <numeric>
#include <span>
#include <vector>
#include <Common/Exception.h>

#if USE_TURBOPFOR
#include <turbopfor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
}

/// TurboPFor block codec wrapper for posting list compression.
///
/// This file provides a static interface wrapper around the TurboPFor library,
/// enabling it to be used as a BlockCodec template parameter in PostingListCodecImpl.
///
/// TurboPFor is optimized for compressing sorted integer sequences (posting lists)
/// using patched frame-of-reference (PFor) compression with SIMD acceleration.
///
/// The codec applies delta encoding before compression and relies on TurboPFor's
/// integrated delta decoding during decompression. This is ideal for monotonically
/// increasing sequences like row IDs in posting lists.
///
/// Interface semantics match BitpackingBlockCodec and FastPForBlockCodec:
///   - encode()/decode() take span references and advance them past consumed/written data
///   - calculateNeededBytes() returns estimated bytes needed for compression

namespace impl
{

/// Static interface wrapper for TurboPFor codec.
///
/// Adapts TurboPFor library functions to the static BlockCodec interface required
/// by PostingListCodecImpl.
///
/// Format: [4-byte compressed_length][4-byte original_count][compressed_data]
/// - compressed_length: size of compressed data in bytes (excluding header)
/// - original_count: number of uint32_t integers encoded
/// - compressed_data: TurboPFor compressed stream (delta-encoded)
///
/// The encoding process:
/// 1. Apply delta encoding to input data (compute differences between consecutive values)
/// 2. Compress delta-encoded data using TurboPFor's p4Enc128v32
///
/// The decoding process:
/// 1. Decompress using TurboPFor's p4D1Dec128v32 (which integrates delta decoding)
/// 2. Output is the original data (delta decoding restores absolute values)
struct TurboPForBlockCodecImpl
{
    /// Block size for compression (128 elements for SSE4.2 SIMD).
    /// This matches the natural block size of p4Enc128v32/p4D1Dec128v32.
    static constexpr size_t BLOCK_SIZE = 128;

    /// Returns the codec name for identification.
    static constexpr const char * name() noexcept { return "turbopfor"; }

    /// Calculates needed bytes for encoding.
    /// @param data  Input integers (span reference, not modified)
    /// @return      Estimated bytes needed for compression
    static size_t calculateNeededBytes(const std::span<uint32_t> & data) noexcept
    {
        if (data.empty())
            return 0;

        /// Header: 4 bytes length prefix + 4 bytes count
        /// Data: worst-case is slightly larger than uncompressed for pathological data
        /// TurboPFor typically compresses very well, but we need safe upper bound
        return sizeof(uint32_t) * 2 + data.size() * sizeof(uint32_t) + 256;
    }

    /// Encodes integers from `in` to compressed bytes in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// The input `in` contains delta-encoded values from PostingListCodecBlockImpl.
    /// Since TurboPFor's p4D1Dec adds 1 to each stored value during decoding,
    /// we subtract 1 from each delta before encoding to compensate.
    /// After decoding, p4D1Dec will produce: sum(stored+1) = sum(delta-1+1) = sum(delta)
    ///
    /// Format: [4-byte compressed_length][4-byte count][compressed_data]
    ///
    /// @param in       Input deltas (span reference, advanced to end after encoding)
    /// @param out      Output buffer (span reference, advanced past written bytes)
    /// @return         Number of bytes written to `out`
    static size_t encode(std::span<uint32_t> & in, std::span<char> & out)
    {
        if (in.empty())
            return 0;

        constexpr size_t header_size = sizeof(uint32_t) * 2;  // length + count

        if (out.size() < header_size)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TurboPFor encode: output buffer too small for header, need {} but got {}",
                header_size, out.size());

        /// Convert deltas to delta-1 format for TurboPFor's p4D1Dec
        /// p4D1Dec does: out[i] = start + sum(stored[j]+1) for j=0..i
        /// If we store delta-1, then: out[i] = start + sum(delta-1+1) = start + sum(delta)
        /// which is exactly the prefix sum we want.
        std::vector<uint32_t> delta1_encoded(in.size());
        for (size_t i = 0; i < in.size(); ++i)
        {
            /// Deltas should be >= 0. For delta=0 case (can happen for first element),
            /// we use uint32 underflow which will be corrected by +1 during decode.
            delta1_encoded[i] = in[i] - 1;
        }

        /// Encode using TurboPFor's SIMD-optimized 128-element block encoder
        unsigned char * data_start = reinterpret_cast<unsigned char *>(out.data() + header_size);
        unsigned char * data_end = turbopfor::p4Enc128v32(
            delta1_encoded.data(),
            static_cast<unsigned>(delta1_encoded.size()),
            data_start
        );

        size_t compressed_bytes = static_cast<size_t>(data_end - data_start);

        /// Write header
        uint32_t length_prefix = static_cast<uint32_t>(compressed_bytes);
        uint32_t count = static_cast<uint32_t>(in.size());
        std::memcpy(out.data(), &length_prefix, sizeof(length_prefix));
        std::memcpy(out.data() + sizeof(uint32_t), &count, sizeof(count));

        size_t total_written = header_size + compressed_bytes;

        /// Advance spans
        in = in.subspan(in.size());
        out = out.subspan(total_written);

        return total_written;
    }

    /// Decodes compressed bytes from `in` to integers in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// Uses TurboPFor's p4D1Dec128v32 which does integrated delta decoding.
    /// Since we stored delta-1 values, p4D1Dec's +1 compensation gives us
    /// the correct prefix sums: out[i] = start + sum(delta) for deltas 0..i
    ///
    /// Format: [4-byte compressed_length][4-byte count][compressed_data]
    ///
    /// @param in       Compressed input (span reference, advanced past consumed bytes)
    /// @param count    Number of integers to decode (should match stored count)
    /// @param out      Output buffer (span reference, advanced past decoded integers)
    /// @return         Number of bytes consumed from `in`
    static size_t decode(std::span<const std::byte> & in, size_t count, std::span<uint32_t> & out)
    {
        if (count == 0)
            return 0;

        constexpr size_t header_size = sizeof(uint32_t) * 2;

        if (in.size() < header_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "TurboPFor decode: input buffer too small for header, need {} but got {}",
                header_size, in.size());

        if (out.size() < count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TurboPFor decode: output buffer too small, need {} but got {}", count, out.size());

        /// Read header
        uint32_t compressed_bytes;
        uint32_t stored_count;
        std::memcpy(&compressed_bytes, in.data(), sizeof(compressed_bytes));
        std::memcpy(&stored_count, in.data() + sizeof(uint32_t), sizeof(stored_count));

        if (stored_count != count)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "TurboPFor decode: count mismatch, stored {} but requested {}",
                stored_count, count);

        size_t total_block_size = header_size + compressed_bytes;
        if (in.size() < total_block_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "TurboPFor decode: input buffer too small, need {} but got {}",
                total_block_size, in.size());

        /// Decode using TurboPFor's SIMD-optimized 128-element block decoder with delta1
        /// p4D1Dec does: out[i] = start + sum(stored[j]+1) for j=0..i
        /// Since we stored delta-1, this gives: out[i] = start + sum(delta)
        unsigned char * data_start = const_cast<unsigned char *>(
            reinterpret_cast<const unsigned char *>(in.data() + header_size));

        turbopfor::p4D1Dec128v32(
            data_start,
            static_cast<unsigned>(count),
            out.data(),
            0  // start value
        );

        /// Advance spans
        in = in.subspan(total_block_size);
        out = out.subspan(count);

        return total_block_size;
    }

    /// Restore absolute row IDs by adding prev_value offset.
    /// TurboPFor's p4D1Dec128v32 already outputs prefix sums (cumulative sums of deltas).
    /// We just need to add prev_value to each element to get absolute row IDs.
    /// @param data      The prefix sums from decode (relative to 0)
    /// @param prev_value The previous row ID to add as offset
    /// @return          The last absolute value (new prev_value for next block)
    static uint32_t restoreDelta(std::vector<uint32_t> & data, uint32_t prev_value)
    {
        for (auto & v : data)
            v += prev_value;
        return data.empty() ? prev_value : data.back();
    }
};

}  // namespace impl

/// TurboPFor: Patched Frame-of-Reference with SIMD acceleration (SSE4.2/AVX2).
/// High compression ratio with very fast decode speed.
/// Optimized for sorted integer sequences like posting lists.
/// Uses 128-element SIMD blocks for optimal performance.
///
/// Note: This codec applies delta encoding during compression and delta decoding
/// during decompression. This is transparent to users but provides optimal
/// compression for monotonically increasing sequences.
using TurboPForBlockCodec = impl::TurboPForBlockCodecImpl;

}  // namespace DB

#endif // USE_TURBOPFOR
