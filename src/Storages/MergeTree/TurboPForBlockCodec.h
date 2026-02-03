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
/// TurboPFor is optimized for compressing integer sequences using patched
/// frame-of-reference (PFor) compression with SIMD acceleration (SSE4.2/AVX2).
///
/// IMPORTANT: Interface semantics match BitpackingBlockCodec and FastPForBlockCodec:
///   - encode() takes delta values as input and produces compressed bytes
///   - decode() takes compressed bytes and produces delta values as output
///   - restoreDelta() converts delta values to absolute values using prefix sum
///
/// This ensures consistent behavior regardless of which codec is used.

namespace impl
{

/// Static interface wrapper for TurboPFor codec.
///
/// Adapts TurboPFor library functions to the static BlockCodec interface required
/// by PostingListCodecImpl.
///
/// TurboPFor's p4D1Dec128v32 function has built-in delta decoding:
///   out[i] = start + sum(stored[j] + 1) for j = 0..i
///
/// To make our interface match other codecs (where encode/decode preserve delta values),
/// we use the following approach:
///   - encode(): Store delta values directly using p4Enc128v32
///   - decode(): Use p4D1Dec128v32 with start=0, then convert prefix sums back to deltas
///
/// This way, encode(deltas) -> decode() -> deltas, matching other codecs.
///
/// Format: [4-byte compressed_length][4-byte count][compressed_data]
/// - compressed_length: size of compressed data in bytes (excluding header)
/// - count: number of uint32_t integers encoded
/// - compressed_data: TurboPFor compressed stream
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

    /// Encodes delta values from `in` to compressed bytes in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
    ///
    /// The input `in` contains delta-encoded values. We store them directly
    /// using p4Enc128v32. During decode, we'll convert the p4D1Dec output
    /// back to delta values for consistency with other codecs.
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

        /// TurboPFor may read beyond the actual data size for SIMD alignment.
        /// We need to ensure the input buffer is large enough.
        /// Create a padded copy to be safe.
        const size_t n = in.size();
        const size_t padded_size = ((n + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        std::vector<uint32_t> padded_input(padded_size, 0);
        std::memcpy(padded_input.data(), in.data(), n * sizeof(uint32_t));

        /// Encode using TurboPFor's SIMD-optimized 128-element block encoder
        unsigned char * data_start = reinterpret_cast<unsigned char *>(out.data() + header_size);
        unsigned char * data_end = turbopfor::p4Enc128v32(
            padded_input.data(),
            static_cast<unsigned>(n),
            data_start
        );

        size_t compressed_bytes = static_cast<size_t>(data_end - data_start);

        /// Write header
        uint32_t length_prefix = static_cast<uint32_t>(compressed_bytes);
        uint32_t count = static_cast<uint32_t>(n);
        std::memcpy(out.data(), &length_prefix, sizeof(length_prefix));
        std::memcpy(out.data() + sizeof(uint32_t), &count, sizeof(count));

        size_t total_written = header_size + compressed_bytes;

        /// Advance spans
        in = in.subspan(in.size());
        out = out.subspan(total_written);

        return total_written;
    }

    /// Decodes compressed bytes from `in` to delta values in `out`.
    /// Advances both `in` and `out` spans past the consumed/written data.
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

        /// TurboPFor may write beyond the actual data size for SIMD alignment.
        /// Allocate padded buffer for decoding.
        const size_t padded_size = ((count + BLOCK_SIZE - 1) / BLOCK_SIZE) * BLOCK_SIZE;
        std::vector<uint32_t> prefix_sums(padded_size, 0);

        /// Decode using TurboPFor's SIMD-optimized decoder with delta1
        /// This produces prefix sums: out[i] = sum(stored[j]+1) for j=0..i
        unsigned char * data_start = const_cast<unsigned char *>(
            reinterpret_cast<const unsigned char *>(in.data() + header_size));

        turbopfor::p4D1Dec128v32(
            data_start,
            static_cast<unsigned>(count),
            prefix_sums.data(),
            0  // start value
        );

        /// Convert prefix sums back to delta values
        /// prefix_sums[i] = sum(delta[0..i]) + (i+1)
        /// delta[0] = prefix_sums[0] - 1
        /// delta[i] = prefix_sums[i] - prefix_sums[i-1] - 1  for i > 0
        out[0] = prefix_sums[0] - 1;
        for (size_t i = 1; i < count; ++i)
            out[i] = prefix_sums[i] - prefix_sums[i - 1] - 1;

        /// Advance spans
        in = in.subspan(total_block_size);
        out = out.subspan(count);

        return total_block_size;
    }

    /// Restore absolute row IDs from delta-encoded values using inclusive scan.
    ///
    /// This matches the interface of BitpackingBlockCodec and FastPForBlockCodec:
    ///   absolute[i] = prev_value + sum(delta[0..i])
    ///
    /// @param data       The delta-encoded values to restore in-place
    /// @param prev_value The previous row ID (used as initial prefix sum value)
    /// @return           The last restored value (new prev_value for next block)
    static uint32_t restoreDelta(std::vector<uint32_t> & data, uint32_t prev_value)
    {
        std::inclusive_scan(data.begin(), data.end(), data.begin(), std::plus<uint32_t>{}, prev_value);
        return data.empty() ? prev_value : data.back();
    }
};

}

/// TurboPFor: Patched Frame-of-Reference with SIMD acceleration (SSE4.2/AVX2).
/// High compression ratio with very fast decode speed.
/// Optimized for sorted integer sequences like posting lists.
/// Uses 128-element SIMD blocks for optimal performance.
///
/// Interface semantics match BitpackingBlockCodec and FastPForBlockCodec:
///   - encode(deltas) produces compressed bytes
///   - decode(bytes) produces deltas
///   - restoreDelta(deltas, prev) produces absolute values
using TurboPForBlockCodec = impl::TurboPForBlockCodecImpl;

}

#endif // USE_TURBOPFOR
