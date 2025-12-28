#pragma once
#if defined(__x86_64__) || defined(_M_X64)
#if defined(__SSE4_1__)
   #define USE_SIMDCOMP 1
#endif
#endif

extern "C"
{
#if defined(USE_SIMDCOMP)
#include <simdcomp.h>
#endif
}

namespace DB
{

/// If the target platform is __x86_64__ or _M_X64, and only the SSE4.1 instruction set is available,
/// we use simdcomp to encode/decode unsigned integer arrays.
/// In all other cases, we fall back to the regular (non-SIMD) implementation in the follow anonymous namespace.
#if !defined(USE_SIMDCOMP)
namespace
{
    ALWAYS_INLINE static uint32_t maxBits(const uint32_t * data, size_t size)
    {
        uint32_t acc = 0;
        for (size_t i = 0; i < size; ++i)
            acc |= data[i];
        return 32u - static_cast<uint32_t>(__builtin_clz(acc));
    }

    // bw=1: 8 values per byte => 128/8 = 16 bytes
    ALWAYS_INLINE static uint32_t encodeB1u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t b = 0; b < 16; ++b)
        {
            const uint32_t * q = in + b * 8;
            out[b] = static_cast<unsigned char>(
                ((q[0] & 1u) << 0) |
                ((q[1] & 1u) << 1) |
                ((q[2] & 1u) << 2) |
                ((q[3] & 1u) << 3) |
                ((q[4] & 1u) << 4) |
                ((q[5] & 1u) << 5) |
                ((q[6] & 1u) << 6) |
                ((q[7] & 1u) << 7));
        }
        out += 16;
        return 16;
    }
    ALWAYS_INLINE static uint32_t decodeB1u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t b = 0; b < 16; ++b)
        {
            unsigned char x = *(in++);
            uint32_t base = b * 8;
            out[base + 0] = (x >> 0) & 1u;
            out[base + 1] = (x >> 1) & 1u;
            out[base + 2] = (x >> 2) & 1u;
            out[base + 3] = (x >> 3) & 1u;
            out[base + 4] = (x >> 4) & 1u;
            out[base + 5] = (x >> 5) & 1u;
            out[base + 6] = (x >> 6) & 1u;
            out[base + 7] = (x >> 7) & 1u;
        }
        return 16;
    }

    // bw=2: 4 values per byte => 128/4 = 32 bytes
    ALWAYS_INLINE static uint32_t encodeB2u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t b = 0; b < 32; ++b)
        {
            const uint32_t * q = in + b * 4;
            out[b] = static_cast<unsigned char>(
                ((q[0] & 3u) << 0) |
                ((q[1] & 3u) << 2) |
                ((q[2] & 3u) << 4) |
                ((q[3] & 3u) << 6));
        }
        out += 32;
        return 32;
    }
    ALWAYS_INLINE static uint32_t decodeB2u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t b = 0; b < 32; ++b)
        {
            unsigned char x = *(in++);
            uint32_t base = b * 4;
            out[base + 0] = (x >> 0) & 3u;
            out[base + 1] = (x >> 2) & 3u;
            out[base + 2] = (x >> 4) & 3u;
            out[base + 3] = (x >> 6) & 3u;
        }
        return 32;
    }

    // bw=4: 2 values per byte => 64 bytes
    ALWAYS_INLINE static uint32_t encodeB4u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t i = 0; i < 64; ++i)
        {
            uint32_t a = in[i * 2 + 0] & 0xFu;
            uint32_t b = in[i * 2 + 1] & 0xFu;
            out[i] = static_cast<unsigned char>(a | (b << 4));
        }
        out += 64;
        return 64;
    }
    ALWAYS_INLINE static uint32_t decodeB4u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t i = 0; i < 64; ++i)
        {
            unsigned char x = *(in++);
            out[i * 2 + 0] = (x >> 0) & 0xFu;
            out[i * 2 + 1] = (x >> 4) & 0xFu;
        }
        return 64;
    }

    // bw=8: 1 value per byte => 128 bytes
    ALWAYS_INLINE static uint32_t encodeB8u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t i = 0; i < 128; ++i)
            out[i] = static_cast<unsigned char>(in[i] & 0xFFu);
        out += 128;
        return 128;
    }
    ALWAYS_INLINE static uint32_t decodeB8u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t i = 0; i < 128; ++i)
            out[i] = static_cast<uint32_t>(*(in++));
        return 128;
    }

    // bw=16: 2 bytes per value => 256 bytes (little-endian per value)
    ALWAYS_INLINE static uint32_t encodeB16u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t i = 0; i < 128; ++i)
        {
            uint32_t v = in[i] & 0xFFFFu;
            out[i * 2 + 0] = static_cast<unsigned char>(v);
            out[i * 2 + 1] = static_cast<unsigned char>(v >> 8);
        }
        out += 256;
        return 256;
    }
    ALWAYS_INLINE static uint32_t decodeB16u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t i = 0; i < 128; ++i)
        {
            uint32_t b0 = in[i * 2 + 0];
            uint32_t b1 = in[i * 2 + 1];
            out[i] = b0 | (b1 << 8);
        }
        in += 256;
        return 256;
    }

    // bw=24: 3 bytes per value => 384 bytes (little-endian per value)
    ALWAYS_INLINE static uint32_t encodeB24u128(const uint32_t * in, unsigned char * & out)
    {
        for (uint32_t i = 0; i < 128; ++i)
        {
            uint32_t v = in[i] & 0x00FF'FFFFu;
            out[i * 3 + 0] = static_cast<unsigned char>(v);
            out[i * 3 + 1] = static_cast<unsigned char>(v >> 8);
            out[i * 3 + 2] = static_cast<unsigned char>(v >> 16);
        }
        out += 384;
        return 384;
    }
    ALWAYS_INLINE static uint32_t decodeB24u128(unsigned char * & in, uint32_t * out)
    {
        for (uint32_t i = 0; i < 128; ++i)
        {
            uint32_t b0 = in[i * 3 + 0];
            uint32_t b1 = in[i * 3 + 1];
            uint32_t b2 = in[i * 3 + 2];
            out[i] = b0 | (b1 << 8) | (b2 << 16);
        }
        in += 384;
        return 384;
    }

    // bw=32: 4 bytes per value => 512 bytes
    ALWAYS_INLINE static uint32_t encodeB32u128(const uint32_t * in, unsigned char * & out)
    {
        __builtin_memcpy(out, in, 128u * sizeof(uint32_t));
        out += 512;
        return 512;
    }
    ALWAYS_INLINE static uint32_t decodeB32u128(unsigned char * & in, uint32_t * out)
    {
        __builtin_memcpy(out, in, 128u * sizeof(uint32_t));
        in += 512;
        return 512;
    }

    ALWAYS_INLINE static uint32_t mask(uint32_t bits)
    {
        if (bits >= 32) return 0xFFFF'FFFFu;
        return (1u << bits) - 1u;
    }

    ALWAYS_INLINE static uint32_t encodeGeneric(const uint32_t * in, uint32_t bits, unsigned char * & out)
    {
        const uint32_t m = mask(bits);

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t out_pos = 0;

        for (uint32_t i = 0; i < 128; ++i)
        {
            const uint32_t v = in[i] & m;
            acc |= (static_cast<uint64_t>(v) << acc_bits);
            acc_bits += bits;

            while (acc_bits >= 8)
            {
                out[out_pos++] = static_cast<unsigned char>(acc & 0xFFu);
                acc >>= 8;
                acc_bits -= 8;
            }
        }

        if (acc_bits > 0)
            out[out_pos++] = static_cast<unsigned char>(acc & 0xFFu);

        out += out_pos;
        return out_pos;
    }
    ALWAYS_INLINE static uint32_t decodeGeneric(unsigned char * & in, uint32_t bits, uint32_t * out)
    {
        const uint32_t need = static_cast<uint32_t>((128u * static_cast<uint64_t>(bits) + 7u) / 8u);
        const uint32_t mask = bits == 32 ? 0xFFFF'FFFFu : (1u << bits) - 1u;

        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t in_pos = 0;

        for (uint32_t i = 0; i < 128; ++i)
        {
            while (acc_bits < bits)
            {
                acc |= (static_cast<uint64_t>(in[in_pos++]) << acc_bits);
                acc_bits += 8;
            }

            out[i] = static_cast<uint32_t>(acc) & mask;
            acc >>= bits;
            acc_bits -= bits;
        }
        in += need;
        return need;
    }

    ALWAYS_INLINE static uint32_t encodeVariadic(const uint32_t * in, size_t n, uint32_t bits, unsigned char * & out)
    {
        chassert(n < 128);
        if (bits == 0) return 0;

        const uint32_t mask = (1u << bits) - 1u;
        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t out_pos = 0;

        for (size_t i = 0; i < n; ++i)
        {
            uint32_t v = in[i] & mask;
            acc |= (static_cast<uint64_t>(v) << acc_bits);
            acc_bits += bits;

            while (acc_bits >= 8)
            {
                out[out_pos++] = static_cast<uint8_t>(acc & 0xFFu);
                acc >>= 8;
                acc_bits -= 8;
            }
        }

        if (acc_bits > 0)
            out[out_pos++] = static_cast<uint8_t>(acc & 0xFFu);
        out += out_pos;
        return out_pos;
    }

    ALWAYS_INLINE static uint32_t decodeVariadic(uint8_t * & in, size_t n, uint32_t bits, uint32_t * out)
    {
        if (bits == 0)
            return 0;
        const uint32_t need = static_cast<uint32_t>((n * static_cast<size_t>(bits) + 7) / 8);
        const uint32_t mask = (1u << bits) - 1u;
        uint64_t acc = 0;
        uint32_t acc_bits = 0;
        uint32_t in_pos = 0;

        for (size_t i = 0; i < n; ++i)
        {
            while (acc_bits < bits)
            {
                acc |= (static_cast<uint64_t>(in[in_pos++]) << acc_bits);
                acc_bits += 8;
            }
            out[i] = static_cast<uint32_t>(acc) & mask;
            acc >>= bits;
            acc_bits -= bits;
        }
        in += need;
        return need;
    }

    ALWAYS_INLINE static uint32_t encode128(const uint32_t * in, [[maybe_unused]] std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char * & out)
    {
        if (n != 128)
            return encodeVariadic(in, n, bits, out);
        switch (bits)
        {
            case 1:
                return encodeB1u128(in, out);
            case 2:
                return encodeB2u128(in, out);
            case 4:
                return encodeB4u128(in, out);
            case 8:
                return encodeB8u128(in, out);
            case 16:
                return encodeB16u128(in, out);
            case 24:
                return encodeB24u128(in, out);
            case 32:
                return encodeB32u128(in, out);
            default:
                return encodeGeneric(in, bits, out);
        }
    }

    ALWAYS_INLINE static uint32_t decode128(unsigned char * & in, [[maybe_unused]] std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t * out)
    {
        if (n != 128)
            return decodeVariadic(in, n, bits, out);
        switch (bits)
        {
            case 1:
                return decodeB1u128(in, out);
            case 2:
                return decodeB2u128(in, out);
            case 4:
                return decodeB4u128(in, out);
            case 8:
                return decodeB8u128(in, out);
            case 16:
                return decodeB16u128(in, out);
            case 24:
                return decodeB24u128(in, out);
            case 32:
                return decodeB32u128(in, out);
            default:
                return decodeGeneric(in, bits, out);
        }
    }
}
#endif

/// Block codec used by PostingsContainerImpl to compress/decompress arrays of
/// unsigned integers (typically delta/gap values).
struct BlockCodec
{
    static constexpr size_t kBlockSize =  128;

    /// Returns {compressed_bytes, bits} where bits is the max bit-width required
    /// to represent all values in [0..n).
    ALWAYS_INLINE static std::pair<size_t, size_t> evaluateSizeAndMaxBits([[maybe_unused]] const uint32_t * data, size_t n)
    {
#if defined(USE_SIMDCOMP)
        auto bits = maxbits_length(data, n);
        auto bytes = simdpack_compressedbytes(n, bits);
        return {bytes, bits};
#else
        auto bits = maxBits(data, n);
        auto bytes = static_cast<uint32_t>((n * static_cast<size_t>(bits) + 7) / 8);
        return {bytes, bits};
#endif
    }

    ALWAYS_INLINE static uint32_t encode(uint32_t * in, std::size_t n, [[maybe_unused]] uint32_t bits, unsigned char * & out)
    {
#if defined(USE_SIMDCOMP)
        /// simdcomp expects __m128i* output pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128_out = reinterpret_cast<__m128i*>(out);
        auto * m128_out_end = simdpack_length(in, n, m128_out, bits);

        auto used = static_cast<size_t>(m128_out_end - m128_out) * sizeof(__m128i);
        out += used;
        return used;
#else
        if (n != kBlockSize)
            return encodeVariadic(in, n, bits, out);
        return encode128(in, n, bits, out);
#endif
    }

    ALWAYS_INLINE static std::size_t decode(unsigned char * & in, std::size_t n, [[maybe_unused]] uint32_t bits, uint32_t * out)
    {
#if defined(USE_SIMDCOMP)
        /// simdcomp expects __m128i* input pointer; we compute consumed bytes
        /// from the returned end pointer (in units of 16-byte vectors).
        auto * m128i_p = reinterpret_cast<__m128i*>(in);
        auto * m128i_p_end = simdunpack_length(m128i_p, n, out, bits);
        auto used = static_cast<size_t>(m128i_p_end - m128i_p) * sizeof(__m128);
        in += used;
        return used;
#else
        /// Fallback: optimized path for full 128-value blocks, generic otherwise.
        if (n != kBlockSize)
            return decodeVariadic(in, n, bits, out);
        return decode128(in, n, bits, out);
#endif
    }
};

}
