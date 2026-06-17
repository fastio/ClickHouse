#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace knowhere
{

struct fp16
{
    uint16_t value = 0;

    fp16() = default;
    fp16(float v) : value(floatToHalf(v)) {}

    operator float() const { return halfToFloat(value); }

private:
    static uint16_t floatToHalf(float v)
    {
        uint32_t bits = 0;
        std::memcpy(&bits, &v, sizeof(bits));
        const uint32_t sign = (bits >> 16) & 0x8000U;
        int32_t exponent = static_cast<int32_t>((bits >> 23) & 0xffU) - 127 + 15;
        uint32_t mantissa = bits & 0x7fffffU;
        if (exponent <= 0)
            return static_cast<uint16_t>(sign);
        if (exponent >= 31)
            return static_cast<uint16_t>(sign | 0x7c00U);
        return static_cast<uint16_t>(sign | (static_cast<uint32_t>(exponent) << 10) | (mantissa >> 13));
    }

    static float halfToFloat(uint16_t h)
    {
        const uint32_t sign = static_cast<uint32_t>(h & 0x8000U) << 16;
        uint32_t exponent = (h >> 10) & 0x1fU;
        uint32_t mantissa = h & 0x03ffU;
        uint32_t bits = 0;
        if (exponent == 0)
        {
            bits = sign;
        }
        else if (exponent == 31)
        {
            bits = sign | 0x7f800000U | (mantissa << 13);
        }
        else
        {
            exponent = exponent - 15 + 127;
            bits = sign | (exponent << 23) | (mantissa << 13);
        }
        float out = 0;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }
};

struct bf16
{
    uint16_t value = 0;

    bf16() = default;
    bf16(float v)
    {
        uint32_t bits = 0;
        std::memcpy(&bits, &v, sizeof(bits));
        value = static_cast<uint16_t>(bits >> 16);
    }

    operator float() const
    {
        uint32_t bits = static_cast<uint32_t>(value) << 16;
        float out = 0;
        std::memcpy(&out, &bits, sizeof(out));
        return out;
    }
};

template <typename T>
struct KnowhereFloatTypeCheck : std::bool_constant<std::is_same_v<T, float> || std::is_same_v<T, fp16> || std::is_same_v<T, bf16>>
{
};

}
