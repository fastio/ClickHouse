#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <queue>
#include <utility>
#include <vector>

#include "knowhere/operands.h"
#include "knowhere/thread_pool.h"

namespace knowhere
{

struct DistId
{
    int64_t id = 0;
    float dist = 0;

    DistId() = default;
    DistId(int64_t id_, float dist_) : id(id_), dist(dist_) {}
};

class BitsetView
{
public:
    BitsetView() = default;
    BitsetView(std::nullptr_t) {}
    bool empty() const { return true; }
    bool test(size_t) const { return false; }
    size_t count() const { return 0; }
    size_t size() const { return 0; }
};

template <typename DistT, typename IdT>
class ResultMaxHeap
{
public:
    explicit ResultMaxHeap(size_t limit_) : limit(limit_) {}

    void Push(DistT dist, IdT id)
    {
        if (limit == 0)
            return;
        heap.emplace(dist, id);
        if (heap.size() > limit)
            heap.pop();
    }

    std::optional<std::pair<DistT, IdT>> Pop()
    {
        if (heap.empty())
            return std::nullopt;
        auto top = heap.top();
        heap.pop();
        return top;
    }

    size_t Size() const { return heap.size(); }

private:
    using Entry = std::pair<DistT, IdT>;
    struct Compare
    {
        bool operator()(const Entry & lhs, const Entry & rhs) const
        {
            return lhs.first < rhs.first;
        }
    };

    size_t limit = 0;
    std::priority_queue<Entry, std::vector<Entry>, Compare> heap;
};

inline uint64_t hash_vec(const float *, size_t)
{
    return 0;
}

namespace faiss_compat
{
template <typename T>
inline float toFloat(T value)
{
    return static_cast<float>(value);
}
}

}

namespace faiss::cppcontrib::knowhere
{

inline float fvec_L2sqr(const float * x, const float * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const float diff = x[i] - y[i];
        result += diff * diff;
    }
    return result;
}

inline float fvec_inner_product(const float * x, const float * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
        result += x[i] * y[i];
    return result;
}

inline float fvec_norm_L2sqr(const float * x, size_t size)
{
    return fvec_inner_product(x, x, size);
}

inline float fp16_vec_L2sqr(const ::knowhere::fp16 * x, const ::knowhere::fp16 * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const float diff = static_cast<float>(x[i]) - static_cast<float>(y[i]);
        result += diff * diff;
    }
    return result;
}

inline float fp16_vec_inner_product(const ::knowhere::fp16 * x, const ::knowhere::fp16 * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
        result += static_cast<float>(x[i]) * static_cast<float>(y[i]);
    return result;
}

inline float fp16_vec_norm_L2sqr(const ::knowhere::fp16 * x, size_t size)
{
    return fp16_vec_inner_product(x, x, size);
}

inline float bf16_vec_L2sqr(const ::knowhere::bf16 * x, const ::knowhere::bf16 * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
    {
        const float diff = static_cast<float>(x[i]) - static_cast<float>(y[i]);
        result += diff * diff;
    }
    return result;
}

inline float bf16_vec_inner_product(const ::knowhere::bf16 * x, const ::knowhere::bf16 * y, size_t size)
{
    float result = 0;
    for (size_t i = 0; i < size; ++i)
        result += static_cast<float>(x[i]) * static_cast<float>(y[i]);
    return result;
}

inline float bf16_vec_norm_L2sqr(const ::knowhere::bf16 * x, size_t size)
{
    return bf16_vec_inner_product(x, x, size);
}

}
