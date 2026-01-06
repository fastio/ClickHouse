#pragma once
#pragma clang optimize off
#include <Common/assert_cast.h>
#include <roaring/roaring.hh>
#include <boost/noncopyable.hpp>
#include <IO/WriteBufferFromString.h>
#include <Core/Field.h>
#include <Common/PODArray.h>   // PaddedPODArray
#include <bit>
#include <cstring>             // std::memcpy
#include <algorithm>
#include <bit>
#include <cassert>
#include <cstdint>
#include <span>
#include <utility>
#include <vector>


namespace DB
{
    class PostingSet {
    public:
        using uint32_t = uint32_t;
        enum class Rep : uint8_t { Sparse, Dense };

        explicit PostingSet(uint32_t universe = 0, Rep preferred = Rep::Sparse)
            : universe_(universe) {
            if (preferred == Rep::Dense) ensure_dense_();
        }

        uint32_t universe() const noexcept { return universe_; }
        Rep rep() const noexcept { return rep_; }

        // 单点插入：docid 必须 < universe
        inline void insert(uint32_t id) {
            assert(id < universe_);
            if (rep_ == Rep::Dense) {
                dense_set_(id);
                return;
            }
            if (normalized_ && !sparse_.empty() && id > sparse_.back()) {
                sparse_.push_back(id);
                ++cardinality_;
            } else {
                sparse_.push_back(id);
                normalized_ = false;
                cardinality_valid_ = false;
            }
            maybe_densify_();
        }

        // 批量插入：assume_sorted_unique=true 时 ids 必须严格递增且无重复
        void insert_batch(std::span<const uint32_t> ids, bool assume_sorted_unique = false) {
            if (ids.empty()) return;
            assert(ids.back() < universe_);

            if (rep_ == Rep::Dense) {
                for (uint32_t id : ids) dense_set_(id);
                return;
            }

            if (assume_sorted_unique && normalized_) {
                merge_union_sorted_unique_(ids);
                maybe_densify_();
                return;
            }

            sparse_.insert(sparse_.end(), ids.begin(), ids.end());
            normalized_ = false;
            cardinality_valid_ = false;
            maybe_densify_();
        }

        // ===== 集合运算：不要求 universe 相等 =====
        friend PostingSet Or(const PostingSet& a, const PostingSet& b) {
            const uint32_t out_univ = (a.universe_ > b.universe_) ? a.universe_ : b.universe_;
            PostingSet out(out_univ);
            out.or_from_(a, b);
            return out;
        }

        friend PostingSet And(const PostingSet& a, const PostingSet& b) {
            const uint32_t out_univ = (a.universe_ < b.universe_) ? a.universe_ : b.universe_;
            PostingSet out(out_univ);
            out.and_from_(a, b);
            return out;
        }

        void or_with(const PostingSet& other) { *this = Or(*this, other); }
        void and_with(const PostingSet& other) { *this = And(*this, other); }

        // 可选：取有序稀疏列表（必要时 decode/normalize）
        const std::vector<uint32_t>& materialize_sparse_sorted() {
            if (rep_ == Rep::Dense) {
                sparse_.clear();
                sparse_.reserve(popcount_);
                for (uint32_t w = 0; w < dense_.size(); ++w) {
                    uint64_t x = dense_[w];
                    while (x) {
                        uint32_t t = static_cast<uint32_t>(std::countr_zero(x));
                        uint32_t id = (w << 6) + t;
                        if (id < universe_) sparse_.push_back(id);
                        x &= (x - 1);
                    }
                }
                rep_ = Rep::Sparse;
                normalized_ = true;
                cardinality_ = static_cast<uint32_t>(sparse_.size());
                cardinality_valid_ = true;
            } else {
                normalize_();
            }
            return sparse_;
        }

        uint32_t cardinality() {
            if (rep_ == Rep::Dense) return popcount_;
            if (!cardinality_valid_) {
                normalize_();
                cardinality_ = static_cast<uint32_t>(sparse_.size());
                cardinality_valid_ = true;
            }
            return cardinality_;
        }

        size_t getSizeInBytes() const noexcept {
            size_t bytes = sizeof(*this);

            // vector 本体已经包含在 sizeof(*this) 里，这里只加它们的动态数组部分
            if (rep_ == Rep::Sparse) {
                bytes += sparse_.size() * sizeof(uint32_t);
                // Dense vector 可能为空，也可能保留了 capacity（我们这里按 size 统计）
                bytes += dense_.size() * sizeof(uint64_t);
            } else { // Dense
                bytes += dense_.size() * sizeof(uint64_t);
                bytes += sparse_.size() * sizeof(uint32_t);
            }
            return bytes;
        }
        void addRange(size_t offset, size_t size) {
    clear_();
    if (size == 0) return;

    // uint32_t 是 uint32_t，做溢出/范围检查
    constexpr uint64_t kMaxDocPlus1 = uint64_t(std::numeric_limits<uint32_t>::max()) + 1ULL;

    const uint64_t start = static_cast<uint64_t>(offset);
    const uint64_t len   = static_cast<uint64_t>(size);

    // offset/size 超过 uint32_t 可表示范围时直接 assert（你也可改成抛异常）
    assert(start < kMaxDocPlus1);
    assert(len   <= kMaxDocPlus1);
    assert(start + len <= kMaxDocPlus1);

    const uint64_t end = start + len; // end exclusive

    // "初始化"语义：universe 至少覆盖 end
    const uint32_t need_universe = static_cast<uint32_t>(end);
    if (need_universe > universe_) universe_ = need_universe;

    // 决策：是否用 Dense（你也可以按业务调阈值）
    const uint32_t range_size = static_cast<uint32_t>(len);
    const bool use_dense =
        (universe_ != 0) &&
        (range_size >= kDensifyAbs || uint64_t(range_size) * kDensifyDiv >= universe_);

    if (!use_dense) {
        // Sparse：直接生成连续递增数组
        rep_ = Rep::Sparse;
        normalized_ = true;
        cardinality_valid_ = true;
        cardinality_ = range_size;

        sparse_.resize(range_size);
        uint32_t v = static_cast<uint32_t>(start);
        for (uint32_t i = 0; i < range_size; ++i, ++v) {
            sparse_[i] = v;
        }
        return;
    }

    // Dense：按 word 批量置位（避免逐个 set）
    rep_ = Rep::Dense;
    dense_.assign(words_for_(universe_), 0ULL);

    const uint64_t first = start;
    const uint64_t last_excl = end;

    const uint32_t first_word = static_cast<uint32_t>(first >> 6);
    const uint32_t last_word  = static_cast<uint32_t>((last_excl - 1) >> 6);

    const uint32_t first_bit  = static_cast<uint32_t>(first & 63ULL);
    const uint32_t last_bit   = static_cast<uint32_t>((last_excl - 1) & 63ULL);

    if (first_word == last_word) {
        // 同一个 word 内
        const uint64_t mask =
            ((~0ULL) << first_bit) &
            ((last_bit == 63) ? ~0ULL : ((1ULL << (last_bit + 1)) - 1ULL));
        dense_[first_word] |= mask;
    } else {
        // 首 word：从 first_bit 到 63
        dense_[first_word] |= (~0ULL) << first_bit;

        // 中间完整 words：全 1
        for (uint32_t w = first_word + 1; w < last_word; ++w) {
            dense_[w] = ~0ULL;
        }

        // 末 word：从 0 到 last_bit
        const uint64_t tail_mask = (last_bit == 63) ? ~0ULL : ((1ULL << (last_bit + 1)) - 1ULL);
        dense_[last_word] |= tail_mask;
    }

    // 保证最后一个 word 的高位（超过 universe）为 0
    const uint32_t rem = universe_ & 63u;
    if (rem != 0) {
        dense_.back() &= (1ULL << rem) - 1ULL;
    }

    popcount_ = range_size;
    cardinality_ = popcount_;
    cardinality_valid_ = true;
    normalized_ = true;
}
        void toUint32Array(DB::PaddedPODArray<UInt32> & indices) const
        {
            indices.clear();

            if (rep_ == Rep::Sparse)
            {
                // 确保 sparse_ 有序去重
                normalize_const_();

                const size_t n = sparse_.size();
                indices.resize(n);
                if (n)
                    std::memcpy(indices.data(), sparse_.data(), n * sizeof(UInt32));
                return;
            }

            // Dense：按 word 枚举 set bits（升序），避免逐位扫描
            indices.reserve(popcount_);

            const uint32_t words = static_cast<uint32_t>(dense_.size());
            for (uint32_t w = 0; w < words; ++w)
            {
                uint64_t x = dense_[w];
                while (x)
                {
                    const uint32_t t = static_cast<uint32_t>(std::countr_zero(x));
                    const uint32_t id = (w << 6) + t;

                    // universe_ 可能不是 64 对齐，最后一字的 padding 位要过滤
                    if (id < universe_)
                        indices.push_back(static_cast<UInt32>(id));

                    x &= (x - 1); // 清掉最低位的 1
                }
            }
        }
        void toUint32Array(std::vector<uint32_t>& out) const {
            out.clear();

            if (rep_ == Rep::Sparse) {
                // Sparse：确保有序去重，然后直接拷贝
                normalize_const_();
                out.insert(out.end(), sparse_.begin(), sparse_.end());
                return;
            }

            // Dense：按 word 枚举 set bits（升序）
            out.reserve(popcount_); // popcount_ 在 Dense 下应当是准确的

            const uint32_t words = static_cast<uint32_t>(dense_.size());
            for (uint32_t w = 0; w < words; ++w) {
                uint64_t x = dense_[w];
                while (x) {
                    // 找到最低位的 1
                    uint32_t t = static_cast<uint32_t>(std::countr_zero(x));
                    uint32_t id = (w << 6) + t;
                    if (id < universe_) out.push_back(id); // 最后一个 word 可能有 padding bits
                    x &= (x - 1); // 清掉最低位的 1
                }
            }
        }
    private:
        void clear_() noexcept {
            sparse_.clear();
            dense_.clear();
            popcount_ = 0;

            rep_ = Rep::Sparse;
            normalized_ = true;
            cardinality_valid_ = true;
            cardinality_ = 0;
        }
        // ======= 状态 =======
        uint32_t universe_{0};
        Rep rep_{Rep::Sparse};

        // Sparse
        std::vector<uint32_t> sparse_;
        bool normalized_{true};
        bool cardinality_valid_{true};
        uint32_t cardinality_{0};

        // Dense
        std::vector<uint64_t> dense_;
        uint32_t popcount_{0};

        // ======= densify 策略（可调） =======
        static constexpr uint32_t kDensifyDiv = 32;   // 密度 > 1/32
        static constexpr uint32_t kDensifyAbs = 8192; // 或者绝对数量

        inline void maybe_densify_() {
            if (rep_ != Rep::Sparse) return;
            if (universe_ == 0) return;
            const uint32_t approx = static_cast<uint32_t>(sparse_.size()); // 允许重复（保守）
            if (approx >= kDensifyAbs || approx * kDensifyDiv >= universe_) {
                to_dense_();
            }
        }

        inline void ensure_dense_() {
            if (rep_ == Rep::Dense) return;
            dense_.assign(words_for_(universe_), 0ULL);
            popcount_ = 0;
            rep_ = Rep::Dense;
            sparse_.clear();
            sparse_.shrink_to_fit();
            normalized_ = true;
            cardinality_valid_ = true;
            cardinality_ = 0;
        }

        static inline uint32_t words_for_(uint32_t univ) {
            return (univ + 63u) >> 6;
        }

        void normalize_() {
            if (normalized_) return;
            std::sort(sparse_.begin(), sparse_.end());
            sparse_.erase(std::unique(sparse_.begin(), sparse_.end()), sparse_.end());
            normalized_ = true;
            cardinality_ = static_cast<uint32_t>(sparse_.size());
            cardinality_valid_ = true;
        }

        void to_dense_() {
            normalize_();
            dense_.assign(words_for_(universe_), 0ULL);
            popcount_ = 0;
            for (uint32_t id : sparse_) dense_set_(id);
            rep_ = Rep::Dense;
            sparse_.clear();
            sparse_.shrink_to_fit();
            cardinality_ = popcount_;
            cardinality_valid_ = true;
            normalized_ = true;
        }

        inline void dense_set_(uint32_t id) {
            const uint32_t word = id >> 6;
            const uint32_t bit  = id & 63u;
            const uint64_t mask = 1ULL << bit;
            uint64_t before = dense_[word];
            uint64_t after  = before | mask;
            dense_[word] = after;
            popcount_ += static_cast<uint32_t>((before ^ after) != 0ULL);
        }

        static inline bool dense_test_(const std::vector<uint64_t>& w, uint32_t id) {
            const uint32_t word = id >> 6;
            const uint32_t bit  = id & 63u;
            return (w[word] >> bit) & 1ULL;
        }

        // assume: sparse_ 已归一化且 ids 严格递增无重复
        void merge_union_sorted_unique_(std::span<const uint32_t> ids) {
            assert(normalized_);
            std::vector<uint32_t> out;
            out.reserve(sparse_.size() + ids.size());

            size_t i = 0, j = 0;
            while (i < sparse_.size() && j < ids.size()) {
                uint32_t a = sparse_[i], b = ids[j];
                if (a < b) { out.push_back(a); ++i; }
                else if (b < a) { out.push_back(b); ++j; }
                else { out.push_back(a); ++i; ++j; }
            }
            while (i < sparse_.size()) out.push_back(sparse_[i++]);
            while (j < ids.size()) out.push_back(ids[j++]);

            sparse_.swap(out);
            cardinality_ = static_cast<uint32_t>(sparse_.size());
            cardinality_valid_ = true;
            normalized_ = true;
        }

        // ======= OR 实现（支持不同 universe/word 长度） =======
        void or_from_(const PostingSet& a, const PostingSet& b) {
            // 若任一为 Dense，结果优先 Dense（对 OR 通常划算）
            if (a.rep_ == Rep::Dense || b.rep_ == Rep::Dense) {
                ensure_dense_(); // out.universe_ 已是 max

                // 把 a 合并进 dense_
                if (a.rep_ == Rep::Dense) {
                    or_words_maybe_diff_(dense_, a.dense_);
                } else {
                    a.normalize_const_();
                    for (uint32_t id : a.sparse_) dense_set_(id);
                }

                // 把 b 合并进 dense_
                if (b.rep_ == Rep::Dense) {
                    or_words_maybe_diff_(dense_, b.dense_);
                } else {
                    b.normalize_const_();
                    for (uint32_t id : b.sparse_) dense_set_(id);
                }

                popcount_ = popcount_words_(dense_, universe_);
                return;
            }

            // Sparse OR Sparse：merge union（不依赖 universe）
            a.normalize_const_();
            b.normalize_const_();

            rep_ = Rep::Sparse;
            normalized_ = true;
            sparse_.clear();
            sparse_.reserve(a.sparse_.size() + b.sparse_.size());

            size_t i = 0, j = 0;
            while (i < a.sparse_.size() && j < b.sparse_.size()) {
                uint32_t x = a.sparse_[i], y = b.sparse_[j];
                if (x < y) { sparse_.push_back(x); ++i; }
                else if (y < x) { sparse_.push_back(y); ++j; }
                else { sparse_.push_back(x); ++i; ++j; }
            }
            while (i < a.sparse_.size()) sparse_.push_back(a.sparse_[i++]);
            while (j < b.sparse_.size()) sparse_.push_back(b.sparse_[j++]);

            cardinality_ = static_cast<uint32_t>(sparse_.size());
            cardinality_valid_ = true;
            maybe_densify_();
        }

        // ======= AND 实现（支持不同 universe/word 长度） =======
        void and_from_(const PostingSet& a, const PostingSet& b) {
            // out.universe_ 已是 min

            // Dense & Dense：只算重叠部分（min words）
            if (a.rep_ == Rep::Dense && b.rep_ == Rep::Dense) {
                ensure_dense_(); // size = out.words(min universe)
                and_words_diff_into_(dense_, a.dense_, b.dense_);
                popcount_ = popcount_words_(dense_, universe_);
                return;
            }

            // Dense & Sparse：遍历 sparse，注意 id 可能 >= dense.universe_（需跳过）
            if (a.rep_ == Rep::Dense && b.rep_ == Rep::Sparse) {
                b.normalize_const_();
                rep_ = Rep::Sparse;
                normalized_ = true;
                sparse_.clear();
                sparse_.reserve(b.sparse_.size());
                for (uint32_t id : b.sparse_) {
                    if (id >= a.universe_) break; // b 是递增的；>=a.universe 不可能命中
                    if (dense_test_(a.dense_, id)) sparse_.push_back(id);
                }
                cardinality_ = static_cast<uint32_t>(sparse_.size());
                cardinality_valid_ = true;
                maybe_densify_();
                return;
            }
            if (a.rep_ == Rep::Sparse && b.rep_ == Rep::Dense) {
                a.normalize_const_();
                rep_ = Rep::Sparse;
                normalized_ = true;
                sparse_.clear();
                sparse_.reserve(a.sparse_.size());
                for (uint32_t id : a.sparse_) {
                    if (id >= b.universe_) break;
                    if (dense_test_(b.dense_, id)) sparse_.push_back(id);
                }
                cardinality_ = static_cast<uint32_t>(sparse_.size());
                cardinality_valid_ = true;
                maybe_densify_();
                return;
            }

            // Sparse & Sparse：双指针 intersection（但 AND 结果 universe=min，所以可提前停在 min）
            a.normalize_const_();
            b.normalize_const_();

            rep_ = Rep::Sparse;
            normalized_ = true;
            sparse_.clear();
            sparse_.reserve(std::min(a.sparse_.size(), b.sparse_.size()));

            const uint32_t limit = universe_; // min(a.universe_, b.universe_)
            size_t i = 0, j = 0;
            while (i < a.sparse_.size() && j < b.sparse_.size()) {
                uint32_t x = a.sparse_[i], y = b.sparse_[j];
                if (x >= limit || y >= limit) break;
                if (x < y) ++i;
                else if (y < x) ++j;
                else { sparse_.push_back(x); ++i; ++j; }
            }

            cardinality_ = static_cast<uint32_t>(sparse_.size());
            cardinality_valid_ = true;
            maybe_densify_();
        }

        // dst |= src，允许 src 更短/更长（按重叠部分 OR，剩余保持 dst 原样）
        static inline void or_words_maybe_diff_(std::vector<uint64_t>& dst,
                                               const std::vector<uint64_t>& src) {
            const size_t n = std::min(dst.size(), src.size());
            for (size_t i = 0; i < n; ++i) dst[i] |= src[i];
        }

        // dst = a & b（只取重叠的 min words，dst 由 out.universe_ 决定）
        static inline void and_words_diff_into_(std::vector<uint64_t>& dst,
                                               const std::vector<uint64_t>& a,
                                               const std::vector<uint64_t>& b) {
            const size_t n = dst.size();
            const size_t m = std::min(a.size(), b.size());
            const size_t k = std::min(n, m);
            // k 之前：a&b
            for (size_t i = 0; i < k; ++i) dst[i] = a[i] & b[i];
            // k..n：清零（因为 out.universe=min，不应出现多余位）
            for (size_t i = k; i < n; ++i) dst[i] = 0ULL;
        }

        static inline uint32_t popcount_words_(const std::vector<uint64_t>& words, uint32_t universe) {
            const size_t n = words.size();
            if (n == 0) return 0;

            uint32_t total = 0;
            for (size_t i = 0; i + 1 < n; ++i) total += static_cast<uint32_t>(std::popcount(words[i]));

            const uint32_t rem = universe & 63u;
            uint64_t last = words[n - 1];
            if (rem != 0) last &= ((1ULL << rem) - 1ULL);
            total += static_cast<uint32_t>(std::popcount(last));
            return total;
        }

        void normalize_const_() const {
            if (rep_ != Rep::Sparse) return;
            if (normalized_) return;
            auto* self = const_cast<PostingSet*>(this);
            self->normalize_();
        }
    };
    using PostingSetPtr = std::shared_ptr<PostingSet>;
}
