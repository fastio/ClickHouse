#pragma once

#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

#include <memory>
#include <string>


namespace ProfileEvents
{
    extern const Event AnnSearcherCacheMisses;
    extern const Event AnnSearcherCacheHits;
    extern const Event AnnSearcherCacheWeightLost;
}

namespace CurrentMetrics
{
    extern const Metric AnnSearcherCacheBytes;
    extern const Metric AnnSearcherCacheCells;
}

namespace DB
{

/// Identity of one open ANN searcher entry.
///
/// `path_to_data_part` is the on-disk reflection-ANN-index part path (the
/// `algorithm_private_*` folder under a materialized index part). It changes
/// whenever the part is rewritten (merge / mutation produce a new part name),
/// so it doubles as a part-content fingerprint without needing to read
/// `checksums.txt`.
///
/// `build_params_hash` distinguishes searchers built with different DDL-time
/// parameters that happen to land on the same path (defensive, normally not
/// needed). It comes from `IANNIndexAlgorithm::getBuildParamsHash`.
///
/// Search-time tunables are intentionally NOT part of this key — same part +
/// same build params yields one searcher process-wide regardless of how the
/// query is parameterised.
struct ANNSearcherCacheKey
{
    String path_to_data_part;
    String build_params_hash;

    bool operator==(const ANNSearcherCacheKey & rhs) const
    {
        return path_to_data_part == rhs.path_to_data_part && build_params_hash == rhs.build_params_hash;
    }
};

struct ANNSearcherCacheHashFunction
{
    size_t operator()(const ANNSearcherCacheKey & key) const
    {
        SipHash siphash;
        siphash.update(key.path_to_data_part);
        siphash.update(key.build_params_hash);
        return siphash.get64();
    }
};

/// Cell wraps an opaque, already-loaded searcher instance plus its measured
/// memory footprint. The cell is parameterised over `HandleT` so DiskANN and
/// SPANN can store their own searcher types without any common base class.
template <typename HandleT>
struct ANNSearcherCacheCell
{
    /// memoryUsageBytes() / folder-size estimates are approximate; pad by a
    /// small constant to account for cache bookkeeping itself. Mirrors the
    /// guess used by `VectorSimilarityIndexCacheCell`.
    static constexpr auto ENTRY_OVERHEAD_BYTES_GUESS = 256uz;

    std::shared_ptr<HandleT> handle;
    size_t memory_bytes;

    ANNSearcherCacheCell(std::shared_ptr<HandleT> handle_, size_t handle_memory_bytes)
        : handle(std::move(handle_))
        , memory_bytes(handle_memory_bytes + ENTRY_OVERHEAD_BYTES_GUESS)
    {
    }

    ANNSearcherCacheCell(const ANNSearcherCacheCell &) = delete;
    ANNSearcherCacheCell & operator=(const ANNSearcherCacheCell &) = delete;
};

template <typename HandleT>
struct ANNSearcherCacheWeightFunction
{
    size_t operator()(const ANNSearcherCacheCell<HandleT> & cell) const
    {
        return cell.memory_bytes;
    }
};

/// Reflection-instance-level cache of opened ANN searchers.
///
/// One cache lives on each `IANNIndexAlgorithm` instance (which itself lives
/// on the owning `ReflectionANNIndex`). DiskANN and SPANN parameterise the
/// template with their own searcher types so the LRU bookkeeping is shared
/// while the searcher payload remains algorithm-private.
///
/// Bytes accounting is approximate:
///  - DiskANN reports `DiskANNSearcherHandle::memoryUsage` from its FFI layer.
///  - SPANN approximates with `getSizeRecursively(folder) * 3 / 2`, since
///    SPTAG does not expose a memory-usage interface.
template <typename HandleT>
class ANNSearcherCache
    : public CacheBase<
          ANNSearcherCacheKey,
          ANNSearcherCacheCell<HandleT>,
          ANNSearcherCacheHashFunction,
          ANNSearcherCacheWeightFunction<HandleT>>
{
public:
    using Cell = ANNSearcherCacheCell<HandleT>;
    using Base = CacheBase<
        ANNSearcherCacheKey,
        Cell,
        ANNSearcherCacheHashFunction,
        ANNSearcherCacheWeightFunction<HandleT>>;
    using Key = typename Base::Key;
    using MappedPtr = typename Base::MappedPtr;

    ANNSearcherCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_count, double size_ratio)
        : Base(
              cache_policy,
              CurrentMetrics::AnnSearcherCacheBytes,
              CurrentMetrics::AnnSearcherCacheCells,
              max_size_in_bytes,
              max_count,
              size_ratio)
    {
    }

    /// LoadFunc has signature () -> std::pair<std::shared_ptr<HandleT>, size_t>
    /// where the second element is the measured memory footprint of the new
    /// handle (so the cache can charge it to its weight budget without making
    /// CacheBase aware of how that number was obtained).
    template <typename LoadFunc>
    std::shared_ptr<HandleT> getOrSet(const Key & key, LoadFunc && load)
    {
        auto wrapped_load = [&]() -> std::shared_ptr<Cell>
        {
            auto [handle, memory_bytes] = load();
            return std::make_shared<Cell>(std::move(handle), memory_bytes);
        };

        auto result = Base::getOrSet(key, wrapped_load);
        if (result.second)
            ProfileEvents::increment(ProfileEvents::AnnSearcherCacheMisses);
        else
            ProfileEvents::increment(ProfileEvents::AnnSearcherCacheHits);

        return result.first->handle;
    }

private:
    void onEntryRemoval(const size_t weight_loss, const MappedPtr & mapped_ptr) override
    {
        ProfileEvents::increment(ProfileEvents::AnnSearcherCacheWeightLost, weight_loss);
        UNUSED(mapped_ptr);
    }
};

}
