#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Core/UUID.h>
#include <Disks/DirectoryIterator.h>
#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <IO/WriteHelpers.h>

#include <algorithm>
#include <filesystem>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

namespace
{
    constexpr std::string_view GROUP_DIR_PREFIX = "group_";
    constexpr std::string_view TMP_GROUP_DIR_PREFIX = "tmp_group_";

    bool hasGroupDirPrefix(std::string_view name)
    {
        return name.starts_with(GROUP_DIR_PREFIX) || name.starts_with(TMP_GROUP_DIR_PREFIX);
    }

    LoggerPtr resolveLogger(const LoggerPtr & supplied)
    {
        return supplied ? supplied : getLogger("ANNIndexManager");
    }
}

ANNIndexManager::ANNIndexManager(Config config_)
    : config(std::move(config_))
{
    if (!config.volume)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager requires a non-null volume");
    if (config.shape.dim == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager requires `shape.dim > 0`");

    config.log = resolveLogger(config.log);

    /// Always start with a non-null empty snapshot so that readers never have to special-case
    /// `nullptr` on the hot path.
    std::atomic_store_explicit(
        &active,
        std::shared_ptr<const ANNActiveGroupsSnapshot>(std::make_shared<const ANNActiveGroupsSnapshot>()),
        std::memory_order_release);
}

DiskPtr ANNIndexManager::getDisk() const
{
    return config.volume->getDisk(0);
}

template <typename F>
void ANNIndexManager::publishWithLock(F && func)
{
    std::lock_guard lk(write_mtx);

    auto current = std::atomic_load_explicit(&active, std::memory_order_acquire);
    std::vector<ANNIndexGroupPtr> next_groups = current ? current->groups : std::vector<ANNIndexGroupPtr>{};

    /// Caller is free to mutate both the next active list and the retired map.
    func(next_groups, retired_group_meta);

    auto next_snap = std::make_shared<ANNActiveGroupsSnapshot>();
    next_snap->groups = std::move(next_groups);
    std::atomic_store_explicit(&active, std::shared_ptr<const ANNActiveGroupsSnapshot>(std::move(next_snap)), std::memory_order_release);
}

void ANNIndexManager::registerGroup(ANNIndexGroupPtr new_group)
{
    if (!new_group)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManager::registerGroup: null group");

    if (new_group->getShape() != config.shape)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ANNIndexManager::registerGroup: shape fingerprint does not match the table's");

    if (new_group->getHashSeed() != config.hash_seed)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ANNIndexManager::registerGroup: hash_seed does not match the table's");

    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> &)
    {
        next_groups.push_back(std::move(new_group));
    });
}

std::vector<std::string> ANNIndexManager::getRetiredGroupDirs() const
{
    std::lock_guard lk(write_mtx);
    std::vector<std::string> result;
    result.reserve(retired_group_meta.size());
    for (const auto & [name, _] : retired_group_meta)
        result.push_back(name);
    std::sort(result.begin(), result.end());
    return result;
}

void ANNIndexManager::invalidateAllGroupsForShapeChange()
{
    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> & retired)
    {
        const auto now = std::chrono::steady_clock::now();
        for (auto & grp : next_groups)
        {
            if (!grp)
                continue;
            std::string dir = grp->getGroupDir();
            retired[dir] = RetiredMeta{now, std::move(grp)};
        }
        next_groups.clear();
    });
}

void ANNIndexManager::invalidateGroupsForMutation(const std::vector<DataPartPtr> & affected_parts)
{
    if (affected_parts.empty())
        return;

    std::vector<AffectedRange> affected;
    affected.reserve(affected_parts.size());
    for (const auto & part : affected_parts)
    {
        if (!part)
            continue;
        affected.push_back(AffectedRange{
            hashPartitionId(part->info.getPartitionId()),
            static_cast<UInt64>(part->info.min_block),
            static_cast<UInt64>(part->info.max_block),
        });
    }

    invalidateGroupsForRanges(affected);
}

void ANNIndexManager::invalidateGroupsForRanges(const std::vector<AffectedRange> & affected)
{
    if (affected.empty())
        return;

    publishWithLock([&](std::vector<ANNIndexGroupPtr> & next_groups,
                         std::unordered_map<std::string, RetiredMeta> & retired)
    {
        const auto now = std::chrono::steady_clock::now();
        std::vector<ANNIndexGroupPtr> keep;
        keep.reserve(next_groups.size());

        for (auto & grp : next_groups)
        {
            if (!grp)
                continue;

            bool intersects = false;
            for (const auto & r : affected)
            {
                if (grp->containsPart(r.partition_hash, r.min_block, r.max_block))
                {
                    intersects = true;
                    break;
                }
            }

            if (intersects)
            {
                std::string dir = grp->getGroupDir();
                retired[dir] = RetiredMeta{now, std::move(grp)};
            }
            else
            {
                keep.push_back(std::move(grp));
            }
        }

        next_groups = std::move(keep);
    });
}

bool ANNIndexManager::isPartCovered(const DataPartPtr & part) const
{
    if (!part)
        return false;

    const UInt64 partition_hash = hashPartitionId(part->info.getPartitionId());
    const UInt64 min_block = static_cast<UInt64>(part->info.min_block);
    const UInt64 max_block = static_cast<UInt64>(part->info.max_block);

    return isRangeCovered(partition_hash, min_block, max_block);
}

bool ANNIndexManager::isRangeCovered(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const
{
    auto snap = std::atomic_load_explicit(&active, std::memory_order_acquire);
    if (!snap || snap->empty())
        return false;

    for (const auto & grp : snap->groups)
    {
        if (grp && grp->containsPart(partition_hash, min_block, max_block))
            return true;
    }
    return false;
}

UInt64 ANNIndexManager::hashPartitionId(const String & partition_id) const
{
    SipHash hasher(config.hash_seed, 0);
    hasher.update(partition_id.data(), partition_id.size());
    return hasher.get64();
}

std::vector<ANNSearchHit> ANNIndexManager::search(
    const float * query,
    size_t query_dim,
    size_t k,
    size_t rescoring_factor) const
{
    if (k == 0)
        return {};

    auto snap = std::atomic_load_explicit(&active, std::memory_order_acquire);
    if (!snap || snap->empty())
        return {};

    const size_t factor = std::max<size_t>(rescoring_factor, 1);
    const size_t target_k = k * factor;

    std::vector<ANNSearchHit> merged;
    merged.reserve(target_k * snap->groups.size());

    for (const auto & grp : snap->groups)
    {
        if (!grp)
            continue;

        const auto hits = grp->search(query, query_dim, target_k);
        for (const auto & h : hits)
            merged.push_back(ANNSearchHit{grp->lookup(h.internal_id), h.distance});
    }

    const size_t keep = std::min(target_k, merged.size());
    if (keep == 0)
        return {};

    std::partial_sort(
        merged.begin(),
        merged.begin() + keep,
        merged.end(),
        [](const ANNSearchHit & a, const ANNSearchHit & b) { return a.distance < b.distance; });

    merged.resize(keep);
    return merged;
}

ANNGroupStoragePtr ANNIndexManager::createTempGroupStorage(DiskTransactionPtr txn) const
{
    const auto uuid = UUIDHelpers::generateV4();
    const std::string dirname = std::string(TMP_GROUP_DIR_PREFIX) + toString(uuid);
    const std::string rel = (fs::path(config.relative_root_path) / dirname).string();
    return std::make_shared<ANNGroupStorageDiskFull>(config.volume, rel, std::move(txn));
}

ANNGroupStoragePtr ANNIndexManager::openGroupStorage(const std::string & group_dir) const
{
    const std::string rel = (fs::path(config.relative_root_path) / group_dir).string();
    return std::make_shared<ANNGroupStorageDiskFull>(config.volume, rel, /*txn=*/nullptr);
}

std::vector<std::string> ANNIndexManager::listGroupDirsOnDisk() const
{
    std::vector<std::string> result;

    auto disk = getDisk();
    if (!disk->existsDirectory(config.relative_root_path))
        return result;

    for (auto it = disk->iterateDirectory(config.relative_root_path); it->isValid(); it->next())
    {
        const std::string name = it->name();
        if (!hasGroupDirPrefix(name))
            continue;
        const std::string child_rel = (fs::path(config.relative_root_path) / name).string();
        if (!disk->existsDirectory(child_rel))
            continue;
        result.push_back(name);
    }

    std::sort(result.begin(), result.end());
    return result;
}

void ANNIndexManager::loadFromDisk()
{
    std::lock_guard lk(write_mtx);

    auto disk = getDisk();
    if (!disk->existsDirectory(config.relative_root_path))
        disk->createDirectories(config.relative_root_path);

    /// A corrupt `manifest.json` is allowed to escape from here: the caller (server startup)
    /// should abort rather than silently forget the table's index state.
    auto manifest = ANNIndexManifest::loadOrEmpty(config.volume, config.relative_root_path);

    const auto now = std::chrono::steady_clock::now();
    std::vector<ANNIndexGroupPtr> next_groups;

    auto retire_by_name = [&](const std::string & name)
    {
        retired_group_meta[name] = RetiredMeta{now, nullptr};
    };

    const bool manifest_empty =
        manifest.active_groups.empty()
        && manifest.retired_groups.empty()
        && manifest.shape == ANNIndexShapeFingerprint{};

    const bool shape_mismatch = !manifest_empty && !(manifest.shape == config.shape);

    if (shape_mismatch)
    {
        LOG_WARNING(config.log,
            "ANNIndexManager: manifest shape does not match table shape; retiring {} active and {} retired groups",
            manifest.active_groups.size(),
            manifest.retired_groups.size());

        for (const auto & g : manifest.active_groups)
            retire_by_name(g);
        for (const auto & g : manifest.retired_groups)
            retire_by_name(g);
    }
    else
    {
        for (const auto & group_dir : manifest.active_groups)
        {
            ANNIndexGroupPtr grp;
            try
            {
                auto storage = std::make_shared<ANNGroupStorageDiskFull>(
                    config.volume,
                    (fs::path(config.relative_root_path) / group_dir).string(),
                    /*txn=*/nullptr);
                grp = ANNIndexGroup::load(std::move(storage), config.search_defaults);
            }
            catch (const Exception & e)
            {
                LOG_ERROR(config.log,
                    "ANNIndexManager: failed to load active group `{}`: {}; retiring",
                    group_dir, e.displayText());
                retire_by_name(group_dir);
                continue;
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(config.log,
                    "ANNIndexManager: failed to load active group `{}`: {}; retiring",
                    group_dir, e.what());
                retire_by_name(group_dir);
                continue;
            }

            if (!(grp->getShape() == config.shape))
            {
                LOG_ERROR(config.log,
                    "ANNIndexManager: active group `{}` has shape mismatch with table; retiring",
                    group_dir);
                retire_by_name(group_dir);
                continue;
            }

            if (grp->getHashSeed() != config.hash_seed)
            {
                LOG_ERROR(config.log,
                    "ANNIndexManager: active group `{}` has hash_seed mismatch with table; retiring",
                    group_dir);
                retire_by_name(group_dir);
                continue;
            }

            next_groups.push_back(std::move(grp));
        }

        for (const auto & g : manifest.retired_groups)
            retire_by_name(g);
    }

    auto next_snap = std::make_shared<ANNActiveGroupsSnapshot>();
    next_snap->groups = std::move(next_groups);
    std::atomic_store_explicit(&active, std::shared_ptr<const ANNActiveGroupsSnapshot>(std::move(next_snap)), std::memory_order_release);
}

bool ANNIndexManager::tryReserveBuildSlot()
{
    bool expected = false;
    return build_in_flight.compare_exchange_strong(
        expected, true,
        std::memory_order_acq_rel,
        std::memory_order_acquire);
}

void ANNIndexManager::releaseBuildSlot()
{
    build_in_flight.store(false, std::memory_order_release);
}

std::chrono::steady_clock::time_point ANNIndexManager::getRetiredAt(const std::string & dir) const
{
    std::lock_guard lk(write_mtx);
    auto it = retired_group_meta.find(dir);
    if (it == retired_group_meta.end())
        return std::chrono::steady_clock::time_point{};
    return it->second.retired_at;
}

ANNIndexGroupPtr ANNIndexManager::getRetiredGroupPtr(const std::string & dir) const
{
    std::lock_guard lk(write_mtx);
    auto it = retired_group_meta.find(dir);
    if (it == retired_group_meta.end())
        return nullptr;
    return it->second.group_ptr;
}

void ANNIndexManager::forgetRetiredGroup(const std::string & dir)
{
    std::lock_guard lk(write_mtx);
    retired_group_meta.erase(dir);
}

}
#endif
