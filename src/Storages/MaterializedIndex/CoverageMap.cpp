#include <Storages/MaterializedIndex/CoverageMap.h>

#include <algorithm>
#include <utility>

namespace DB
{

void CoverageMap::rebuildSourceMapNoLock()
{
    source_uuid_to_rows.clear();
    for (const auto & [_, entries] : mi_to_entries)
    {
        for (const auto & entry : entries)
        {
            auto it = source_uuid_to_rows.find(entry.source_part_uuid);
            if (it == source_uuid_to_rows.end())
                source_uuid_to_rows.emplace(entry.source_part_uuid, entry.rows);
            else
                it->second = std::max(it->second, entry.rows);
        }
    }
}

bool CoverageMap::isFullyCoveringNoLock(const std::unordered_set<UUID> & source_active_uuids) const
{
    for (const auto & uuid : source_active_uuids)
    {
        if (!source_uuid_to_rows.contains(uuid))
            return false;
    }
    return true;
}

void CoverageMap::replaceAll(std::vector<std::pair<UUID, std::vector<CoverageEntry>>> entries)
{
    {
        std::unique_lock lock(mutex);
        mi_to_entries.clear();
        for (auto & [mi_uuid, mi_entries] : entries)
            mi_to_entries.emplace(mi_uuid, std::move(mi_entries));
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::appendFromBuild(UUID mi_part_uuid, std::vector<CoverageEntry> entries)
{
    {
        std::unique_lock lock(mutex);
        mi_to_entries[mi_part_uuid] = std::move(entries);
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::applyRemap(
    UUID new_mi_part_uuid,
    UUID retired_mi_part_uuid,
    std::vector<CoverageEntry> incoming,
    std::vector<UUID> /*outgoing_source_uuids*/)
{
    {
        std::unique_lock lock(mutex);
        mi_to_entries.erase(retired_mi_part_uuid);
        mi_to_entries[new_mi_part_uuid] = std::move(incoming);
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::dropMiPart(UUID mi_part_uuid)
{
    {
        std::unique_lock lock(mutex);
        if (mi_to_entries.erase(mi_part_uuid) == 0)
            return;
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::clear()
{
    {
        std::unique_lock lock(mutex);
        mi_to_entries.clear();
        source_uuid_to_rows.clear();
    }
    cv.notify_all();
}

std::unordered_set<UUID> CoverageMap::coveredSourceUuids() const
{
    std::shared_lock lock(mutex);
    std::unordered_set<UUID> out;
    out.reserve(source_uuid_to_rows.size());
    for (const auto & [uuid, _] : source_uuid_to_rows)
        out.insert(uuid);
    return out;
}

UInt64 CoverageMap::coveredRows() const
{
    std::shared_lock lock(mutex);
    UInt64 total = 0;
    for (const auto & [_, rows] : source_uuid_to_rows)
        total += rows;
    return total;
}

bool CoverageMap::isFullyCovering(const std::unordered_set<UUID> & source_active_uuids) const
{
    std::shared_lock lock(mutex);
    return isFullyCoveringNoLock(source_active_uuids);
}

bool CoverageMap::waitForFullCoverage(
    const std::unordered_set<UUID> & source_active_uuids,
    std::chrono::milliseconds timeout)
{
    std::unique_lock lock(mutex);
    return cv.wait_for(lock, timeout, [&] { return isFullyCoveringNoLock(source_active_uuids); });
}

}
