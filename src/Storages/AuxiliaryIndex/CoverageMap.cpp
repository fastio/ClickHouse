#include <Storages/AuxiliaryIndex/CoverageMap.h>

#include <algorithm>
#include <utility>

namespace DB
{

void CoverageMap::rebuildSourceMapNoLock()
{
    source_uuid_to_rows.clear();
    for (const auto & [_, entries] : auxiliary_index_to_entries)
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
        auxiliary_index_to_entries.clear();
        for (auto & [auxiliary_index_uuid, auxiliary_index_entries] : entries)
            auxiliary_index_to_entries.emplace(auxiliary_index_uuid, std::move(auxiliary_index_entries));
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::appendFromBuild(UUID auxiliary_index_part_uuid, std::vector<CoverageEntry> entries)
{
    {
        std::unique_lock lock(mutex);
        auxiliary_index_to_entries[auxiliary_index_part_uuid] = std::move(entries);
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::applyRemap(
    UUID new_auxiliary_index_part_uuid,
    UUID retired_auxiliary_index_part_uuid,
    std::vector<CoverageEntry> incoming,
    std::vector<UUID> /*outgoing_source_uuids*/)
{
    {
        std::unique_lock lock(mutex);
        auxiliary_index_to_entries.erase(retired_auxiliary_index_part_uuid);
        auxiliary_index_to_entries[new_auxiliary_index_part_uuid] = std::move(incoming);
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::applyRemapBatch(std::vector<RemapCommit> commits)
{
    {
        std::unique_lock lock(mutex);
        for (auto & commit : commits)
        {
            auxiliary_index_to_entries.erase(commit.retired_auxiliary_index_part_uuid);
            auxiliary_index_to_entries[commit.new_auxiliary_index_part_uuid] = std::move(commit.incoming);
        }
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::applyCompact(
    UUID new_auxiliary_index_part_uuid,
    const std::vector<UUID> & retired_auxiliary_index_part_uuids,
    std::vector<CoverageEntry> incoming)
{
    {
        std::unique_lock lock(mutex);
        for (const auto & retired_uuid : retired_auxiliary_index_part_uuids)
            auxiliary_index_to_entries.erase(retired_uuid);
        auxiliary_index_to_entries[new_auxiliary_index_part_uuid] = std::move(incoming);
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::dropMiPart(UUID auxiliary_index_part_uuid)
{
    {
        std::unique_lock lock(mutex);
        if (auxiliary_index_to_entries.erase(auxiliary_index_part_uuid) == 0)
            return;
        rebuildSourceMapNoLock();
    }
    cv.notify_all();
}

void CoverageMap::clear()
{
    {
        std::unique_lock lock(mutex);
        auxiliary_index_to_entries.clear();
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

std::unordered_map<UUID, CoverageEntry> CoverageMap::coverageEntriesBySourceUuid() const
{
    std::shared_lock lock(mutex);
    std::unordered_map<UUID, CoverageEntry> out;
    out.reserve(source_uuid_to_rows.size());

    for (const auto & [_, entries] : auxiliary_index_to_entries)
    {
        for (const auto & entry : entries)
        {
            auto [it, inserted] = out.emplace(entry.source_part_uuid, entry);
            if (!inserted && entry.rows > it->second.rows)
                it->second = entry;
        }
    }

    return out;
}

std::unordered_map<UUID, std::vector<CoverageEntry>> CoverageMap::coverageEntriesByMiPartUuid() const
{
    std::shared_lock lock(mutex);
    return auxiliary_index_to_entries;
}

std::unordered_set<UUID> CoverageMap::miPartUuidsCoveringAnySourceUuid(const std::vector<UUID> & source_uuids) const
{
    std::shared_lock lock(mutex);

    std::unordered_set<UUID> wanted(source_uuids.begin(), source_uuids.end());
    std::unordered_set<UUID> out;
    if (wanted.empty())
        return out;

    for (const auto & [auxiliary_index_part_uuid, entries] : auxiliary_index_to_entries)
    {
        for (const auto & entry : entries)
        {
            if (wanted.contains(entry.source_part_uuid))
            {
                out.insert(auxiliary_index_part_uuid);
                break;
            }
        }
    }

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
