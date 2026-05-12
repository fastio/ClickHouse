#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/MaterializedIndex/CoverageMap.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <algorithm>
#include <chrono>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

class MaterializedIndexSchedulerState
{
public:
    enum class SourceLifecycle
    {
        Active,
        Obsolete,
    };

    enum class TaskKind
    {
        BuildBatch,
        RemapLineage,
        CompactMerge,
        CompactRebuild,
    };

    enum class TaskLifecycle
    {
        Scheduled,
        Running,
        Committing,
        Finished,
        Failed,
    };

    struct SourceState
    {
        UUID uuid;
        UInt64 rows = 0;
        UInt64 bytes = 0;
        String part_name;
        MergeTreePartInfo part_info;
        std::chrono::steady_clock::time_point first_seen;
        std::chrono::steady_clock::time_point last_seen;
        SourceLifecycle lifecycle = SourceLifecycle::Active;
    };

    struct CoverageState
    {
        UUID source_uuid;
        std::vector<UUID> ready_covering_materialized_index_parts;
        std::optional<UUID> primary_covering_materialized_index_part;
        std::unordered_set<String> pending_tasks;
    };

    struct TaskState
    {
        String task_id;
        TaskKind kind = TaskKind::BuildBatch;
        std::vector<UUID> input_source_uuids;
        std::vector<UUID> input_materialized_index_part_uuids;
        UUID output_materialized_index_part_uuid;
        TaskLifecycle state = TaskLifecycle::Scheduled;
    };

    struct BacklogStats
    {
        UInt64 rows = 0;
        UInt64 bytes = 0;
        UInt64 parts = 0;
    };

    struct ObservabilitySnapshot
    {
        BacklogStats backlog;
        UInt64 pending_task_count = 0;
        UInt64 ready_materialized_index_part_count = 0;
        UInt64 obsolete_ready_source_count = 0;
        UInt64 repeated_failure_count = 0;
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
    };

    void clear()
    {
        sources.clear();
        coverage.clear();
        tasks.clear();
        reserved_source_uuids.clear();
        reserved_materialized_index_part_uuids.clear();
        ready_materialized_index_part_to_source_uuids.clear();
        repeated_failures.clear();
        retry_count = 0;
        next_retry_time = {};
        last_error.clear();
    }

    void refreshSources(const MergeTreeData::DataPartsVector & active_source_parts)
    {
        const auto now = std::chrono::steady_clock::now();
        std::unordered_set<UUID> active_uuids;
        active_uuids.reserve(active_source_parts.size());

        for (const auto & part : active_source_parts)
        {
            if (!part)
                continue;

            active_uuids.insert(part->uuid);
            auto [it, inserted] = sources.emplace(part->uuid, SourceState{});
            auto & state = it->second;
            if (inserted)
            {
                state.uuid = part->uuid;
                state.first_seen = now;
            }

            state.rows = part->rows_count;
            state.bytes = part->getBytesOnDisk();
            state.part_name = part->name;
            state.part_info = part->info;
            state.last_seen = now;
            state.lifecycle = SourceLifecycle::Active;
        }

        for (auto & [uuid, state] : sources)
        {
            if (!active_uuids.contains(uuid))
            {
                state.last_seen = now;
                state.lifecycle = SourceLifecycle::Obsolete;
            }
        }
    }

    void replaceReadyCoverage(const std::vector<std::pair<UUID, std::vector<CoverageEntry>>> & entries)
    {
        coverage.clear();
        ready_materialized_index_part_to_source_uuids.clear();

        for (const auto & [materialized_index_part_uuid, covered_sources] : entries)
            appendReadyCoverage(materialized_index_part_uuid, covered_sources);
    }

    void appendReadyCoverage(UUID materialized_index_part_uuid, const std::vector<CoverageEntry> & entries)
    {
        auto & source_uuids = ready_materialized_index_part_to_source_uuids[materialized_index_part_uuid];
        for (const auto & entry : entries)
        {
            source_uuids.push_back(entry.source_part_uuid);

            auto & state = coverage[entry.source_part_uuid];
            state.source_uuid = entry.source_part_uuid;
            if (std::find(state.ready_covering_materialized_index_parts.begin(), state.ready_covering_materialized_index_parts.end(), materialized_index_part_uuid)
                == state.ready_covering_materialized_index_parts.end())
                state.ready_covering_materialized_index_parts.push_back(materialized_index_part_uuid);
            if (!state.primary_covering_materialized_index_part)
                state.primary_covering_materialized_index_part = materialized_index_part_uuid;
        }
    }

    void applyRemap(
        UUID new_materialized_index_part_uuid,
        UUID retired_materialized_index_part_uuid,
        const std::vector<CoverageEntry> & incoming,
        const std::vector<UUID> & /*outgoing_source_uuids*/)
    {
        dropReadyMiPart(retired_materialized_index_part_uuid);
        appendReadyCoverage(new_materialized_index_part_uuid, incoming);
    }

    bool reserveBuildBatch(
        const String & task_id,
        const std::vector<UUID> & source_uuids,
        UUID output_materialized_index_part_uuid)
    {
        if (task_id.empty() || source_uuids.empty() || tasks.contains(task_id))
            return false;

        for (const auto & uuid : source_uuids)
        {
            if (reserved_source_uuids.contains(uuid))
                return false;
        }

        TaskState task;
        task.task_id = task_id;
        task.kind = TaskKind::BuildBatch;
        task.input_source_uuids = source_uuids;
        task.output_materialized_index_part_uuid = output_materialized_index_part_uuid;

        tasks.emplace(task_id, std::move(task));
        for (const auto & uuid : source_uuids)
        {
            reserved_source_uuids.emplace(uuid, task_id);
            coverage[uuid].source_uuid = uuid;
            coverage[uuid].pending_tasks.insert(task_id);
        }

        return true;
    }

    bool reserveRemapLineage(
        const String & task_id,
        const std::vector<UUID> & materialized_index_part_uuids,
        const std::vector<UUID> & source_uuids,
        UUID output_materialized_index_part_uuid)
    {
        if (task_id.empty() || materialized_index_part_uuids.empty() || tasks.contains(task_id))
            return false;

        for (const auto & uuid : materialized_index_part_uuids)
        {
            if (reserved_materialized_index_part_uuids.contains(uuid))
                return false;
        }
        for (const auto & uuid : source_uuids)
        {
            if (reserved_source_uuids.contains(uuid))
                return false;
        }

        TaskState task;
        task.task_id = task_id;
        task.kind = TaskKind::RemapLineage;
        task.input_source_uuids = source_uuids;
        task.input_materialized_index_part_uuids = materialized_index_part_uuids;
        task.output_materialized_index_part_uuid = output_materialized_index_part_uuid;

        tasks.emplace(task_id, std::move(task));
        for (const auto & uuid : materialized_index_part_uuids)
            reserved_materialized_index_part_uuids.emplace(uuid, task_id);
        for (const auto & uuid : source_uuids)
        {
            reserved_source_uuids.emplace(uuid, task_id);
            coverage[uuid].source_uuid = uuid;
            coverage[uuid].pending_tasks.insert(task_id);
        }

        return true;
    }

    bool reserveCompactRebuild(
        const String & task_id,
        const std::vector<UUID> & materialized_index_part_uuids,
        const std::vector<UUID> & source_uuids,
        UUID output_materialized_index_part_uuid)
    {
        if (task_id.empty() || materialized_index_part_uuids.empty() || tasks.contains(task_id))
            return false;

        for (const auto & uuid : materialized_index_part_uuids)
        {
            if (reserved_materialized_index_part_uuids.contains(uuid))
                return false;
        }

        TaskState task;
        task.task_id = task_id;
        task.kind = TaskKind::CompactRebuild;
        task.input_source_uuids = source_uuids;
        task.input_materialized_index_part_uuids = materialized_index_part_uuids;
        task.output_materialized_index_part_uuid = output_materialized_index_part_uuid;

        tasks.emplace(task_id, std::move(task));
        for (const auto & uuid : materialized_index_part_uuids)
            reserved_materialized_index_part_uuids.emplace(uuid, task_id);
        for (const auto & uuid : source_uuids)
        {
            coverage[uuid].source_uuid = uuid;
            coverage[uuid].pending_tasks.insert(task_id);
        }

        return true;
    }

    void applyCompact(
        UUID new_materialized_index_part_uuid,
        const std::vector<UUID> & retired_materialized_index_part_uuids,
        const std::vector<CoverageEntry> & incoming)
    {
        for (const auto & retired_uuid : retired_materialized_index_part_uuids)
            dropReadyMiPart(retired_uuid);
        appendReadyCoverage(new_materialized_index_part_uuid, incoming);
    }

    void releaseTask(const String & task_id)
    {
        auto task_it = tasks.find(task_id);
        if (task_it == tasks.end())
            return;

        const auto & task = task_it->second;
        for (const auto & uuid : task.input_source_uuids)
        {
            auto reserved_it = reserved_source_uuids.find(uuid);
            if (reserved_it != reserved_source_uuids.end() && reserved_it->second == task_id)
                reserved_source_uuids.erase(reserved_it);

            auto coverage_it = coverage.find(uuid);
            if (coverage_it != coverage.end())
            {
                coverage_it->second.pending_tasks.erase(task_id);
                if (coverage_it->second.ready_covering_materialized_index_parts.empty() && coverage_it->second.pending_tasks.empty())
                    coverage.erase(coverage_it);
            }
        }

        for (const auto & uuid : task.input_materialized_index_part_uuids)
        {
            auto reserved_it = reserved_materialized_index_part_uuids.find(uuid);
            if (reserved_it != reserved_materialized_index_part_uuids.end() && reserved_it->second == task_id)
                reserved_materialized_index_part_uuids.erase(reserved_it);
        }

        tasks.erase(task_it);
    }

    bool hasActiveTasks() const
    {
        return !tasks.empty();
    }

    UInt64 pendingTaskCount() const
    {
        return tasks.size();
    }

    UInt64 readyMiPartCount() const
    {
        return ready_materialized_index_part_to_source_uuids.size();
    }

    bool isSourceReserved(UUID uuid) const
    {
        return reserved_source_uuids.contains(uuid);
    }

    bool isSourceReservedBy(UUID uuid, const String & task_id) const
    {
        auto it = reserved_source_uuids.find(uuid);
        return it != reserved_source_uuids.end() && it->second == task_id;
    }

    bool isMiPartReserved(UUID uuid) const
    {
        return reserved_materialized_index_part_uuids.contains(uuid);
    }

    bool isMiPartReservedBy(UUID uuid, const String & task_id) const
    {
        auto it = reserved_materialized_index_part_uuids.find(uuid);
        return it != reserved_materialized_index_part_uuids.end() && it->second == task_id;
    }

    bool hasPendingTaskForSource(UUID uuid) const
    {
        auto it = coverage.find(uuid);
        return it != coverage.end() && !it->second.pending_tasks.empty();
    }

    UInt64 obsoleteReadySourceCount() const
    {
        UInt64 count = 0;
        for (const auto & [uuid, state] : coverage)
        {
            auto source_it = sources.find(uuid);
            if (source_it != sources.end()
                && source_it->second.lifecycle == SourceLifecycle::Obsolete
                && !state.ready_covering_materialized_index_parts.empty())
                ++count;
        }
        return count;
    }

    void setBacklogStats(BacklogStats stats)
    {
        backlog_stats = stats;
    }

    BacklogStats getBacklogStats() const
    {
        return backlog_stats;
    }

    void postponeForResourceFailure(String reason, std::chrono::seconds backoff)
    {
        ++retry_count;
        last_error = std::move(reason);
        next_retry_time = std::chrono::system_clock::now() + backoff;
    }

    void clearResourceBackoff()
    {
        retry_count = 0;
        next_retry_time = {};
        last_error.clear();
    }

    bool isResourceBackoffActive() const
    {
        return next_retry_time != std::chrono::system_clock::time_point{}
            && std::chrono::system_clock::now() < next_retry_time;
    }

    void recordTaskFailure(String failure_key, String reason, std::chrono::seconds backoff)
    {
        const auto now = std::chrono::system_clock::now();
        clearExpiredTaskFailures(now);
        auto & failure = repeated_failures[failure_key];
        ++failure.retry_count;
        failure.last_error = std::move(reason);
        failure.next_retry_time = now + backoff;
    }

    void clearTaskFailure(const String & failure_key)
    {
        repeated_failures.erase(failure_key);
    }

    bool isTaskFailureBackoffActive(const String & failure_key) const
    {
        auto it = repeated_failures.find(failure_key);
        if (it == repeated_failures.end())
            return false;

        const auto now = std::chrono::system_clock::now();
        if (it->second.next_retry_time == std::chrono::system_clock::time_point{}
            || now >= it->second.next_retry_time)
        {
            repeated_failures.erase(it);
            return false;
        }

        return true;
    }

    ObservabilitySnapshot getObservabilitySnapshot() const
    {
        clearExpiredTaskFailures(std::chrono::system_clock::now());

        ObservabilitySnapshot snapshot;
        snapshot.backlog = backlog_stats;
        snapshot.pending_task_count = tasks.size();
        snapshot.ready_materialized_index_part_count = ready_materialized_index_part_to_source_uuids.size();
        snapshot.obsolete_ready_source_count = obsoleteReadySourceCount();
        snapshot.repeated_failure_count = repeated_failures.size();
        snapshot.retry_count = retry_count;
        snapshot.next_retry_time = next_retry_time;
        snapshot.last_error = last_error;
        return snapshot;
    }

private:
    void clearExpiredTaskFailures(std::chrono::system_clock::time_point now) const
    {
        for (auto it = repeated_failures.begin(); it != repeated_failures.end();)
        {
            if (it->second.next_retry_time == std::chrono::system_clock::time_point{}
                || now >= it->second.next_retry_time)
                it = repeated_failures.erase(it);
            else
                ++it;
        }
    }

    void dropReadyMiPart(UUID materialized_index_part_uuid)
    {
        auto source_it = ready_materialized_index_part_to_source_uuids.find(materialized_index_part_uuid);
        if (source_it == ready_materialized_index_part_to_source_uuids.end())
            return;

        for (const auto & source_uuid : source_it->second)
        {
            auto coverage_it = coverage.find(source_uuid);
            if (coverage_it == coverage.end())
                continue;

            auto & ready = coverage_it->second.ready_covering_materialized_index_parts;
            ready.erase(std::remove(ready.begin(), ready.end(), materialized_index_part_uuid), ready.end());
            if (coverage_it->second.primary_covering_materialized_index_part == materialized_index_part_uuid)
            {
                if (ready.empty())
                    coverage_it->second.primary_covering_materialized_index_part.reset();
                else
                    coverage_it->second.primary_covering_materialized_index_part = ready.front();
            }

            if (ready.empty() && coverage_it->second.pending_tasks.empty())
                coverage.erase(coverage_it);
        }

        ready_materialized_index_part_to_source_uuids.erase(source_it);
    }

    std::unordered_map<UUID, SourceState> sources;
    std::unordered_map<UUID, CoverageState> coverage;
    std::unordered_map<String, TaskState> tasks;
    std::unordered_map<UUID, String> reserved_source_uuids;
    std::unordered_map<UUID, String> reserved_materialized_index_part_uuids;
    std::unordered_map<UUID, std::vector<UUID>> ready_materialized_index_part_to_source_uuids;
    struct RepeatedFailureState
    {
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
    };
    mutable std::unordered_map<String, RepeatedFailureState> repeated_failures;
    BacklogStats backlog_stats;
    UInt64 retry_count = 0;
    std::chrono::system_clock::time_point next_retry_time{};
    String last_error;
};

}
