#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>
#include <Storages/Reflection/ANNIndex/CoverageMap.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>


namespace DB
{

class ANNIndexSchedulerState
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
        std::vector<UUID> ready_covering_ann_index_parts;
        std::optional<UUID> primary_covering_ann_index_part;
        std::unordered_set<String> pending_tasks;
    };

    struct TaskState
    {
        String task_id;
        TaskKind kind = TaskKind::BuildBatch;
        std::vector<UUID> input_source_uuids;
        std::vector<UUID> input_ann_index_part_uuids;
        UUID output_ann_index_part_uuid;
        TaskLifecycle state = TaskLifecycle::Scheduled;
        std::chrono::system_clock::time_point created_at{};
        std::chrono::system_clock::time_point started_at{};
        std::chrono::system_clock::time_point finished_at{};
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
        bool quarantined = false;
    };

    struct TaskSnapshot
    {
        struct SourcePart
        {
            UUID uuid;
            String name;
            UInt64 rows = 0;
            UInt64 bytes = 0;
        };

        String task_id;
        TaskKind kind = TaskKind::BuildBatch;
        std::vector<UUID> input_source_uuids;
        std::vector<SourcePart> input_source_parts;
        std::vector<UUID> input_ann_index_part_uuids;
        UUID output_ann_index_part_uuid;
        TaskLifecycle state = TaskLifecycle::Scheduled;
        std::chrono::system_clock::time_point created_at{};
        std::chrono::system_clock::time_point started_at{};
        std::chrono::system_clock::time_point finished_at{};
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
        bool quarantined = false;
    };

    struct RepeatedFailureSnapshot
    {
        String failure_key;
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
        bool quarantined = false;
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
        UInt64 ready_ann_index_part_count = 0;
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
        reserved_ann_index_part_uuids.clear();
        ready_ann_index_part_to_source_uuids.clear();
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
        ready_ann_index_part_to_source_uuids.clear();

        for (const auto & [ann_index_part_uuid, covered_sources] : entries)
            appendReadyCoverage(ann_index_part_uuid, covered_sources);
    }

    void appendReadyCoverage(UUID ann_index_part_uuid, const std::vector<CoverageEntry> & entries)
    {
        auto & source_uuids = ready_ann_index_part_to_source_uuids[ann_index_part_uuid];
        for (const auto & entry : entries)
        {
            source_uuids.push_back(entry.source_part_uuid);

            auto & state = coverage[entry.source_part_uuid];
            state.source_uuid = entry.source_part_uuid;
            if (std::find(state.ready_covering_ann_index_parts.begin(), state.ready_covering_ann_index_parts.end(), ann_index_part_uuid)
                == state.ready_covering_ann_index_parts.end())
                state.ready_covering_ann_index_parts.push_back(ann_index_part_uuid);
            if (!state.primary_covering_ann_index_part)
                state.primary_covering_ann_index_part = ann_index_part_uuid;
        }
    }

    void applyRemap(
        UUID new_ann_index_part_uuid,
        UUID retired_ann_index_part_uuid,
        const std::vector<CoverageEntry> & incoming,
        const std::vector<UUID> & /*outgoing_source_uuids*/)
    {
        dropReadyMiPart(retired_ann_index_part_uuid);
        appendReadyCoverage(new_ann_index_part_uuid, incoming);
    }

    void applyRemapBatch(const std::vector<CoverageMap::RemapCommit> & commits)
    {
        for (const auto & commit : commits)
            dropReadyMiPart(commit.retired_ann_index_part_uuid);
        for (const auto & commit : commits)
            appendReadyCoverage(commit.new_ann_index_part_uuid, commit.incoming);
    }

    bool reserveBuildBatch(
        const String & task_id,
        const std::vector<UUID> & source_uuids,
        UUID output_ann_index_part_uuid)
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
        task.output_ann_index_part_uuid = output_ann_index_part_uuid;
        task.created_at = std::chrono::system_clock::now();

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
        const std::vector<UUID> & ann_index_part_uuids,
        const std::vector<UUID> & source_uuids,
        UUID output_ann_index_part_uuid)
    {
        if (task_id.empty() || ann_index_part_uuids.empty() || tasks.contains(task_id))
            return false;

        for (const auto & uuid : ann_index_part_uuids)
        {
            if (reserved_ann_index_part_uuids.contains(uuid))
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
        task.input_ann_index_part_uuids = ann_index_part_uuids;
        task.output_ann_index_part_uuid = output_ann_index_part_uuid;
        task.created_at = std::chrono::system_clock::now();

        tasks.emplace(task_id, std::move(task));
        for (const auto & uuid : ann_index_part_uuids)
            reserved_ann_index_part_uuids.emplace(uuid, task_id);
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
        const std::vector<UUID> & ann_index_part_uuids,
        const std::vector<UUID> & source_uuids,
        UUID output_ann_index_part_uuid)
    {
        if (task_id.empty() || ann_index_part_uuids.empty() || tasks.contains(task_id))
            return false;

        for (const auto & uuid : ann_index_part_uuids)
        {
            if (reserved_ann_index_part_uuids.contains(uuid))
                return false;
        }

        TaskState task;
        task.task_id = task_id;
        task.kind = TaskKind::CompactRebuild;
        task.input_source_uuids = source_uuids;
        task.input_ann_index_part_uuids = ann_index_part_uuids;
        task.output_ann_index_part_uuid = output_ann_index_part_uuid;
        task.created_at = std::chrono::system_clock::now();

        tasks.emplace(task_id, std::move(task));
        for (const auto & uuid : ann_index_part_uuids)
            reserved_ann_index_part_uuids.emplace(uuid, task_id);
        for (const auto & uuid : source_uuids)
        {
            coverage[uuid].source_uuid = uuid;
            coverage[uuid].pending_tasks.insert(task_id);
        }

        return true;
    }

    void applyCompact(
        UUID new_ann_index_part_uuid,
        const std::vector<UUID> & retired_ann_index_part_uuids,
        const std::vector<CoverageEntry> & incoming)
    {
        for (const auto & retired_uuid : retired_ann_index_part_uuids)
            dropReadyMiPart(retired_uuid);
        appendReadyCoverage(new_ann_index_part_uuid, incoming);
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
                if (coverage_it->second.ready_covering_ann_index_parts.empty() && coverage_it->second.pending_tasks.empty())
                    coverage.erase(coverage_it);
            }
        }

        for (const auto & uuid : task.input_ann_index_part_uuids)
        {
            auto reserved_it = reserved_ann_index_part_uuids.find(uuid);
            if (reserved_it != reserved_ann_index_part_uuids.end() && reserved_it->second == task_id)
                reserved_ann_index_part_uuids.erase(reserved_it);
        }

        tasks.erase(task_it);
    }

    void markTaskRunning(const String & task_id)
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return;
        it->second.state = TaskLifecycle::Running;
        it->second.started_at = std::chrono::system_clock::now();
    }

    void markTaskCommitting(const String & task_id)
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return;
        it->second.state = TaskLifecycle::Committing;
    }

    void markTaskFinished(const String & task_id)
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return;
        it->second.state = TaskLifecycle::Finished;
        it->second.finished_at = std::chrono::system_clock::now();
        it->second.last_error.clear();
    }

    void markTaskFailed(const String & task_id, const String & reason)
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return;
        it->second.state = TaskLifecycle::Failed;
        it->second.finished_at = std::chrono::system_clock::now();
        it->second.last_error = reason;
    }

    void markRunningTasksAsFailed(const String & reason)
    {
        const auto now = std::chrono::system_clock::now();
        for (auto & [_, task] : tasks)
        {
            if (task.state != TaskLifecycle::Running && task.state != TaskLifecycle::Committing)
                continue;
            task.state = TaskLifecycle::Failed;
            task.finished_at = now;
            task.last_error = reason;
        }
    }

    void setBuildsEnabled(bool enabled)
    {
        builds_enabled.store(enabled, std::memory_order_relaxed);
    }

    void setRemapsEnabled(bool enabled)
    {
        remaps_enabled.store(enabled, std::memory_order_relaxed);
    }

    bool areBuildsEnabled() const
    {
        return builds_enabled.load(std::memory_order_relaxed);
    }

    bool areRemapsEnabled() const
    {
        return remaps_enabled.load(std::memory_order_relaxed);
    }

    std::vector<TaskSnapshot> getTasksSnapshot() const
    {
        std::vector<TaskSnapshot> snapshot;
        snapshot.reserve(tasks.size());
        for (const auto & [_, task] : tasks)
        {
            snapshot.push_back(TaskSnapshot{
                .task_id = task.task_id,
                .kind = task.kind,
                .input_source_uuids = task.input_source_uuids,
                .input_source_parts = getSourcePartSnapshots(task.input_source_uuids),
                .input_ann_index_part_uuids = task.input_ann_index_part_uuids,
                .output_ann_index_part_uuid = task.output_ann_index_part_uuid,
                .state = task.state,
                .created_at = task.created_at,
                .started_at = task.started_at,
                .finished_at = task.finished_at,
                .retry_count = task.retry_count,
                .next_retry_time = task.next_retry_time,
                .last_error = task.last_error,
                .quarantined = task.quarantined,
            });
        }
        return snapshot;
    }

    std::optional<TaskSnapshot> getTaskSnapshot(const String & task_id) const
    {
        auto it = tasks.find(task_id);
        if (it == tasks.end())
            return std::nullopt;

        const auto & task = it->second;
        return TaskSnapshot{
            .task_id = task.task_id,
            .kind = task.kind,
            .input_source_uuids = task.input_source_uuids,
            .input_source_parts = getSourcePartSnapshots(task.input_source_uuids),
            .input_ann_index_part_uuids = task.input_ann_index_part_uuids,
            .output_ann_index_part_uuid = task.output_ann_index_part_uuid,
            .state = task.state,
            .created_at = task.created_at,
            .started_at = task.started_at,
            .finished_at = task.finished_at,
            .retry_count = task.retry_count,
            .next_retry_time = task.next_retry_time,
            .last_error = task.last_error,
            .quarantined = task.quarantined,
        };
    }

    std::vector<RepeatedFailureSnapshot> getRepeatedFailureSnapshots() const
    {
        const auto now = std::chrono::system_clock::now();
        std::vector<RepeatedFailureSnapshot> result;
        result.reserve(repeated_failures.size());
        for (const auto & [failure_key, failure] : repeated_failures)
        {
            if (!failure.quarantined
                && (failure.next_retry_time == std::chrono::system_clock::time_point{} || now >= failure.next_retry_time))
                continue;

            result.push_back(RepeatedFailureSnapshot{
                .failure_key = failure_key,
                .retry_count = failure.retry_count,
                .next_retry_time = failure.next_retry_time,
                .last_error = failure.last_error,
                .quarantined = failure.quarantined,
            });
        }
        return result;
    }

    bool hasActiveTasks() const
    {
        return !tasks.empty();
    }

    bool hasActiveTaskKind(TaskKind kind) const
    {
        return std::any_of(
            tasks.begin(),
            tasks.end(),
            [kind](const auto & entry)
            {
                return entry.second.kind == kind;
            });
    }

    bool hasActiveNonBuildTasks() const
    {
        return std::any_of(
            tasks.begin(),
            tasks.end(),
            [](const auto & entry)
            {
                return entry.second.kind != TaskKind::BuildBatch;
            });
    }

    std::unordered_set<UUID> activeBuildSourceUuids() const
    {
        std::unordered_set<UUID> result;
        for (const auto & [_, task] : tasks)
        {
            if (task.kind != TaskKind::BuildBatch)
                continue;

            for (const auto & uuid : task.input_source_uuids)
                result.insert(uuid);
        }
        return result;
    }

    UInt64 pendingTaskCount() const
    {
        return tasks.size();
    }

    UInt64 readyMiPartCount() const
    {
        return ready_ann_index_part_to_source_uuids.size();
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
        return reserved_ann_index_part_uuids.contains(uuid);
    }

    bool isMiPartReservedBy(UUID uuid, const String & task_id) const
    {
        auto it = reserved_ann_index_part_uuids.find(uuid);
        return it != reserved_ann_index_part_uuids.end() && it->second == task_id;
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
                && !state.ready_covering_ann_index_parts.empty())
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

    void recordTaskFailure(String failure_key, String reason, std::chrono::seconds backoff, UInt64 max_retries)
    {
        const auto now = std::chrono::system_clock::now();
        auto & failure = repeated_failures[failure_key];
        ++failure.retry_count;
        failure.last_error = std::move(reason);
        if (max_retries != 0 && failure.retry_count >= max_retries)
        {
            failure.quarantined = true;
            failure.next_retry_time = std::chrono::system_clock::time_point::max();
        }
        else
            failure.next_retry_time = now + backoff;
    }

    void clearTaskFailure(const String & failure_key)
    {
        repeated_failures.erase(failure_key);
    }

    bool isTaskFailureBackoffActive(const String & failure_key)
    {
        auto it = repeated_failures.find(failure_key);
        if (it == repeated_failures.end())
            return false;

        if (it->second.quarantined)
            return true;

        const auto now = std::chrono::system_clock::now();
        if (it->second.next_retry_time == std::chrono::system_clock::time_point{}
            || now >= it->second.next_retry_time)
        {
            return false;
        }

        return true;
    }

    ObservabilitySnapshot getObservabilitySnapshot() const
    {
        const auto now = std::chrono::system_clock::now();

        ObservabilitySnapshot snapshot;
        snapshot.backlog = backlog_stats;
        snapshot.pending_task_count = tasks.size();
        snapshot.ready_ann_index_part_count = ready_ann_index_part_to_source_uuids.size();
        snapshot.obsolete_ready_source_count = obsoleteReadySourceCount();
        snapshot.repeated_failure_count = activeTaskFailureCount(now);
        snapshot.retry_count = retry_count;
        snapshot.next_retry_time = next_retry_time;
        snapshot.last_error = last_error;

        /// Surface per-task failures too: `retry_count` / `last_error` above
        /// only reflect the global resource-backoff path (written by
        /// `postponeForResourceFailure`). Build/remap tasks that fail inside
        /// the FFI go through `recordTaskFailure`, which writes per-task
        /// state in `repeated_failures` only — without this fold-in the
        /// observability snapshot would report `retry_count=0,
        /// last_error=""` even when every build attempt is failing for the
        /// same reason. Pick the task with the highest retry_count (quarantined
        /// wins ties) so SYSTEM SYNC's timeout message points straight at the
        /// real failure cause.
        const RepeatedFailureState * worst = nullptr;
        for (const auto & [_, failure] : repeated_failures)
        {
            if (!worst
                || (failure.quarantined && !worst->quarantined)
                || (failure.quarantined == worst->quarantined && failure.retry_count > worst->retry_count))
                worst = &failure;
        }
        if (worst)
        {
            snapshot.retry_count = std::max(snapshot.retry_count, worst->retry_count);
            if (snapshot.last_error.empty() && !worst->last_error.empty())
                snapshot.last_error = worst->last_error;
        }
        return snapshot;
    }

private:
    std::vector<TaskSnapshot::SourcePart> getSourcePartSnapshots(const std::vector<UUID> & uuids) const
    {
        std::vector<TaskSnapshot::SourcePart> result;
        result.reserve(uuids.size());
        for (const auto & uuid : uuids)
        {
            TaskSnapshot::SourcePart part;
            part.uuid = uuid;
            if (auto it = sources.find(uuid); it != sources.end())
            {
                part.name = it->second.part_name;
                part.rows = it->second.rows;
                part.bytes = it->second.bytes;
            }
            result.push_back(std::move(part));
        }
        return result;
    }

    void clearExpiredTaskFailures(std::chrono::system_clock::time_point now)
    {
        for (auto it = repeated_failures.begin(); it != repeated_failures.end();)
        {
            if (!it->second.quarantined
                && it->second.retry_count == 0
                && (it->second.next_retry_time == std::chrono::system_clock::time_point{}
                    || now >= it->second.next_retry_time))
                it = repeated_failures.erase(it);
            else
                ++it;
        }
    }

    UInt64 activeTaskFailureCount(std::chrono::system_clock::time_point now) const
    {
        UInt64 count = 0;
        for (const auto & [_, failure] : repeated_failures)
        {
            if (failure.quarantined
                || (failure.next_retry_time != std::chrono::system_clock::time_point{} && now < failure.next_retry_time))
                ++count;
        }
        return count;
    }

    void dropReadyMiPart(UUID ann_index_part_uuid)
    {
        auto source_it = ready_ann_index_part_to_source_uuids.find(ann_index_part_uuid);
        if (source_it == ready_ann_index_part_to_source_uuids.end())
            return;

        for (const auto & source_uuid : source_it->second)
        {
            auto coverage_it = coverage.find(source_uuid);
            if (coverage_it == coverage.end())
                continue;

            auto & ready = coverage_it->second.ready_covering_ann_index_parts;
            ready.erase(std::remove(ready.begin(), ready.end(), ann_index_part_uuid), ready.end());
            if (coverage_it->second.primary_covering_ann_index_part == ann_index_part_uuid)
            {
                if (ready.empty())
                    coverage_it->second.primary_covering_ann_index_part.reset();
                else
                    coverage_it->second.primary_covering_ann_index_part = ready.front();
            }

            if (ready.empty() && coverage_it->second.pending_tasks.empty())
                coverage.erase(coverage_it);
        }

        ready_ann_index_part_to_source_uuids.erase(source_it);
    }

    std::unordered_map<UUID, SourceState> sources;
    std::unordered_map<UUID, CoverageState> coverage;
    std::unordered_map<String, TaskState> tasks;
    std::unordered_map<UUID, String> reserved_source_uuids;
    std::unordered_map<UUID, String> reserved_ann_index_part_uuids;
    std::unordered_map<UUID, std::vector<UUID>> ready_ann_index_part_to_source_uuids;
    struct RepeatedFailureState
    {
        UInt64 retry_count = 0;
        std::chrono::system_clock::time_point next_retry_time{};
        String last_error;
        bool quarantined = false;
    };
    std::unordered_map<String, RepeatedFailureState> repeated_failures;
    BacklogStats backlog_stats;
    UInt64 retry_count = 0;
    std::chrono::system_clock::time_point next_retry_time{};
    String last_error;
    std::atomic<bool> builds_enabled{true};
    std::atomic<bool> remaps_enabled{true};
};

}
