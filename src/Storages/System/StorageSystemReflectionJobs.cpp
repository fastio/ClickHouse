#include <Storages/System/StorageSystemReflectionJobs.h>

#include <Access/ContextAccess.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ReflectionJobCommon.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

#include <limits>

namespace DB
{

namespace
{

std::vector<ReflectionJob::PartSnapshot> makePartSnapshots(
    const std::vector<ANNIndexSchedulerState::TaskSnapshot::SourcePart> & parts)
{
    std::vector<ReflectionJob::PartSnapshot> result;
    result.reserve(parts.size());
    for (const auto & part : parts)
    {
        result.push_back(ReflectionJob::PartSnapshot{
            .uuid = part.uuid,
            .name = part.name,
            .rows = part.rows,
            .bytes = part.bytes,
        });
    }
    return result;
}

Int8 toLiveTaskState(ANNIndexSchedulerState::TaskLifecycle state)
{
    switch (state)
    {
        case ANNIndexSchedulerState::TaskLifecycle::Scheduled:
            return ReflectionJob::LiveTaskState::SCHEDULED;
        case ANNIndexSchedulerState::TaskLifecycle::Running:
            return ReflectionJob::LiveTaskState::RUNNING;
        case ANNIndexSchedulerState::TaskLifecycle::Committing:
            return ReflectionJob::LiveTaskState::COMMITTING;
        case ANNIndexSchedulerState::TaskLifecycle::Finished:
        case ANNIndexSchedulerState::TaskLifecycle::Failed:
            return ReflectionJob::LiveTaskState::BLOCKED;
    }
    UNREACHABLE();
}

ReflectionJob::CommonFields makeCommonFields(
    const String & database_name,
    const String & table_name,
    const ReflectionANNIndex & ann)
{
    const auto & source_id = ann.getSourceTableID();
    ReflectionJob::CommonFields result;
    result.database = database_name;
    result.reflection_name = table_name;
    result.source_database = source_id.database_name;
    result.source_table = source_id.table_name;
    result.family = ann.getFamily();
    result.impl = ann.getImpl();
    return result;
}

ReflectionJob::LiveRow makeTaskRow(
    const String & database_name,
    const String & table_name,
    const ReflectionANNIndex & ann,
    ReflectionANNIndex::SchedulerTaskSnapshot task,
    std::chrono::system_clock::time_point now)
{
    ReflectionJob::LiveRow row;
    row.snapshot_time = now;
    row.row_type = ReflectionJob::RowType::TASK;
    row.task_state = toLiveTaskState(task.state);
    row.common = makeCommonFields(database_name, table_name, ann);
    row.common.task_id = std::move(task.task_id);
    row.common.task_kind = static_cast<Int8>(task.kind);
    row.common.created_at = task.created_at;
    row.common.started_at = task.started_at;
    row.common.finished_at = task.finished_at;
    row.common.next_retry_time = task.next_retry_time;
    row.common.input_source_parts = makePartSnapshots(task.input_source_parts);
    row.common.input_reflection_part_uuids = std::move(task.input_ann_index_part_uuids);
    row.common.output_reflection_part_uuid = task.output_ann_index_part_uuid;
    row.common.retry_count = static_cast<UInt32>(std::min<UInt64>(task.retry_count, std::numeric_limits<UInt32>::max()));
    row.common.is_quarantined = static_cast<UInt8>(task.quarantined);
    row.common.exception = std::move(task.last_error);
    row.common.algorithm_exception = std::move(task.build_error);
    row.common.stage = std::move(task.build_stage);
    row.common.next_stage = std::move(task.build_next_stage);
    row.common.progress = task.build_progress;
    row.common.progress_total = task.build_stage_progress_total.value_or(0);
    row.common.stage_progress = task.build_stage_progress;
    row.common.stage_progress_total = task.build_stage_progress_total.value_or(0);
    row.common.current_shard = static_cast<UInt32>(std::min<UInt64>(task.build_current_shard, std::numeric_limits<UInt32>::max()));
    row.common.total_shards = static_cast<UInt32>(std::min<UInt64>(task.build_num_shards, std::numeric_limits<UInt32>::max()));
    row.common.rows_processed = task.rows_processed;
    row.common.rows_total = task.rows_total;
    row.common.bytes_processed = task.bytes_processed;
    row.common.bytes_total = task.bytes_total;
    row.common.settings = std::move(task.settings);
    row.common.profile_events = std::move(task.build_profile_events);
    return row;
}

}

StorageSystemReflectionJobs::StorageSystemReflectionJobs(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflectionJobs::getColumnsDescription()
{
    return ReflectionJob::getLiveColumnsDescription();
}

void StorageSystemReflectionJobs::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
    const auto now = std::chrono::system_clock::now();
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE || database->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);
        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            const auto table_name = tables_it->name();
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            const auto table = tables_it->table();
            const auto * ann = dynamic_cast<const ReflectionANNIndex *>(table.get());
            if (!ann)
                continue;

            for (auto task : ann->getSchedulerTasksSnapshot())
                ReflectionJob::appendLiveRow(res_columns, makeTaskRow(database_name, table_name, *ann, std::move(task), now));

            const auto observability = ann->getObservabilitySnapshot();
            if (ReflectionJob::hasTime(observability.next_retry_time) && now < observability.next_retry_time && !observability.last_error.empty())
            {
                ReflectionJob::LiveRow row;
                row.snapshot_time = now;
                row.row_type = ReflectionJob::RowType::RESOURCE_BACKOFF;
                row.task_state = ReflectionJob::LiveTaskState::BLOCKED;
                row.common = makeCommonFields(database_name, table_name, *ann);
                row.common.retry_count = static_cast<UInt32>(std::min<UInt64>(observability.retry_count, std::numeric_limits<UInt32>::max()));
                row.common.next_retry_time = observability.next_retry_time;
                row.common.exception = observability.last_error;
                ReflectionJob::appendLiveRow(res_columns, row);
            }

            for (const auto & failure : ann->getRepeatedFailureSnapshots())
            {
                ReflectionJob::LiveRow row;
                row.snapshot_time = now;
                row.row_type = failure.quarantined ? ReflectionJob::RowType::QUARANTINED_ROW : ReflectionJob::RowType::REPEATED_FAILURE;
                row.task_state = failure.quarantined ? ReflectionJob::LiveTaskState::QUARANTINED_STATE : ReflectionJob::LiveTaskState::BLOCKED;
                row.common = makeCommonFields(database_name, table_name, *ann);
                row.common.failure_key = failure.failure_key;
                row.common.retry_count = static_cast<UInt32>(std::min<UInt64>(failure.retry_count, std::numeric_limits<UInt32>::max()));
                row.common.next_retry_time = failure.next_retry_time;
                row.common.exception = failure.last_error;
                row.common.is_quarantined = static_cast<UInt8>(failure.quarantined);
                ReflectionJob::appendLiveRow(res_columns, row);
            }
        }
    }
}

}
