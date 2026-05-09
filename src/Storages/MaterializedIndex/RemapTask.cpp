#include <Storages/MaterializedIndex/RemapTask.h>

#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedIndexLog.h>
#include <Storages/MaterializedIndex/MaterializedIndexRemapTask.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace
{

void writeLogElement(
    const ContextPtr & ctx,
    StorageMaterializedIndex & storage,
    MaterializedIndexLogElement::Type type,
    UInt64 duration_ms,
    const String & error_message)
{
    if (!ctx)
        return;

    auto log = ctx->getMaterializedIndexLog();
    if (!log)
        return;

    MaterializedIndexLogElement element;
    element.type = type;
    element.event_time = std::chrono::system_clock::now();
    element.event_time_usec = std::chrono::duration_cast<std::chrono::microseconds>(
        element.event_time.time_since_epoch()).count();

    auto sid = storage.getStorageID();
    element.database = sid.database_name;
    element.index_name = sid.table_name;
    element.uuid = sid.uuid;

    const auto & src_id = storage.getSourceTableID();
    element.source_database = src_id.database_name;
    element.source_table = src_id.table_name;
    element.family = storage.getFamily();
    element.impl = storage.getImpl();

    element.duration_ms = static_cast<Float64>(duration_ms);
    element.error = error_message;

    log->add(std::move(element));
}

}


RemapTask::RemapTask(
    StorageMaterializedIndex & storage_,
    MaterializedIndexRemapSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector affected_mi_parts_,
    MergeTreeData::DataPartsVector delta_in_source_parts_,
    std::vector<UUID> delta_out_source_uuids_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_object_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    IExecutableTask::TaskResultCallback task_result_callback_)
    : storage_ref(storage_)
    , entry(std::move(entry_))
    , affected_mi_parts(std::move(affected_mi_parts_))
    , delta_in_source_parts(std::move(delta_in_source_parts_))
    , delta_out_source_uuids(std::move(delta_out_source_uuids_))
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , task_result_callback(std::move(task_result_callback_))
{
    for (const auto & part : affected_mi_parts)
        priority.value += part->getBytesOnDisk();
}

RemapTask::~RemapTask() = default;

StorageID RemapTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String RemapTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::mi-remap";
}

void RemapTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    if (task_result_callback)
        task_result_callback(delay);
}

bool RemapTask::executeStep()
{
    switch (state)
    {
        case State::NEED_PREPARE:
        {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            try
            {
                if (remap_mi_part_task && remap_mi_part_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in MaterializedIndex RemapTask::executeStep");
                writeLogElement(
                    context,
                    storage_ref,
                    MaterializedIndexLogElement::Type::ERROR,
                    /*duration_ms=*/0,
                    getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
        }
        case State::NEED_FINISH:
        {
            finish();
            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MaterializedIndex RemapTask in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void RemapTask::prepare()
{
    writeLogElement(
        context,
        storage_ref,
        MaterializedIndexLogElement::Type::REFRESH_START,
        /*duration_ms=*/0,
        /*error_message=*/{});

    remap_mi_part_task = std::make_unique<MaterializedIndexRemapTask>(
        affected_mi_parts,
        delta_in_source_parts,
        delta_out_source_uuids,
        &storage_ref,
        source_storage,
        source_snapshot_object,
        context,
        memory_budget_bytes);
}

void RemapTask::finish()
{
    Stopwatch watch;

    new_mi_parts = remap_mi_part_task->getFuture().get();

    {
        MergeTreeData::Transaction t(storage_ref, /*txn=*/nullptr);
        for (auto & part : new_mi_parts)
            t.addPart(part, /*need_rename=*/true);
        t.renameParts();
        auto lock = storage_ref.lockParts();
        t.commit(lock);
    }

    /// Update the in-memory CoverageMap *after* releasing the storage lock —
    /// see the matching comment in `BuildTask::finish`. Each new mi-part
    /// retires exactly one old mi-part (1:1 mapping by index in MaterializedIndexRemapContext);
    /// re-parsing the freshly written manifest keeps the on-disk and
    /// in-memory views consistent even if `delta_in` / `delta_out` change in
    /// flight.
    const size_t pair_count = std::min(new_mi_parts.size(), affected_mi_parts.size());
    for (size_t i = 0; i < pair_count; ++i)
    {
        if (!new_mi_parts[i] || !affected_mi_parts[i])
            continue;
        auto incoming = StorageMaterializedIndex::parseCoverageJsonFromMiPart(*new_mi_parts[i]);
        storage_ref.coverage_map.applyRemap(
            new_mi_parts[i]->uuid,
            affected_mi_parts[i]->uuid,
            std::move(incoming),
            delta_out_source_uuids);
    }

    writeLogElement(
        context,
        storage_ref,
        MaterializedIndexLogElement::Type::REFRESH_FINISH,
        watch.elapsedMilliseconds(),
        /*error_message=*/{});

    if (entry)
        entry->finalize();
}

void RemapTask::cancel() noexcept
{
    if (remap_mi_part_task)
        remap_mi_part_task->cancel();
    remap_mi_part_task.reset();

    for (auto & part : new_mi_parts)
        if (part)
            part->removeIfNeeded();
    new_mi_parts.clear();

    if (entry)
        entry->finalize();
}

}
