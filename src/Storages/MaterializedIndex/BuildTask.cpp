#include <Storages/MaterializedIndex/BuildTask.h>

#include <Common/Stopwatch.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedIndexLog.h>
#include <Storages/MaterializedIndex/MaterializedIndexBuildTask.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/StorageInMemoryMetadata.h>


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


BuildTask::BuildTask(
    StorageMaterializedIndex & storage_,
    MaterializedIndexBuildSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector source_snapshot_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_object_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    IExecutableTask::TaskResultCallback task_result_callback_)
    : storage_ref(storage_)
    , entry(std::move(entry_))
    , source_snapshot(std::move(source_snapshot_))
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , source_metadata(std::move(source_metadata_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , task_result_callback(std::move(task_result_callback_))
{
    for (const auto & part : source_snapshot)
        priority.value += part->getBytesOnDisk();
}

BuildTask::~BuildTask() = default;

StorageID BuildTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String BuildTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::mi-build";
}

void BuildTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    if (task_result_callback)
        task_result_callback(delay);
}

bool BuildTask::executeStep()
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
                if (build_mi_part_task && build_mi_part_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in MaterializedIndex BuildTask::executeStep");
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
                "MaterializedIndex BuildTask in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void BuildTask::prepare()
{
    writeLogElement(
        context,
        storage_ref,
        MaterializedIndexLogElement::Type::BUILD_START,
        /*duration_ms=*/0,
        /*error_message=*/{});

    /// The mid-layer task constructs its own scratch / output storage during
    /// stage 1; the top-level only forwards what it received from the cycle.
    MutableDataPartStoragePtr output_storage;
    MutableDataPartStoragePtr intermediate_storage;

    build_mi_part_task = std::make_unique<MaterializedIndexBuildTask>(
        source_snapshot,
        storage_ref.getAlgorithm(),
        &storage_ref,
        entry->future_part->new_part_name,
        source_storage,
        source_snapshot_object,
        source_metadata,
        context,
        std::move(output_storage),
        std::move(intermediate_storage),
        memory_budget_bytes);
}

void BuildTask::finish()
{
    Stopwatch watch;

    new_mi_part = build_mi_part_task->getFuture().get();

    {
        MergeTreeData::Transaction t(storage_ref, /*txn=*/nullptr);
        t.addPart(new_mi_part, /*need_rename=*/false);
        auto lock = storage_ref.lockParts();
        t.commit(lock);
    }

    /// Update the in-memory CoverageMap *after* releasing the storage lock so
    /// that any thread waiting in `waitForFullCoverage` (which takes the
    /// CoverageMap mutex, not the storage one) cannot end up nested under
    /// `lockParts`. Re-parses the manifest we just wrote — the canonical
    /// source of truth for what this mi-part covers is the on-disk file.
    auto entries = StorageMaterializedIndex::parseCoverageJsonFromMiPart(*new_mi_part);
    storage_ref.coverage_map.appendFromBuild(new_mi_part->uuid, std::move(entries));

    writeLogElement(
        context,
        storage_ref,
        MaterializedIndexLogElement::Type::BUILD_FINISH,
        watch.elapsedMilliseconds(),
        /*error_message=*/{});

    if (entry)
        entry->finalize();
}

void BuildTask::cancel() noexcept
{
    if (build_mi_part_task)
        build_mi_part_task->cancel();
    build_mi_part_task.reset();

    if (new_mi_part)
        new_mi_part->removeIfNeeded();

    if (entry)
        entry->finalize();
}

}
