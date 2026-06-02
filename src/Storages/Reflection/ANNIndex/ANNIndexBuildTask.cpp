#include <Storages/Reflection/ANNIndex/ANNIndexBuildTask.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Interpreters/ANNIndexLog.h>
#include <Storages/Reflection/ANNIndex/BuildTask.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartCommitter.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
    extern const int ABORTED;
}

namespace DB::FailPoints
{
    extern const char ann_index_build_pause_in_finish[];
}

namespace DB::ANNIndex
{

namespace ErrorCodes = DB::ErrorCodes;
namespace FailPoints = DB::FailPoints;


namespace
{

std::vector<String> collectPartNames(const MergeTreeData::DataPartsVector & parts)
{
    std::vector<String> names;
    names.reserve(parts.size());
    for (const auto & part : parts)
        if (part)
            names.push_back(part->name);
    return names;
}

}


BuildTask::BuildTask(
    ReflectionANNIndex & storage_,
    StoragePtr storage_holder_,
    StoragePtr source_storage_holder_,
    ANNIndexBuildSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector source_snapshot_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_object_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    UInt64 estimated_output_bytes_,
    IExecutableTask::TaskResultCallback task_result_callback_)
    : storage_holder(std::move(storage_holder_))
    , source_storage_holder(std::move(source_storage_holder_))
    , inner_storage_holder(entry_ && entry_->future_part ? entry_->future_part->inner_table_snapshot : storage_.getInnerTable())
    , storage_ref(storage_)
    , entry(std::move(entry_))
    , source_snapshot(std::move(source_snapshot_))
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , source_metadata(std::move(source_metadata_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , estimated_output_bytes(estimated_output_bytes_)
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
    return getStorageID().getShortName() + "::materialized-index-build";
}

void BuildTask::writeTaskLog(
    ANNIndexLogElement::Type type,
    std::string_view stage,
    UInt64 duration_ms,
    Int32 error_code,
    const String & error_message,
    UInt64 rows_added,
    UInt64 bytes_added) const
{
    if (!context)
        return;

    auto log = context->getANNIndexLog();
    if (!log)
        return;

    ANNIndexLogElement element;
    element.type = type;
    element.event_time = std::chrono::system_clock::now();
    element.event_time_usec = std::chrono::duration_cast<std::chrono::microseconds>(
        element.event_time.time_since_epoch()).count();

    auto sid = storage_ref.getStorageID();
    element.database = sid.database_name;
    element.index_name = sid.table_name;
    element.uuid = sid.uuid;

    const auto & src_id = storage_ref.getSourceTableID();
    element.source_database = src_id.database_name;
    element.source_table = src_id.table_name;
    element.family = storage_ref.getFamily();
    element.impl = storage_ref.getImpl();

    if (entry && entry->future_part)
        element.task_id = toString(entry->future_part->new_part_uuid);
    else
        element.task_id = getQueryId();
    element.task_kind = "Build";
    element.input_source_parts = collectPartNames(source_snapshot);
    element.stage = String(stage);
    element.rows_added = rows_added;
    element.bytes_added = bytes_added;
    element.duration_ms = static_cast<Float64>(duration_ms);
    element.error_code = error_code;
    element.error_message = error_message;
    element.error = error_message;

    log->add(std::move(element));
}

void BuildTask::onCompleted()
{
    if (task_result_callback)
        task_result_callback(true);
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
                if (build_ann_index_part_task && build_ann_index_part_task->execute())
                    return true;

                intermediate_storage.reset();
                tmp_intermediate_dir_holder.reset();
                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in ANNIndex::BuildTask::executeStep");
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                writeTaskLog(
                    ANNIndexLogElement::Type::ERROR,
                    "execute",
                    /*duration_ms=*/0,
                    getCurrentExceptionCode(),
                    getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
        }
        case State::NEED_FINISH:
        {
            try
            {
                finish();
                state = State::SUCCESS;
                return false;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception finishing ANNIndex build task");
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                writeTaskLog(
                    ANNIndexLogElement::Type::ERROR,
                    "finish",
                    /*duration_ms=*/0,
                    getCurrentExceptionCode(),
                    getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
        }
        case State::SUCCESS:
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ANNIndex::BuildTask in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void BuildTask::prepare()
{
    writeTaskLog(
        ANNIndexLogElement::Type::BUILD_START,
        "prepare",
        /*duration_ms=*/0,
        /*error_code=*/0,
        /*error_message=*/{});

    try
    {
        /// Keep the reservation and tmp directory guards alive for the whole
        /// build, otherwise cleanup may race with the writer.
        auto & inner = storage_ref.getInnerMergeTreeData(inner_storage_holder);
        VolumePtr volume = inner.getStoragePolicy()->getVolume(0);
        reserved_space = MergeTreeData::reserveSpace(estimated_output_bytes, volume);
        VolumePtr data_part_volume = createVolumeFromReservation(reserved_space, volume);

        const String relative_data_path = inner.getRelativeDataPath();
        const String tmp_output_dir = String{BuildTaskImpl::TEMP_DIRECTORY_PREFIX} + entry->future_part->new_part_name;
        const String tmp_intermediate_dir = tmp_output_dir + "__intermediate";

        tmp_output_dir_holder = inner.getTemporaryPartDirectoryHolder(tmp_output_dir);
        tmp_intermediate_dir_holder = inner.getTemporaryPartDirectoryHolder(tmp_intermediate_dir);

        output_storage = std::make_shared<DataPartStorageOnDiskFull>(
            data_part_volume, relative_data_path, tmp_output_dir);
        intermediate_storage = std::make_shared<DataPartStorageOnDiskFull>(
            data_part_volume, relative_data_path, tmp_intermediate_dir);

        output_storage->beginTransaction();
        output_storage->createDirectories();
        intermediate_storage->createDirectories();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Exception preparing ANNIndex build storage");
        if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
            storage_ref.postponeForResourceFailure("disk reservation failed for ANNIndex build task");
        writeTaskLog(
            ANNIndexLogElement::Type::ERROR,
            "prepare",
            /*duration_ms=*/0,
            getCurrentExceptionCode(),
            getCurrentExceptionMessage(/*with_stacktrace=*/false));
        throw;
    }

    /// Per-task algorithm instance. Avoids cross-task contamination of
    /// per-build streaming state (e.g. the DiskANN `fbin_writer`).
    if (auto * shared = storage_ref.getAlgorithm())
        build_algorithm = shared->cloneForBuild();

    build_ann_index_part_task = std::make_unique<BuildTaskImpl>(
        source_snapshot,
        build_algorithm.get(),
        &storage_ref,
        inner_storage_holder,
        entry->future_part->new_part_name,
        source_storage,
        source_snapshot_object,
        source_metadata,
        context,
        output_storage,
        intermediate_storage,
        memory_budget_bytes,
        entry->future_part->new_part_uuid);
}

void BuildTask::finish()
{
    Stopwatch watch;

    new_ann_index_part = build_ann_index_part_task->getFuture().get();
    if (!new_ann_index_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex build task did not produce a part");

    new_ann_index_part->uuid = entry->future_part->new_part_uuid;

    /// The build runs outside any user transaction, so stamp the part with
    /// the prehistoric TID — same convention used by `MergeTask` and
    /// loadDataParts when txn == nullptr. Without this, `renameTempPartAndAdd`
    /// rejects the part because `version.creation_tid` is still the all-zero
    /// `EmptyTID` initialised at construction time.
    new_ann_index_part->version->setAndStoreCreationTID(Tx::NonTransactionalTID, nullptr);

    /// Update the in-memory coverage views *after* releasing the storage lock so
    /// that any thread waiting in `waitForFullCoverage` (which takes the
    /// CoverageMap mutex, not the storage one) cannot end up nested under
    /// `lockParts`. Re-parses the manifest we just wrote — the canonical
    /// source of truth for what this materialized-index-part covers is the on-disk file.
    auto entries = ReflectionANNIndex::parseCoverageJsonFromMiPart(*new_ann_index_part);
    String skip_reason;
    if (!storage_ref.shouldCommitBuildOrCompactOutput(entries, "Build", skip_reason))
    {
        writeTaskLog(
            ANNIndexLogElement::Type::BUILD_FINISH,
            "skip_stale",
            watch.elapsedMilliseconds(),
            /*error_code=*/0,
            skip_reason,
            new_ann_index_part->rows_count,
            new_ann_index_part->getBytesOnDisk());
        cleanupTemporaryStorages(/*remove_output_storage=*/true);
        if (entry && entry->future_part)
            storage_ref.clearTaskFailure(*entry->future_part);
        if (entry)
            entry->finalize();
        return;
    }

    scope_guard cleanup_on_commit_failure = [this] { cleanupAfterFailedCommit(); };

    /// Test hook: pause here after the materialized-index part is fully
    /// written but before the Keeper commit. Used by integration tests
    /// that need to simulate a leader exception while it holds the lease
    /// — killing the process at this point leaves the ephemeral lease /
    /// task-lock guard to expire naturally and the surviving replica
    /// must re-acquire them and rebuild.
    FailPointInjection::pauseFailPoint(FailPoints::ann_index_build_pause_in_finish);

    /// If the storage entered shutdown while we were paused (e.g.
    /// `DROP TABLE ... SYNC` raced with a paused build), bail out before the
    /// Keeper commit. `cleanup_on_commit_failure` releases the half-finished
    /// part and replicated reservations; `entry->finalize()` (called from the
    /// `cancel()` path or from this task's destructor) drops the `StoragePtr`
    /// so `waitTableFinallyDropped` can complete.
    if (storage_ref.isShuttingDown())
        throw Exception(ErrorCodes::ABORTED, "ANNIndex build aborted by storage shutdown");

    ANNIndexPartCommitter::commitNewPart(
        storage_ref,
        inner_storage_holder,
        new_ann_index_part,
        *entry->future_part);
    cleanup_on_commit_failure.release();

    storage_ref.recordBuildCommit(new_ann_index_part->uuid, entries);
    if (entry && entry->future_part)
        storage_ref.clearTaskFailure(*entry->future_part);

    writeTaskLog(
        ANNIndexLogElement::Type::BUILD_FINISH,
        "finish",
        watch.elapsedMilliseconds(),
        /*error_code=*/0,
        /*error_message=*/{},
        new_ann_index_part->rows_count,
        new_ann_index_part->getBytesOnDisk());

    if (entry)
        entry->finalize();
}

void BuildTask::cleanupTemporaryStorages(bool remove_output_storage) noexcept
{
    if (remove_output_storage)
    {
        try
        {
            if (output_storage)
                output_storage->removeRecursive();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove ANNIndex build tmp output");
        }
    }

    try
    {
        if (intermediate_storage && intermediate_storage->exists())
            intermediate_storage->removeRecursive();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove ANNIndex build tmp intermediate");
    }

    output_storage.reset();
    intermediate_storage.reset();
    reserved_space.reset();
    tmp_output_dir_holder.reset();
    tmp_intermediate_dir_holder.reset();
}

void BuildTask::cleanupAfterFailedCommit() noexcept
{
    /// Replicated commit may keep a locally committed part for later part check
    /// when Keeper status is unknown; do not remove such an `Active` output.
    const bool remove_output_storage
        = !new_ann_index_part || new_ann_index_part->getState() != MergeTreeDataPartState::Active;
    cleanupTemporaryStorages(remove_output_storage);

    try
    {
        if (entry)
            entry->finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finalize ANNIndex build entry after commit failure");
    }
}

void BuildTask::cancel() noexcept
{
    if (build_ann_index_part_task)
    {
        build_ann_index_part_task->cancel();
        if (!new_ann_index_part)
            new_ann_index_part = build_ann_index_part_task->getUnfinishedPart();
    }
    build_ann_index_part_task.reset();

    if (new_ann_index_part)
    {
        new_ann_index_part->removeIfNeeded();
        output_storage.reset();
    }
    cleanupTemporaryStorages();

    if (entry)
        entry->finalize();
}

}
