#include <Storages/MaterializedIndex/MaterializedIndexBuildTask.h>

#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedIndexLog.h>
#include <Storages/MaterializedIndex/BuildTask.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
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


MaterializedIndexBuildTask::MaterializedIndexBuildTask(
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

MaterializedIndexBuildTask::~MaterializedIndexBuildTask() = default;

StorageID MaterializedIndexBuildTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String MaterializedIndexBuildTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::materialized-index-build";
}

void MaterializedIndexBuildTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    if (task_result_callback)
        task_result_callback(delay);
}

bool MaterializedIndexBuildTask::executeStep()
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
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in MaterializedIndex MaterializedIndexBuildTask::executeStep");
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
                "MaterializedIndex MaterializedIndexBuildTask in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void MaterializedIndexBuildTask::prepare()
{
    writeLogElement(
        context,
        storage_ref,
        MaterializedIndexLogElement::Type::BUILD_START,
        /*duration_ms=*/0,
        /*error_message=*/{});

    try
    {
        /// Keep the reservation and tmp directory guards alive for the whole
        /// build, otherwise cleanup may race with the writer.
        UInt64 expected_size = 0;
        for (const auto & part : source_snapshot)
            expected_size += part->getBytesOnDisk();

        VolumePtr volume = storage_ref.getStoragePolicy()->getVolume(0);
        reserved_space = MergeTreeData::reserveSpace(expected_size, volume);
        VolumePtr data_part_volume = createVolumeFromReservation(reserved_space, volume);

        const String relative_data_path = storage_ref.getRelativeDataPath();
        const String tmp_output_dir = String{BuildTask::TEMP_DIRECTORY_PREFIX} + entry->future_part->new_part_name;
        const String tmp_intermediate_dir = tmp_output_dir + "__intermediate";

        tmp_output_dir_holder = storage_ref.getTemporaryPartDirectoryHolder(tmp_output_dir);
        tmp_intermediate_dir_holder = storage_ref.getTemporaryPartDirectoryHolder(tmp_intermediate_dir);

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
        tryLogCurrentException(__PRETTY_FUNCTION__, "Exception preparing MaterializedIndex build storage");
        writeLogElement(
            context,
            storage_ref,
            MaterializedIndexLogElement::Type::ERROR,
            /*duration_ms=*/0,
            getCurrentExceptionMessage(/*with_stacktrace=*/false));
        throw;
    }

    build_mi_part_task = std::make_unique<BuildTask>(
        source_snapshot,
        storage_ref.getAlgorithm(),
        &storage_ref,
        entry->future_part->new_part_name,
        source_storage,
        source_snapshot_object,
        source_metadata,
        context,
        output_storage,
        intermediate_storage,
        memory_budget_bytes);
}

void MaterializedIndexBuildTask::finish()
{
    Stopwatch watch;

    new_mi_part = build_mi_part_task->getFuture().get();
    if (!new_mi_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex build task did not produce a part");

    new_mi_part->uuid = entry->future_part->new_part_uuid;

    /// The build runs outside any user transaction, so stamp the part with
    /// the prehistoric TID — same convention used by `MergeTask` and
    /// loadDataParts when txn == nullptr. Without this, `renameTempPartAndAdd`
    /// rejects the part because `version.creation_tid` is still the all-zero
    /// `EmptyTID` initialised at construction time.
    new_mi_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);

    {
        MergeTreeData::Transaction t(storage_ref, /*txn=*/nullptr);
        auto lock = storage_ref.lockParts();
        storage_ref.renameTempPartAndAdd(
            new_mi_part,
            t,
            lock,
            /*rename_in_transaction=*/false);
        t.commit(lock);
    }

    /// Update the in-memory CoverageMap *after* releasing the storage lock so
    /// that any thread waiting in `waitForFullCoverage` (which takes the
    /// CoverageMap mutex, not the storage one) cannot end up nested under
    /// `lockParts`. Re-parses the manifest we just wrote — the canonical
    /// source of truth for what this materialized-index-part covers is the on-disk file.
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

void MaterializedIndexBuildTask::cancel() noexcept
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
