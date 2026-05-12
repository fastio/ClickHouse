#include <Storages/MaterializedIndex/MaterializedIndexCompactTask.h>

#include <Common/Exception.h>
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

#include <algorithm>
#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_ENOUGH_SPACE;
}

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

void assertReplacedPartsMatchInput(
    const MergeTreeData::DataPartsVector & replaced_parts,
    const MergeTreeData::DataPartsVector & input_materialized_index_parts,
    const String & new_part_name)
{
    auto replaced_names = collectPartNames(replaced_parts);
    auto input_names = collectPartNames(input_materialized_index_parts);
    std::sort(replaced_names.begin(), replaced_names.end());
    std::sort(input_names.begin(), input_names.end());

    if (replaced_names != input_names)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndex compact part {} replaced unexpected parts: [{}] instead of [{}]",
            new_part_name,
            fmt::join(replaced_names, ", "),
            fmt::join(input_names, ", "));
}

}

MaterializedIndexCompactTask::MaterializedIndexCompactTask(
    StorageMaterializedIndex & storage_,
    MaterializedIndexBuildSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector source_snapshot_,
    MergeTreeData::DataPartsVector input_materialized_index_parts_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_object_,
    StorageMetadataPtr source_metadata_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    UInt64 estimated_output_bytes_,
    IExecutableTask::TaskResultCallback task_result_callback_)
    : storage_ref(storage_)
    , entry(std::move(entry_))
    , source_snapshot(std::move(source_snapshot_))
    , input_materialized_index_parts(std::move(input_materialized_index_parts_))
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , source_metadata(std::move(source_metadata_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , estimated_output_bytes(estimated_output_bytes_)
    , task_result_callback(std::move(task_result_callback_))
{
    for (const auto & part : input_materialized_index_parts)
        if (part)
            priority.value += part->getBytesOnDisk();
}

MaterializedIndexCompactTask::~MaterializedIndexCompactTask() = default;

StorageID MaterializedIndexCompactTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String MaterializedIndexCompactTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::materialized-index-compact";
}

void MaterializedIndexCompactTask::writeTaskLog(
    MaterializedIndexLogElement::Type type,
    std::string_view stage,
    UInt64 duration_ms,
    Int32 error_code,
    const String & error_message,
    UInt64 rows_added,
    UInt64 bytes_added) const
{
    if (!context)
        return;

    auto log = context->getMaterializedIndexLog();
    if (!log)
        return;

    MaterializedIndexLogElement element;
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
    element.task_kind = "CompactRebuild";
    element.input_source_parts = collectPartNames(source_snapshot);
    element.input_materialized_index_parts = collectPartNames(input_materialized_index_parts);
    element.stage = String(stage);
    element.rows_added = rows_added;
    element.bytes_added = bytes_added;
    element.duration_ms = static_cast<Float64>(duration_ms);
    element.error_code = error_code;
    element.error_message = error_message;
    element.error = error_message;

    log->add(std::move(element));
}

void MaterializedIndexCompactTask::onCompleted()
{
    if (task_result_callback)
        task_result_callback(true);
}

bool MaterializedIndexCompactTask::executeStep()
{
    switch (state)
    {
        case State::NEED_PREPARE:
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        case State::NEED_EXECUTE:
        {
            try
            {
                if (build_materialized_index_part_task && build_materialized_index_part_task->execute())
                    return true;
                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk write failed for MaterializedIndex compact task");
                writeTaskLog(
                    MaterializedIndexLogElement::Type::ERROR,
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
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk commit failed for MaterializedIndex compact task");
                writeTaskLog(
                    MaterializedIndexLogElement::Type::ERROR,
                    "finish",
                    /*duration_ms=*/0,
                    getCurrentExceptionCode(),
                    getCurrentExceptionMessage(/*with_stacktrace=*/false));
                throw;
            }
        }
        case State::SUCCESS:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex compact task in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void MaterializedIndexCompactTask::prepare()
{
    writeTaskLog(
        MaterializedIndexLogElement::Type::REFRESH_START,
        "prepare",
        /*duration_ms=*/0,
        /*error_code=*/0,
        /*error_message=*/{});

    try
    {
        VolumePtr volume = storage_ref.getStoragePolicy()->getVolume(0);
        reserved_space = MergeTreeData::reserveSpace(estimated_output_bytes, volume);
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
        if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
            storage_ref.postponeForResourceFailure("disk reservation failed for MaterializedIndex compact task");
        writeTaskLog(
            MaterializedIndexLogElement::Type::ERROR,
            "prepare",
            /*duration_ms=*/0,
            getCurrentExceptionCode(),
            getCurrentExceptionMessage(/*with_stacktrace=*/false));
        throw;
    }

    build_materialized_index_part_task = std::make_unique<BuildTask>(
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

void MaterializedIndexCompactTask::finish()
{
    Stopwatch watch;

    new_materialized_index_part = build_materialized_index_part_task->getFuture().get();
    if (!new_materialized_index_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex compact task did not produce a part");

    new_materialized_index_part->uuid = entry->future_part->new_part_uuid;
    new_materialized_index_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);

    /// Keep the parts lock until the old MI parts are retired so readers never
    /// observe both the input parts and the compacted part as `Active`.
    {
        MergeTreeData::Transaction t(storage_ref, /*txn=*/nullptr);
        auto lock = storage_ref.lockParts();
        auto replaced_parts = storage_ref.renameTempPartAndReplaceUnlocked(
            new_materialized_index_part,
            lock,
            t,
            /*rename_in_transaction=*/false);
        assertReplacedPartsMatchInput(replaced_parts, input_materialized_index_parts, new_materialized_index_part->name);
        t.commit(lock);
    }

    std::vector<UUID> retired_uuids;
    retired_uuids.reserve(input_materialized_index_parts.size());
    for (const auto & part : input_materialized_index_parts)
    {
        if (!part)
            continue;
        retired_uuids.push_back(part->uuid);
    }

    auto entries = StorageMaterializedIndex::parseCoverageJsonFromMiPart(*new_materialized_index_part);
    storage_ref.recordCompactCommit(new_materialized_index_part->uuid, retired_uuids, entries);

    writeTaskLog(
        MaterializedIndexLogElement::Type::REFRESH_FINISH,
        "finish",
        watch.elapsedMilliseconds(),
        /*error_code=*/0,
        /*error_message=*/{},
        new_materialized_index_part->rows_count,
        new_materialized_index_part->getBytesOnDisk());

    if (entry)
        entry->finalize();
}

void MaterializedIndexCompactTask::cleanupTemporaryStorages() noexcept
{
    try
    {
        if (output_storage)
            output_storage->removeRecursive();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove MaterializedIndex compact tmp output");
    }

    try
    {
        if (intermediate_storage)
            intermediate_storage->removeRecursive();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove MaterializedIndex compact tmp intermediate");
    }

    output_storage.reset();
    intermediate_storage.reset();
    reserved_space.reset();
    tmp_output_dir_holder.reset();
    tmp_intermediate_dir_holder.reset();
}

void MaterializedIndexCompactTask::cancel() noexcept
{
    if (build_materialized_index_part_task)
    {
        build_materialized_index_part_task->cancel();
        if (!new_materialized_index_part)
            new_materialized_index_part = build_materialized_index_part_task->getUnfinishedPart();
    }
    build_materialized_index_part_task.reset();

    if (new_materialized_index_part)
    {
        new_materialized_index_part->removeIfNeeded();
        output_storage.reset();
    }
    cleanupTemporaryStorages();

    if (entry)
        entry->finalize();
}

}
