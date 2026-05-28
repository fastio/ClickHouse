#include <Storages/Reflection/ANNIndex/ANNIndexCompactTask.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/createVolume.h>
#include <Interpreters/Context.h>
#include <Interpreters/ANNIndexLog.h>
#include <Storages/Reflection/ANNIndex/BuildTask.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartCommitter.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
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
    const MergeTreeData::DataPartsVector & input_ann_index_parts,
    const String & new_part_name)
{
    auto replaced_names = collectPartNames(replaced_parts);
    auto input_names = collectPartNames(input_ann_index_parts);
    std::sort(replaced_names.begin(), replaced_names.end());
    std::sort(input_names.begin(), input_names.end());

    if (replaced_names != input_names)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ANNIndex compact part {} replaced unexpected parts: [{}] instead of [{}]",
            new_part_name,
            fmt::join(replaced_names, ", "),
            fmt::join(input_names, ", "));
}

}

ANNIndexCompactTask::ANNIndexCompactTask(
    ReflectionANNIndex & storage_,
    StoragePtr storage_holder_,
    StoragePtr source_storage_holder_,
    ANNIndexBuildSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector source_snapshot_,
    MergeTreeData::DataPartsVector input_ann_index_parts_,
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
    , input_ann_index_parts(std::move(input_ann_index_parts_))
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , source_metadata(std::move(source_metadata_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , estimated_output_bytes(estimated_output_bytes_)
    , task_result_callback(std::move(task_result_callback_))
{
    for (const auto & part : input_ann_index_parts)
        if (part)
            priority.value += part->getBytesOnDisk();
}

ANNIndexCompactTask::~ANNIndexCompactTask() = default;

StorageID ANNIndexCompactTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String ANNIndexCompactTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::materialized-index-compact";
}

void ANNIndexCompactTask::writeTaskLog(
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
    element.task_kind = "CompactRebuild";
    element.input_source_parts = collectPartNames(source_snapshot);
    element.input_ann_index_parts = collectPartNames(input_ann_index_parts);
    element.stage = String(stage);
    element.rows_added = rows_added;
    element.bytes_added = bytes_added;
    element.duration_ms = static_cast<Float64>(duration_ms);
    element.error_code = error_code;
    element.error_message = error_message;
    element.error = error_message;

    log->add(std::move(element));
}

void ANNIndexCompactTask::onCompleted()
{
    if (task_result_callback)
        task_result_callback(true);
}

bool ANNIndexCompactTask::executeStep()
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
                if (build_ann_index_part_task && build_ann_index_part_task->execute())
                    return true;
                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk write failed for ANNIndex compact task");
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
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk commit failed for ANNIndex compact task");
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
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex compact task in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void ANNIndexCompactTask::prepare()
{
    writeTaskLog(
        ANNIndexLogElement::Type::REFRESH_START,
        "prepare",
        /*duration_ms=*/0,
        /*error_code=*/0,
        /*error_message=*/{});

    try
    {
        auto & inner = storage_ref.getInnerMergeTreeData(inner_storage_holder);
        VolumePtr volume = inner.getStoragePolicy()->getVolume(0);
        reserved_space = MergeTreeData::reserveSpace(estimated_output_bytes, volume);
        VolumePtr data_part_volume = createVolumeFromReservation(reserved_space, volume);

        const String relative_data_path = inner.getRelativeDataPath();
        const String tmp_output_dir = String{BuildTask::TEMP_DIRECTORY_PREFIX} + entry->future_part->new_part_name;
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
        if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
            storage_ref.postponeForResourceFailure("disk reservation failed for ANNIndex compact task");
        writeTaskLog(
            ANNIndexLogElement::Type::ERROR,
            "prepare",
            /*duration_ms=*/0,
            getCurrentExceptionCode(),
            getCurrentExceptionMessage(/*with_stacktrace=*/false));
        throw;
    }

    build_ann_index_part_task = std::make_unique<BuildTask>(
        source_snapshot,
        storage_ref.getAlgorithm(),
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

void ANNIndexCompactTask::finish()
{
    Stopwatch watch;

    new_ann_index_part = build_ann_index_part_task->getFuture().get();
    if (!new_ann_index_part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex compact task did not produce a part");

    new_ann_index_part->uuid = entry->future_part->new_part_uuid;
    new_ann_index_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);

    auto entries = ReflectionANNIndex::parseCoverageJsonFromMiPart(*new_ann_index_part);
    String skip_reason;
    if (!storage_ref.shouldCommitBuildOrCompactOutput(entries, "Compact", skip_reason))
    {
        writeTaskLog(
            ANNIndexLogElement::Type::REFRESH_FINISH,
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

    /// Keep the parts lock until the old MI parts are retired so readers never
    /// observe both the input parts and the compacted part as `Active`.
    scope_guard cleanup_on_commit_failure = [this] { cleanupAfterFailedCommit(); };
    auto replaced_parts = ANNIndexPartCommitter::commitReplacingPart(
        storage_ref,
        inner_storage_holder,
        new_ann_index_part,
        *entry->future_part);
    cleanup_on_commit_failure.release();
    assertReplacedPartsMatchInput(replaced_parts, input_ann_index_parts, new_ann_index_part->name);

    std::vector<UUID> retired_uuids;
    retired_uuids.reserve(input_ann_index_parts.size());
    for (const auto & part : input_ann_index_parts)
    {
        if (!part)
            continue;
        retired_uuids.push_back(part->uuid);
    }

    storage_ref.recordCompactCommit(new_ann_index_part->uuid, retired_uuids, entries);
    if (entry && entry->future_part)
        storage_ref.clearTaskFailure(*entry->future_part);

    writeTaskLog(
        ANNIndexLogElement::Type::REFRESH_FINISH,
        "finish",
        watch.elapsedMilliseconds(),
        /*error_code=*/0,
        /*error_message=*/{},
        new_ann_index_part->rows_count,
        new_ann_index_part->getBytesOnDisk());

    if (entry)
        entry->finalize();
}

void ANNIndexCompactTask::cleanupTemporaryStorages(bool remove_output_storage) noexcept
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
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove ANNIndex compact tmp output");
        }
    }

    try
    {
        if (intermediate_storage)
            intermediate_storage->removeRecursive();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove ANNIndex compact tmp intermediate");
    }

    output_storage.reset();
    intermediate_storage.reset();
    reserved_space.reset();
    tmp_output_dir_holder.reset();
    tmp_intermediate_dir_holder.reset();
}

void ANNIndexCompactTask::cleanupAfterFailedCommit() noexcept
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
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finalize ANNIndex compact entry after commit failure");
    }
}

void ANNIndexCompactTask::cancel() noexcept
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
