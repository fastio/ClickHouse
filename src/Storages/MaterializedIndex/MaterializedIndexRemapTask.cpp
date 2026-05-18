#include <Storages/MaterializedIndex/MaterializedIndexRemapTask.h>

#include <base/scope_guard.h>

#include <Common/Exception.h>
#include <Common/Stopwatch.h>
#include <Common/TransactionID.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/MaterializedIndexLog.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartCommitter.h>
#include <Storages/MaterializedIndex/RemapTask.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>

#include <limits>


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

UInt64 estimateRemapReservationBytes(const MergeTreeData::DataPartPtr & part)
{
    if (!part)
        return 0;

    /// `locator_<seg>.bin` stores 12-byte locator entries
    /// `(part_uuid_id, part_offset)`. Reserve the worst-case rewrite size for the
    /// part, plus a small allowance for header / coverage / checksum metadata.
    static constexpr UInt64 locator_entry_size = sizeof(UInt32) + sizeof(UInt64);
    static constexpr UInt64 metadata_bytes = 64 * 1024;

    const UInt64 max = std::numeric_limits<UInt64>::max();
    if (part->rows_count > (max - metadata_bytes) / locator_entry_size)
        return max;
    return part->rows_count * locator_entry_size + metadata_bytes;
}

}


MaterializedIndexRemapTask::MaterializedIndexRemapTask(
    StorageMaterializedIndex & storage_,
    StoragePtr storage_holder_,
    StoragePtr source_storage_holder_,
    MaterializedIndexRemapSelectedEntryPtr entry_,
    MergeTreeData::DataPartsVector affected_materialized_index_parts_,
    MergeTreeData::DataPartsVector delta_in_source_parts_,
    std::vector<UUID> delta_out_source_uuids_,
    MaterializedIndexRemapKind remap_kind_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_object_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_,
    IExecutableTask::TaskResultCallback task_result_callback_)
    : storage_holder(std::move(storage_holder_))
    , source_storage_holder(std::move(source_storage_holder_))
    , storage_ref(storage_)
    , entry(std::move(entry_))
    , affected_materialized_index_parts(std::move(affected_materialized_index_parts_))
    , delta_in_source_parts(std::move(delta_in_source_parts_))
    , delta_out_source_uuids(std::move(delta_out_source_uuids_))
    , remap_kind(remap_kind_)
    , source_storage(source_storage_)
    , source_snapshot_object(std::move(source_snapshot_object_))
    , context(std::move(context_))
    , memory_budget_bytes(memory_budget_bytes_)
    , task_result_callback(std::move(task_result_callback_))
{
    if (remap_kind == MaterializedIndexRemapKind::ObsoleteCoverageCleanup)
    {
        if (!delta_in_source_parts.empty() || delta_out_source_uuids.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "ObsoleteCoverageCleanup remap expects no incoming source parts and at least one outgoing source UUID");
    }
    else if (remap_kind == MaterializedIndexRemapKind::MergeLineage
        || remap_kind == MaterializedIndexRemapKind::MutationLineage)
    {
        if (delta_in_source_parts.size() != 1 || delta_out_source_uuids.empty())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "{} remap expects exactly one incoming source part and at least one outgoing source UUID",
                materializedIndexRemapKindName(remap_kind));
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex remap task requires an explicit remap kind");
    }

    for (const auto & part : affected_materialized_index_parts)
        priority.value += part->getBytesOnDisk();
}

MaterializedIndexRemapTask::~MaterializedIndexRemapTask() = default;

StorageID MaterializedIndexRemapTask::getStorageID() const
{
    return storage_ref.getStorageID();
}

String MaterializedIndexRemapTask::getQueryId() const
{
    if (entry && entry->future_part)
        return getStorageID().getShortName() + "::" + entry->future_part->new_part_name;
    return getStorageID().getShortName() + "::materialized-index-remap";
}

void MaterializedIndexRemapTask::writeTaskLog(
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
    element.task_kind = String(materializedIndexRemapKindName(remap_kind));
    element.input_source_parts = collectPartNames(delta_in_source_parts);
    element.input_materialized_index_parts = collectPartNames(affected_materialized_index_parts);
    element.stage = String(stage);
    element.rows_added = rows_added;
    element.bytes_added = bytes_added;
    element.duration_ms = static_cast<Float64>(duration_ms);
    element.error_code = error_code;
    element.error_message = error_message;
    element.error = error_message;

    log->add(std::move(element));
}

void MaterializedIndexRemapTask::onCompleted()
{
    if (task_result_callback)
        task_result_callback(true);
}

bool MaterializedIndexRemapTask::executeStep()
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
                if (remap_materialized_index_part_task && remap_materialized_index_part_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception in MaterializedIndex MaterializedIndexRemapTask::executeStep");
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk write failed for MaterializedIndex remap task");
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
                tryLogCurrentException(__PRETTY_FUNCTION__, "Exception finishing MaterializedIndex remap task");
                if (entry && entry->future_part)
                    storage_ref.recordTaskFailure(*entry->future_part, getCurrentExceptionMessage(false));
                if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
                    storage_ref.postponeForResourceFailure("disk commit failed for MaterializedIndex remap task");
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
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MaterializedIndex MaterializedIndexRemapTask in SUCCESS state must not be executed again");
    }
    UNREACHABLE();
}

void MaterializedIndexRemapTask::prepare()
{
    writeTaskLog(
        MaterializedIndexLogElement::Type::REFRESH_START,
        "prepare",
        /*duration_ms=*/0,
        /*error_code=*/0,
        /*error_message=*/{});

    try
    {
        reserved_spaces.clear();
        reserved_spaces.reserve(affected_materialized_index_parts.size());
        for (const auto & part : affected_materialized_index_parts)
        {
            if (!part)
                continue;
            /// Remap temporary storage is derived from the old MI part storage,
            /// so reserve on the same disk rather than on an arbitrary policy volume.
            reserved_spaces.push_back(MergeTreeData::reserveSpace(
                estimateRemapReservationBytes(part),
                part->getDataPartStorage()));
        }

        remap_materialized_index_part_task = std::make_unique<RemapTask>(
            affected_materialized_index_parts,
            delta_in_source_parts,
            delta_out_source_uuids,
            &storage_ref,
            source_storage,
            source_snapshot_object,
            context,
            memory_budget_bytes,
            entry->future_part->new_part_uuid);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Exception preparing MaterializedIndex remap task");
        if (getCurrentExceptionCode() == ErrorCodes::NOT_ENOUGH_SPACE)
            storage_ref.postponeForResourceFailure("disk reservation failed for MaterializedIndex remap task");
        reserved_spaces.clear();
        writeTaskLog(
            MaterializedIndexLogElement::Type::ERROR,
            "prepare",
            /*duration_ms=*/0,
            getCurrentExceptionCode(),
            getCurrentExceptionMessage(/*with_stacktrace=*/false));
        throw;
    }
}

void MaterializedIndexRemapTask::finish()
{
    Stopwatch watch;

    new_materialized_index_parts = remap_materialized_index_part_task->getFuture().get();
    for (auto & part : new_materialized_index_parts)
    {
        if (part)
            part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
    }

    String skip_reason;
    if (entry && entry->future_part && !storage_ref.shouldCommitRemapOutput(*entry->future_part, skip_reason))
    {
        UInt64 rows_skipped = 0;
        UInt64 bytes_skipped = 0;
        for (const auto & part : new_materialized_index_parts)
        {
            if (!part)
                continue;
            rows_skipped += part->rows_count;
            bytes_skipped += part->getBytesOnDisk();
        }
        writeTaskLog(
            MaterializedIndexLogElement::Type::REFRESH_FINISH,
            "skip_stale",
            watch.elapsedMilliseconds(),
            /*error_code=*/0,
            skip_reason,
            rows_skipped,
            bytes_skipped);
        storage_ref.clearTaskFailure(*entry->future_part);
        cleanupAfterFailedCommit();
        return;
    }

    scope_guard cleanup_on_commit_failure = [this] { cleanupAfterFailedCommit(); };
    MaterializedIndexPartCommitter::commitNewParts(
        storage_ref,
        new_materialized_index_parts,
        *entry->future_part);
    cleanup_on_commit_failure.release();

    /// Update the in-memory coverage views *after* releasing the storage lock —
    /// see the matching comment in `MaterializedIndexBuildTask::finish`. Each new materialized-index-part
    /// retires exactly one old materialized-index-part (1:1 mapping by index in MaterializedIndexRemapContext);
    /// re-parsing the freshly written manifest keeps the on-disk and
    /// in-memory views consistent even if `delta_in` / `delta_out` change in
    /// flight.
    const size_t pair_count = std::min(new_materialized_index_parts.size(), affected_materialized_index_parts.size());
    for (size_t i = 0; i < pair_count; ++i)
    {
        if (!new_materialized_index_parts[i] || !affected_materialized_index_parts[i])
            continue;
        auto incoming = StorageMaterializedIndex::parseCoverageJsonFromMiPart(*new_materialized_index_parts[i]);
        storage_ref.recordRemapCommit(
            new_materialized_index_parts[i]->uuid,
            affected_materialized_index_parts[i]->uuid,
            incoming,
            delta_out_source_uuids);
    }
    if (entry && entry->future_part)
        storage_ref.clearTaskFailure(*entry->future_part);

    UInt64 rows_added = 0;
    UInt64 bytes_added = 0;
    for (const auto & part : new_materialized_index_parts)
    {
        if (!part)
            continue;
        rows_added += part->rows_count;
        bytes_added += part->getBytesOnDisk();
    }

    writeTaskLog(
        MaterializedIndexLogElement::Type::REFRESH_FINISH,
        "finish",
        watch.elapsedMilliseconds(),
        /*error_code=*/0,
        /*error_message=*/{},
        rows_added,
        bytes_added);

    reserved_spaces.clear();

    if (entry)
        entry->finalize();
}

void MaterializedIndexRemapTask::cleanupAfterFailedCommit() noexcept
{
    remap_materialized_index_part_task.reset();

    /// Replicated commit may keep locally committed parts for later part check
    /// when Keeper status is unknown; do not remove such `Active` outputs.
    for (auto & part : new_materialized_index_parts)
    {
        if (!part || part->getState() == MergeTreeDataPartState::Active)
            continue;

        try
        {
            part->removeIfNeeded();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to remove MaterializedIndex remap tmp output");
        }
    }

    new_materialized_index_parts.clear();
    reserved_spaces.clear();

    try
    {
        if (entry)
            entry->finalize();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "Failed to finalize MaterializedIndex remap entry after commit failure");
    }
}

void MaterializedIndexRemapTask::cancel() noexcept
{
    if (remap_materialized_index_part_task)
    {
        remap_materialized_index_part_task->cancel();
        if (new_materialized_index_parts.empty())
            new_materialized_index_parts = remap_materialized_index_part_task->getUnfinishedParts();
    }
    remap_materialized_index_part_task.reset();

    for (auto & part : new_materialized_index_parts)
        if (part)
            part->removeIfNeeded();
    new_materialized_index_parts.clear();
    reserved_spaces.clear();

    if (entry)
        entry->finalize();
}

}
