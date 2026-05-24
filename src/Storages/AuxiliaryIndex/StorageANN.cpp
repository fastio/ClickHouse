#include <Storages/AuxiliaryIndex/StorageANN.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexBuildTask.h>
#include <Storages/AuxiliaryIndex/ANNAlgorithmFactory.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexCompactTask.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexContext.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartName.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartitionScheduling.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexSchedulerPolicy.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexSelectedEntry.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexRemapTask.h>
#include <Storages/AuxiliaryIndex/SnapshotDiffReconciler.h>

#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>
#include <Core/Settings.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPartition.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Common/SipHash.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <exception>
#include <limits>
#include <optional>
#include <tuple>
#include <unordered_map>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool assign_part_uuids;
    extern const MergeTreeSettingsUInt64 auxiliary_index_build_min_bytes;
    extern const MergeTreeSettingsUInt64 auxiliary_index_build_min_parts;
    extern const MergeTreeSettingsUInt64 auxiliary_index_build_min_rows;
    extern const MergeTreeSettingsSeconds auxiliary_index_build_max_delay;
    extern const MergeTreeSettingsUInt64 auxiliary_index_compact_min_parts;
    extern const MergeTreeSettingsUInt64 auxiliary_index_compact_tombstone_ratio_percent;
    extern const MergeTreeSettingsUInt64 auxiliary_index_commit_min_valuable_rows_ratio_percent;
    extern const MergeTreeSettingsUInt64 auxiliary_index_max_background_tasks_per_source_table;
    extern const MergeTreeSettingsUInt64 auxiliary_index_max_global_background_tasks;
    extern const MergeTreeSettingsSeconds auxiliary_index_resource_failure_backoff;
    extern const MergeTreeSettingsUInt64 auxiliary_index_size_ratio_percent;
    extern const MergeTreeSettingsUInt64 auxiliary_index_starvation_protection_cycles;
    extern const MergeTreeSettingsUInt64 auxiliary_index_sync_timeout;
    extern const MergeTreeSettingsUInt64 auxiliary_index_task_max_input_bytes;
    extern const MergeTreeSettingsUInt64 auxiliary_index_task_max_input_rows;
    extern const MergeTreeSettingsUInt64 auxiliary_index_task_memory_budget_bytes;
}

namespace FailPoints
{
    extern const char auxiliary_index_throw_in_try_reserve_future_part[];
    extern const char auxiliary_index_build_pause_in_finish[];
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


namespace
{

String makeAuxiliaryIndexCompactPartName(
    const MergeTreeData::DataPartsVector & auxiliary_index_parts,
    MergeTreeDataFormatVersion format_version)
{
    std::vector<MergeTreePartInfo> part_infos;
    part_infos.reserve(auxiliary_index_parts.size());
    for (const auto & part : auxiliary_index_parts)
    {
        if (!part)
            continue;
        part_infos.push_back(part->info);
    }
    return makeAuxiliaryIndexCompactPartNameFromInfos(part_infos, format_version);
}

String getAuxiliaryIndexPartNameFromAST(const ASTPtr & partition)
{
    const auto * literal = partition->as<ASTLiteral>();
    if (!literal)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected a string literal for part name, got: {}", partition->formatForErrorMessage());
    return literal->value.safeGet<String>();
}

ASTPtr makePartitionIdAst(String partition_id)
{
    auto partition = make_intrusive<ASTPartition>();
    partition->setPartitionID(make_intrusive<ASTLiteral>(std::move(partition_id)));
    return partition;
}

ASTPtr mapAuxiliaryIndexPartitionForInner(
    const StorageANN & storage,
    const ASTPtr & partition,
    ContextPtr context)
{
    const auto * partition_ast = partition->as<ASTPartition>();
    if (!partition_ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected partition AST, got: {}", partition->formatForErrorMessage());

    if (partition_ast->all)
        return partition;

    String partition_id;
    if (!partition_ast->value)
    {
        partition_id = storage.getPartitionIDFromQuery(partition, context);
        return makePartitionIdAst(partition_id);
    }
    else
    {
        auto source_storage = DatabaseCatalog::instance().tryGetTable(storage.getSourceTableID(), context);
        const auto * source_mt = source_storage ? dynamic_cast<const MergeTreeData *>(source_storage.get()) : nullptr;
        if (!source_mt)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot resolve source partition for AuxiliaryIndex {} because source table {} is not available as MergeTree",
                storage.getStorageID().getNameForLogs(),
                storage.getSourceTableID().getNameForLogs());
        partition_id = source_mt->getPartitionIDFromQuery(partition, context);
    }

    return makePartitionIdAst(getAuxiliaryIndexPhysicalPartitionId(partition_id));
}

void executeInnerPartitionCommand(
    const StorageANN & storage,
    PartitionCommand command,
    ContextPtr query_context)
{
    auto inner_storage_table = storage.getInnerTable();
    if (!inner_storage_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AuxiliaryIndex {} does not have an inner table", storage.getStorageID().getNameForLogs());

    PartitionCommands commands{std::move(command)};
    auto & inner = storage.getInnerMergeTreeData(inner_storage_table);
    auto inner_metadata = inner.getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    inner_storage_table->alterPartition(inner_metadata, commands, query_context);
}

PartitionCommand mapAuxiliaryIndexPartitionCommandForInner(
    const StorageANN & storage,
    const PartitionCommand & command,
    ContextPtr query_context)
{
    if (command.type != PartitionCommand::DROP_PARTITION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "{} is not supported for AuxiliaryIndex yet",
            command.typeToString());

    PartitionCommand inner_command = command;
    if (!command.part)
        inner_command.partition = mapAuxiliaryIndexPartitionForInner(storage, command.partition, query_context);
    return inner_command;
}

/// Minimal concrete subclass of MutationsSnapshotBase so stage-1 can return
/// an "empty, read-only" snapshot without depending on StorageMergeTree's
/// private MutationsSnapshot layout. The three pure-virtual methods answer
/// with empties because AuxiliaryIndex never produces mutations in
/// stage-1.
struct EmptyMutationsSnapshot final : public MergeTreeData::MutationsSnapshotBase
{
    using Params = MergeTreeData::IMutationsSnapshot::Params;

    EmptyMutationsSnapshot(Params params_, MutationCounters counters_, DataPartsVector patches_)
        : MergeTreeData::MutationsSnapshotBase(std::move(params_), std::move(counters_), std::move(patches_))
    {
    }

    MutationCommands getOnFlyMutationCommandsForPart(const MergeTreeData::DataPartPtr & /*part*/) const override
    {
        return {};
    }

    std::shared_ptr<MergeTreeData::IMutationsSnapshot> cloneEmpty() const override
    {
        return std::make_shared<EmptyMutationsSnapshot>(Params{}, MutationCounters{}, DataPartsVector{});
    }

    NameSet getAllUpdatedColumns() const override
    {
        return {};
    }
};

std::vector<UUID> collectPartUuids(const MergeTreeData::DataPartsVector & parts)
{
    std::vector<UUID> uuids;
    uuids.reserve(parts.size());
    for (const auto & part : parts)
    {
        if (part)
            uuids.push_back(part->uuid);
    }
    return uuids;
}

std::pair<UInt64, UInt64> sumRowsAndBytes(const MergeTreeData::DataPartsVector & parts)
{
    UInt64 rows = 0;
    UInt64 bytes = 0;
    for (const auto & part : parts)
    {
        if (!part)
            continue;
        rows += part->rows_count;
        bytes += part->getBytesOnDisk();
    }
    return {rows, bytes};
}

std::vector<ReconcileSourcePart> makeReconcileSourcePartViews(const MergeTreeData::DataPartsVector & parts)
{
    std::vector<ReconcileSourcePart> views;
    views.reserve(parts.size());
    for (const auto & part : parts)
    {
        if (!part)
            continue;

        ReconcileSourcePart view;
        view.uuid = part->uuid;
        view.partition_id = part->info.getPartitionId();
        view.min_block = part->info.min_block;
        view.max_block = part->info.max_block;
        view.level = part->info.level;
        view.mutation = part->info.mutation;
        view.rows = part->rows_count;
        view.has_part_info = true;
        view.part = part;
        views.push_back(std::move(view));
    }
    return views;
}

std::unordered_set<UUID> collectPartUuidSet(const MergeTreeData::DataPartsVector & parts)
{
    std::unordered_set<UUID> uuids;
    uuids.reserve(parts.size());
    for (const auto & part : parts)
    {
        if (part)
            uuids.insert(part->uuid);
    }
    return uuids;
}

void updateHashWithUuid(SipHash & hash, const UUID & uuid)
{
    hash.update(UUIDHelpers::getHighBytes(uuid));
    hash.update(UUIDHelpers::getLowBytes(uuid));
}

String makeReplicationTaskKey(const FutureAuxiliaryIndexPart & future_part, const String & family, const String & impl)
{
    SipHash hash;
    hash.update(family);
    hash.update(impl);
    hash.update(static_cast<UInt8>(future_part.kind));
    hash.update(static_cast<UInt8>(future_part.remap_kind));

    auto update_part_vector = [&hash](const MergeTreeData::DataPartsVector & parts)
    {
        std::vector<UUID> uuids = collectPartUuids(parts);
        std::sort(uuids.begin(), uuids.end());
        for (const auto & uuid : uuids)
            updateHashWithUuid(hash, uuid);
    };

    update_part_vector(future_part.source_parts_snapshot);
    update_part_vector(future_part.affected_auxiliary_index_parts);
    update_part_vector(future_part.delta_in_source_parts);

    auto delta_out = future_part.delta_out_source_uuids;
    std::sort(delta_out.begin(), delta_out.end());
    for (const auto & uuid : delta_out)
        updateHashWithUuid(hash, uuid);

    return getSipHash128AsHexString(hash);
}

std::string_view materializedIndexTaskKindName(FutureAuxiliaryIndexPart::Kind kind)
{
    switch (kind)
    {
        case FutureAuxiliaryIndexPart::Kind::Build:
            return "Build";
        case FutureAuxiliaryIndexPart::Kind::Remap:
            return "Remap";
        case FutureAuxiliaryIndexPart::Kind::Compact:
            return "Compact";
    }
    UNREACHABLE();
}

String makeTaskFailureKey(const FutureAuxiliaryIndexPart & future_part, const String & family, const String & impl)
{
    return fmt::format("{}:{}", materializedIndexTaskKindName(future_part.kind), makeReplicationTaskKey(future_part, family, impl));
}

UInt64 readTombstoneRowsFromHeader(const IDataPartStorage & storage)
{
    if (!storage.existsFile("header.json"))
        return 0;

    auto reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj || !obj->has("tombstone_rows"))
        return 0;
    return obj->getValue<UInt64>("tombstone_rows");
}

std::mutex auxiliary_index_task_counters_mutex;
UInt64 global_auxiliary_index_task_count = 0;
std::unordered_map<String, UInt64> auxiliary_index_tasks_by_source_table;

ASTPtr makeInnerColumnList()
{
    auto columns = make_intrusive<ASTColumns>();
    auto column_list = make_intrusive<ASTExpressionList>();

    auto source_partition = make_intrusive<ASTColumnDeclaration>();
    source_partition->name = AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN;
    source_partition->setType(make_intrusive<ASTIdentifier>("String"));
    column_list->children.push_back(source_partition);

    auto marker = make_intrusive<ASTColumnDeclaration>();
    marker->name = "_index_marker";
    marker->setType(make_intrusive<ASTIdentifier>("UInt8"));
    marker->default_specifier = ColumnDefaultSpecifier::Default;
    marker->setDefaultExpression(make_intrusive<ASTLiteral>(UInt64{0}));
    column_list->children.push_back(marker);
    columns->set(columns->columns, column_list);
    return columns;
}

ASTPtr makeInnerStorageDefinition(const String & zookeeper_path, const String & replica_name)
{
    auto storage = make_intrusive<ASTStorage>();
    auto engine = zookeeper_path.empty()
        ? makeASTFunction("MergeTree")
        : makeASTFunction(
            "ReplicatedMergeTree",
            make_intrusive<ASTLiteral>(zookeeper_path),
            make_intrusive<ASTLiteral>(replica_name));
    engine->setKind(ASTFunction::Kind::TABLE_ENGINE);
    storage->set(storage->engine, engine);
    storage->set(storage->partition_by, make_intrusive<ASTIdentifier>(AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN));
    storage->set(storage->order_by, makeASTFunction("tuple"));

    /// Disable insert deduplication on the inner replicated storage. MI parts
    /// are derived deterministically from the source, so two replicas racing
    /// to build the same coverage produce byte-identical blocks; with default
    /// dedup window the loser of the race trips
    /// `ReplicatedMergeTreeSink::writeExistingPart` (it only knows how to
    /// recover dedup for ATTACH PART / RESTORE prefixes, throws otherwise).
    /// The materialized-index leader lease already serializes builds, so the
    /// insert-level dedup adds nothing but failure modes here.
    if (!zookeeper_path.empty())
    {
        auto settings = make_intrusive<ASTSetQuery>();
        settings->is_standalone = false;
        SettingChange change;
        change.name = "replicated_deduplication_window";
        change.value = Field(UInt64{0});
        settings->changes.push_back(std::move(change));
        storage->set(storage->settings, settings);
    }

    storage->normalizeChildrenOrder();
    return storage;
}

}


String makeAuxiliaryIndexReplicationTaskKeyForTest(
    const FutureAuxiliaryIndexPart & future_part,
    const String & family,
    const String & impl)
{
    return makeReplicationTaskKey(future_part, family, impl);
}

StorageANN::StorageANN(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageID & source_table_id_,
    const Names & indexed_columns_,
    const String & family_,
    const String & impl_,
    const ASTPtr & build_params_,
    ContextMutablePtr context_,
    const StorageInMemoryMetadata & metadata_,
    std::unique_ptr<MergeTreeSettings> settings_,
    LoadingStrictnessLevel mode,
    const String & inner_zookeeper_path_,
    const String & inner_replica_name_)
    : MergeTreeData(
          table_id_,
          metadata_,
          context_,
          /*date_column_name=*/ String{},
          MergingParams{},
          std::move(settings_),
          /*require_part_metadata_=*/ true,
          mode)
    , source_table_id(source_table_id_)
    , indexed_columns(indexed_columns_)
    , family(family_)
    , impl(impl_)
    , build_params(build_params_)
    , inner_zookeeper_path(inner_zookeeper_path_)
    , inner_replica_name(inner_replica_name_)
    , cleanup_thread(*this)
{
    initializeDirectoriesAndFormatVersion(relative_data_path_, LoadingStrictnessLevel::ATTACH <= mode, /*date_column_name=*/ String{});
    initializeInnerTable(relative_data_path_, metadata_, nullptr, mode);

    AuxiliaryIndexContext ctx;
    ctx.source_table_id = source_table_id;
    ctx.indexed_columns = indexed_columns;
    ctx.family = family;
    ctx.impl = impl;
    ctx.query_context = context_;

    algorithm = ANNAlgorithmFactory::instance().get(family, impl, build_params, ctx);
    if (algorithm)
        algorithm->initialize(ctx);
}

String StorageANN::generateInnerTableName(const StorageID & index_id)
{
    if (index_id.hasUUID())
        return ".inner_id." + toString(index_id.uuid);
    return ".inner." + index_id.getTableName();
}

void StorageANN::initializeInnerTable(
    const String & /*relative_data_path_*/,
    const StorageInMemoryMetadata & /*metadata_*/,
    std::unique_ptr<MergeTreeSettings> /*settings_*/,
    LoadingStrictnessLevel mode)
{
    const auto index_id = getStorageID();
    StorageID inner_id(index_id.database_name, generateInnerTableName(index_id));
    auto context = getContext();

    if (auto existing = DatabaseCatalog::instance().tryGetTable(inner_id, context))
        inner_table = existing;
    else if (LoadingStrictnessLevel::ATTACH <= mode)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inner table {} for AuxiliaryIndex {} is not loaded",
            inner_id.getNameForLogs(),
            index_id.getNameForLogs());
    }
    else
    {
        auto create_context = Context::createCopy(context);
        auto create = make_intrusive<ASTCreateQuery>();
        create->setDatabase(inner_id.database_name);
        create->setTable(inner_id.table_name);
        create->set(create->columns_list, makeInnerColumnList());
        create->set(create->storage, makeInnerStorageDefinition(inner_zookeeper_path, inner_replica_name));

        InterpreterCreateQuery interpreter(create, create_context);
        interpreter.setInternal(true);
        interpreter.execute();
        inner_table = DatabaseCatalog::instance().getTable(inner_id, context);
    }

    auto * inner_data = dynamic_cast<MergeTreeData *>(inner_table.get());
    if (!inner_data)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Inner table {} for AuxiliaryIndex {} is not MergeTree-family storage",
            inner_id.getNameForLogs(),
            index_id.getNameForLogs());

    /// All parts stored in this inner table are AuxiliaryIndex parts
    /// (`MergeTreeDataPartBuilder::build` stamps them with
    /// `Kind::AuxiliaryIndex`). Tell `MergeTreeData` so that lookups by
    /// `part_name` / `partition_id` string — which cannot recover the kind
    /// from the string — produce keys whose kind matches the stored parts.
    /// Without this, ALTER ... DROP PART / DROP PARTITION on the inner table
    /// transparently miss every part. See `parsePartName`,
    /// `makePartitionID`, `makeDataPartStateAndPartitionID` in MergeTreeData.h.
    inner_data->default_part_kind_for_name_lookup = MergeTreePartInfo::Kind::AuxiliaryIndex;
}

StoragePtr StorageANN::getInnerTable() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return inner_table;
}

MergeTreeData & StorageANN::getInnerMergeTreeData(const StoragePtr & inner_table_snapshot) const
{
    auto * inner_data = dynamic_cast<MergeTreeData *>(inner_table_snapshot.get());
    if (!inner_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AuxiliaryIndex {} has no inner MergeTree storage", getStorageID().getNameForLogs());
    return *inner_data;
}


void StorageANN::startup()
{
    /// D-07 defensive recheck: the source table's `assign_part_uuids` may
    /// have been turned off after CREATE. We do not throw here (would
    /// prevent server startup); a warning is enough — Build / Remap will
    /// still see the missing UUIDs and surface failures through the log.
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, getContext());
    if (source_storage)
    {
        const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
        if (!source_mt || !(*source_mt->getSettings())[MergeTreeSetting::assign_part_uuids])
            LOG_ERROR(
                log,
                "Source table {} no longer has assign_part_uuids = 1; AuxiliaryIndex {} will be degraded.",
                source_table_id.getNameForLogs(),
                getStorageID().getNameForLogs());
    }

    auto inner_table_snapshot = getInnerTable();
    auto & inner = getInnerMergeTreeData(inner_table_snapshot);
    inner.clearOldTemporaryDirectories(
        /*custom_directories_lifetime_seconds=*/0,
        {"tmp_auxiliary_index_build_", "tmp_auxiliary_index_remap_"});

    refreshCoverageFromActiveParts();
    try
    {
        cleanup_thread.start();
        background_operations_assignee.start();
    }
    catch (...)
    {
        try
        {
            shutdown(/*is_drop=*/false);
        }
        catch (...)
        {
            std::terminate();
        }
        throw;
    }
}

void StorageANN::refreshCoverageFromActiveParts()
{
    /// Walk every active materialized-index-part and ingest its `coverage.json` manifest
    /// into the in-memory `CoverageMap`. Startup and replicated followers
    /// call this so the reconciler does not re-trigger Build / Remap for parts
    /// that already fully cover the source. A malformed manifest is logged but
    /// does not abort refresh — `cleanup_thread` will eventually GC truly
    /// broken parts; until then the reconciler may schedule extra work.
    std::vector<std::pair<UUID, std::vector<CoverageEntry>>> snapshot;
    auto auxiliary_index_parts = getAccessPathPartsVectorForInternalUsage();
    snapshot.reserve(auxiliary_index_parts.size());
    for (const auto & part : auxiliary_index_parts)
    {
        if (!part)
            continue;
        try
        {
            if (algorithm)
            {
                auto compatibility = algorithm->checkPartCompatibility(part->getDataPartStorage());
                if (!compatibility.compatible)
                {
                    /// Logged at trace level: this is the expected outcome for the
                    /// empty placeholder parts that ALTER ... DROP PART / DROP
                    /// PARTITION leaves behind in the inner MergeTree (they have
                    /// no `algorithm_private_fingerprint.json`). Logging at warn
                    /// level here would make every DROP query on a AuxiliaryIndex
                    /// inner table emit stderr noise that is indistinguishable
                    /// from a real corruption signal.
                    LOG_TRACE(
                        log,
                        "Skipping materialized-index-part {} because it is incompatible with algorithm {}/{}: {}",
                        part->name,
                        algorithm->getFamily(),
                        algorithm->getName(),
                        compatibility.reason);
                    continue;
                }
            }
            auto entries = parseCoverageJsonFromMiPart(*part);
            snapshot.emplace_back(part->uuid, std::move(entries));
        }
        catch (...)
        {
            tryLogCurrentException(
                log,
                fmt::format("Failed to load coverage.json for materialized-index-part {}", part->name));
        }
    }
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.replaceAll(snapshot);
    scheduler_state.replaceReadyCoverage(snapshot);
}

void StorageANN::shutdown(bool is_drop)
{
    if (shutdown_called.exchange(true))
        return;
    /// Wake any background task currently blocked at a test-only pause hook
    /// (e.g. `auxiliary_index_build_pause_in_finish`). Without this,
    /// `DROP TABLE ... SYNC` would deadlock in `waitTableFinallyDropped`
    /// because the paused task still holds a `StoragePtr` to this storage.
    /// The task itself observes `isShuttingDown()` after the pause returns
    /// and aborts cleanly via its scope-guard cleanup path.
    try
    {
        FailPointInjection::notifyFailPoint(FailPoints::auxiliary_index_build_pause_in_finish);
    }
    catch (...) // NOLINT(bugprone-empty-catch): expected when failpoint is not enabled
    {
    }
    background_operations_assignee.finish();
    cleanup_thread.stop();
    if (is_drop)
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        coverage_map.clear();
        scheduler_state.clear();
    }
}

void StorageANN::renameInMemory(const StorageID & new_table_id)
{
    auto inner_table_snapshot = getInnerTable();
    if (inner_table_snapshot)
    {
        auto old_inner_id = inner_table_snapshot->getStorageID();
        StorageID new_inner_id(new_table_id.database_name, generateInnerTableName(new_table_id));
        if (old_inner_id.database_name != new_inner_id.database_name || old_inner_id.table_name != new_inner_id.table_name)
        {
            auto rename = make_intrusive<ASTRenameQuery>();
            rename->addElement(
                old_inner_id.database_name,
                old_inner_id.table_name,
                new_inner_id.database_name,
                new_inner_id.table_name);
            InterpreterRenameQuery(rename, getContext()).execute();
            auto renamed_inner_table = DatabaseCatalog::instance().getTable(new_inner_id, getContext());
            if (auto * renamed_inner_data = dynamic_cast<MergeTreeData *>(renamed_inner_table.get()))
                renamed_inner_data->default_part_kind_for_name_lookup = MergeTreePartInfo::Kind::AuxiliaryIndex;
            {
                std::lock_guard lock(currently_processing_in_background_mutex);
                if (inner_table == inner_table_snapshot)
                    inner_table = std::move(renamed_inner_table);
            }
        }
    }

    MergeTreeData::renameInMemory(new_table_id);
}

void StorageANN::drop()
{
    dropInnerTableIfAny(/*sync=*/false, getContext());
}

void StorageANN::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    /// Release our own reference to the inner storage before issuing the (possibly
    /// synchronous) DROP. Otherwise `waitTableFinallyDropped` keeps spinning because
    /// our `inner_table` shared_ptr keeps the storage alive after the catalog removes
    /// it, producing a self-deadlock that hangs `DROP TABLE mi_<index> SYNC`.
    StorageID inner_id = StorageID::createEmpty();
    String pending_lease_path;
    String pending_lease_payload;
    StoragePtr pending_lease_inner_table;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        if (!inner_table)
            return;
        inner_id = inner_table->getStorageID();
        inner_table.reset();
        /// A leader lease that has not been handed to a task yet is owned by
        /// this storage object; do not let it keep the inner storage alive.
        pending_lease_path.swap(pending_replicated_leader_lease_path);
        pending_lease_payload.swap(pending_replicated_leader_lease_payload);
        pending_lease_inner_table.swap(pending_replicated_leader_inner_table);
    }

    if (!pending_lease_path.empty() && !pending_lease_payload.empty())
    {
        if (auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(pending_lease_inner_table.get()))
            replicated->releaseAuxiliaryIndexLeaderLease(pending_lease_path, pending_lease_payload);
    }
    pending_lease_inner_table.reset();

    if (DatabaseCatalog::instance().tryGetTable(inner_id, getContext()))
    {
        InterpreterDropQuery::executeDropQuery(
            ASTDropQuery::Kind::Drop,
            getContext(),
            local_context,
            inner_id,
            sync,
            /*ignore_sync_setting=*/true,
            /*need_ddl_guard=*/false);
    }
}

void StorageANN::backupData(
    BackupEntriesCollector &,
    const String &,
    const std::optional<ASTs> &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "BACKUP WITH AUXILIARY INDEXES is not implemented yet. Back up the source table and rebuild the AuxiliaryIndex after restore");
}

void StorageANN::restoreDataFromBackup(
    RestorerFromBackup &,
    const String &,
    const std::optional<ASTs> &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "RESTORE WITH AUXILIARY INDEXES is not implemented yet. Restore the source table and rebuild the AuxiliaryIndex");
}

bool StorageANN::supportsBackupPartition() const
{
    return false;
}

void StorageANN::recordBuildCommit(UUID auxiliary_index_part_uuid, const std::vector<CoverageEntry> & entries)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.appendFromBuild(auxiliary_index_part_uuid, entries);
    scheduler_state.appendReadyCoverage(auxiliary_index_part_uuid, entries);
}

void StorageANN::recordRemapCommit(
    UUID new_auxiliary_index_part_uuid,
    UUID retired_auxiliary_index_part_uuid,
    const std::vector<CoverageEntry> & incoming,
    const std::vector<UUID> & outgoing_source_uuids)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyRemap(new_auxiliary_index_part_uuid, retired_auxiliary_index_part_uuid, incoming, outgoing_source_uuids);
    scheduler_state.applyRemap(new_auxiliary_index_part_uuid, retired_auxiliary_index_part_uuid, incoming, outgoing_source_uuids);
}

void StorageANN::recordRemapBatchCommit(std::vector<CoverageMap::RemapCommit> commits)
{
    std::vector<CoverageMap::RemapCommit> scheduler_commits;
    scheduler_commits.reserve(commits.size());
    for (const auto & commit : commits)
        scheduler_commits.push_back(commit);

    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyRemapBatch(std::move(commits));
    scheduler_state.applyRemapBatch(scheduler_commits);
}

void StorageANN::recordCompactCommit(
    UUID new_auxiliary_index_part_uuid,
    const std::vector<UUID> & retired_auxiliary_index_part_uuids,
    const std::vector<CoverageEntry> & incoming)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyCompact(new_auxiliary_index_part_uuid, retired_auxiliary_index_part_uuids, incoming);
    scheduler_state.applyCompact(new_auxiliary_index_part_uuid, retired_auxiliary_index_part_uuids, incoming);
}

bool StorageANN::shouldCommitBuildOrCompactOutput(
    const std::vector<CoverageEntry> & entries,
    const String & task_kind,
    String & reason) const
{
    const UInt64 threshold_percent = (*getSettings())[MergeTreeSetting::auxiliary_index_commit_min_valuable_rows_ratio_percent];
    if (threshold_percent == 0)
        return true;

    auto context = getContext();
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, context);
    const auto * source_mt = source_storage ? dynamic_cast<const MergeTreeData *>(source_storage.get()) : nullptr;
    if (!source_mt)
    {
        reason = fmt::format("{} output skipped because source table is no longer available", task_kind);
        return false;
    }

    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    const auto value = SnapshotDiffReconciler::evaluateCoverageCommitValue(makeReconcileSourcePartViews(source_snapshot), entries);
    if (value.total_rows == 0)
    {
        reason = fmt::format("{} output skipped because coverage is empty", task_kind);
        return false;
    }

    if (value.valuableRatioPercent() >= threshold_percent)
    {
        LOG_DEBUG(
            log,
            "Committing AuxiliaryIndex {} output: valuable_rows={}, total_rows={}, valuable_ratio={}%, threshold={}%",
            task_kind,
            value.valuableRows(),
            value.total_rows,
            value.valuableRatioPercent(),
            threshold_percent);
        return true;
    }

    reason = fmt::format(
        "{} output skipped because valuable_rows={} total_rows={} valuable_ratio={} below threshold {}",
        task_kind,
        value.valuableRows(),
        value.total_rows,
        value.valuableRatioPercent(),
        threshold_percent);
    LOG_WARNING(log, "{}", reason);
    return false;
}

bool StorageANN::shouldCommitRemapOutput(const FutureAuxiliaryIndexPart & future_part, String & reason) const
{
    if (!future_part.delta_in_source_parts.empty())
    {
        auto context = getContext();
        auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, context);
        const auto * source_mt = source_storage ? dynamic_cast<const MergeTreeData *>(source_storage.get()) : nullptr;
        if (!source_mt)
        {
            reason = "Remap output skipped because source table is no longer available";
            return false;
        }

        const auto active_source_uuids = collectPartUuidSet(source_mt->getDataPartsVectorForInternalUsage());
        for (const auto & part : future_part.delta_in_source_parts)
        {
            if (!part || !active_source_uuids.contains(part->uuid))
            {
                reason = "Remap output skipped because its target source part is no longer active";
                return false;
            }
        }
    }

    const auto active_mi_uuids = collectPartUuidSet(getAccessPathPartsVectorForInternalUsage());
    for (const auto & part : future_part.affected_auxiliary_index_parts)
    {
        if (!part || !active_mi_uuids.contains(part->uuid))
        {
            reason = "Remap output skipped because an input materialized-index-part is no longer active";
            return false;
        }
    }

    return true;
}

void StorageANN::setReplicatedLeaderLeaseForNextTask(String lease_path, String lease_payload, StoragePtr inner_table_snapshot)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    pending_replicated_leader_lease_path = std::move(lease_path);
    pending_replicated_leader_lease_payload = std::move(lease_payload);
    pending_replicated_leader_inner_table = std::move(inner_table_snapshot);
}

void StorageANN::releasePendingReplicatedLeaderLease() noexcept
{
    String lease_path;
    String lease_payload;
    StoragePtr inner_table_snapshot;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        lease_path.swap(pending_replicated_leader_lease_path);
        lease_payload.swap(pending_replicated_leader_lease_payload);
        inner_table_snapshot.swap(pending_replicated_leader_inner_table);
    }

    if (lease_path.empty() || lease_payload.empty())
        return;

    if (auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(inner_table_snapshot.get()))
        replicated->releaseAuxiliaryIndexLeaderLease(lease_path, lease_payload);
}

StorageANN::ObservabilitySnapshot StorageANN::getObservabilitySnapshot() const
{
    /// Take the scheduler snapshot under the lock, then drop the lock before
    /// touching parts — `getAccessPathPartsVectorForInternalUsage` re-enters
    /// the same non-recursive mutex through `getInnerTable`, which would
    /// self-deadlock.
    AuxiliaryIndexSchedulerState::ObservabilitySnapshot scheduler_snapshot;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_snapshot = scheduler_state.getObservabilitySnapshot();
    }

    ObservabilitySnapshot snapshot;
    snapshot.backlog_rows = scheduler_snapshot.backlog.rows;
    snapshot.backlog_bytes = scheduler_snapshot.backlog.bytes;
    snapshot.backlog_parts = scheduler_snapshot.backlog.parts;
    snapshot.pending_task_count = scheduler_snapshot.pending_task_count;
    snapshot.ready_auxiliary_index_part_count = scheduler_snapshot.ready_auxiliary_index_part_count;
    snapshot.obsolete_ready_source_count = scheduler_snapshot.obsolete_ready_source_count;
    snapshot.repeated_failure_count = scheduler_snapshot.repeated_failure_count;
    snapshot.retry_count = scheduler_snapshot.retry_count;
    snapshot.next_retry_time = scheduler_snapshot.next_retry_time;
    snapshot.last_error = scheduler_snapshot.last_error;

    try
    {
        const auto active_parts = getAccessPathPartsVectorForInternalUsage();
        UInt64 total_rows = 0;
        for (const auto & part : active_parts)
        {
            if (!part)
                continue;
            total_rows += part->rows_count;
            snapshot.tombstone_rows += readTombstoneRowsFromHeader(part->getDataPartStorage());
        }
        snapshot.tombstone_ratio = total_rows == 0 ? 0.0 : static_cast<double>(snapshot.tombstone_rows) / static_cast<double>(total_rows);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot read AuxiliaryIndex tombstone observability");
    }
    return snapshot;
}

UInt64 StorageANN::getTaskMemoryBudgetBytes() const
{
    return (*getSettings())[MergeTreeSetting::auxiliary_index_task_memory_budget_bytes];
}

UInt64 StorageANN::estimateBuildOutputBytes(UInt64 input_rows, UInt64 input_bytes) const
{
    UInt64 estimate = 0;
    if (algorithm)
        estimate = algorithm->estimateBuildBytes(input_bytes, input_rows);
    if (estimate != 0)
        return estimate;

    const UInt64 ratio_percent = (*getSettings())[MergeTreeSetting::auxiliary_index_size_ratio_percent];
    if (ratio_percent == 0 || input_bytes == 0)
        return input_bytes;
    if (input_bytes > std::numeric_limits<UInt64>::max() / ratio_percent)
        return input_bytes;
    return std::max<UInt64>(1, input_bytes * ratio_percent / 100);
}

void StorageANN::postponeForResourceFailure(const String & reason)
{
    const UInt64 backoff_seconds = (*getSettings())[MergeTreeSetting::auxiliary_index_resource_failure_backoff].totalSeconds();
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.postponeForResourceFailure(reason, std::chrono::seconds(backoff_seconds));
}

void StorageANN::recordTaskFailure(const FutureAuxiliaryIndexPart & future_part, const String & reason)
{
    static constexpr UInt64 MAX_REPEATED_TASK_FAILURES = 10;
    const UInt64 backoff_seconds = (*getSettings())[MergeTreeSetting::auxiliary_index_resource_failure_backoff].totalSeconds();
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.recordTaskFailure(
        makeTaskFailureKey(future_part, family, impl),
        reason,
        std::chrono::seconds(backoff_seconds),
        MAX_REPEATED_TASK_FAILURES);
}

void StorageANN::clearTaskFailure(const FutureAuxiliaryIndexPart & future_part)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.clearTaskFailure(makeTaskFailureKey(future_part, family, impl));
}

bool StorageANN::isTaskFailureBackoffActive(const FutureAuxiliaryIndexPart & future_part)
{
    return scheduler_state.isTaskFailureBackoffActive(makeTaskFailureKey(future_part, family, impl));
}

bool StorageANN::tryAcquireTaskResources(FutureAuxiliaryIndexPart & future_part, UInt64 input_rows, UInt64 input_bytes)
{
    const auto settings = getSettings();
    const UInt64 max_rows = (*settings)[MergeTreeSetting::auxiliary_index_task_max_input_rows];
    const UInt64 max_bytes = (*settings)[MergeTreeSetting::auxiliary_index_task_max_input_bytes];
    if (max_rows != 0 && input_rows > max_rows)
    {
        postponeForResourceFailure(fmt::format("input rows {} exceed limit {}", input_rows, max_rows));
        return false;
    }
    if (max_bytes != 0 && input_bytes > max_bytes)
    {
        postponeForResourceFailure(fmt::format("input bytes {} exceed limit {}", input_bytes, max_bytes));
        return false;
    }

    const UInt64 max_global = (*settings)[MergeTreeSetting::auxiliary_index_max_global_background_tasks];
    const UInt64 max_per_source = (*settings)[MergeTreeSetting::auxiliary_index_max_background_tasks_per_source_table];
    const String source_key = source_table_id.getFullTableName();

    String failure_reason;
    {
        std::lock_guard counters_lock(auxiliary_index_task_counters_mutex);
        if (max_global != 0 && global_auxiliary_index_task_count >= max_global)
            failure_reason = fmt::format("global AuxiliaryIndex task limit {} reached", max_global);
        else
        {
            const UInt64 current_for_source = auxiliary_index_tasks_by_source_table[source_key];
            if (max_per_source != 0 && current_for_source >= max_per_source)
                failure_reason = fmt::format("AuxiliaryIndex task limit {} reached for source table {}", max_per_source, source_key);
            else
            {
                ++global_auxiliary_index_task_count;
                ++auxiliary_index_tasks_by_source_table[source_key];
                future_part.source_table_key = source_key;
                future_part.resource_accounted = true;
            }
        }
    }

    if (!failure_reason.empty())
    {
        postponeForResourceFailure(failure_reason);
        return false;
    }

    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_state.clearResourceBackoff();
    }
    return true;
}

void StorageANN::releaseTaskResources(FutureAuxiliaryIndexPart & future_part) noexcept
{
    if (!future_part.resource_accounted)
        return;

    try
    {
        std::lock_guard lock(auxiliary_index_task_counters_mutex);
        if (global_auxiliary_index_task_count != 0)
            --global_auxiliary_index_task_count;

        auto it = auxiliary_index_tasks_by_source_table.find(future_part.source_table_key);
        if (it != auxiliary_index_tasks_by_source_table.end())
        {
            if (it->second != 0)
                --it->second;
            if (it->second == 0)
                auxiliary_index_tasks_by_source_table.erase(it);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release AuxiliaryIndex resource counters");
    }

    future_part.resource_accounted = false;
}

void StorageANN::rollbackUncommittedTaskReservation(FutureAuxiliaryIndexPart & future_part) noexcept
{
    try
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        if (future_part.scheduler_reserved)
        {
            scheduler_state.releaseTask(future_part.task_id);
            future_part.scheduler_reserved = false;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release AuxiliaryIndex scheduler reservation during rollback");
    }
    releaseReplicatedLeaderLease(future_part);
    releaseReplicatedTaskReservation(future_part);
    releaseTaskResources(future_part);
}

bool StorageANN::tryReserveReplicatedTask(FutureAuxiliaryIndexPart & future_part)
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (!replicated)
        return true;

    String payload = fmt::format(
        "task_id={}\npart={}\nreplica={}\nreservation_id={}\n",
        future_part.task_id,
        future_part.new_part_name,
        replicated->getReplicaName(),
        toString(UUIDHelpers::generateV4()));
    const String key = makeReplicationTaskKey(future_part, family, impl);
    if (replicated->tryReserveAuxiliaryIndexTask(key, payload, future_part.replicated_task_lock_path))
    {
        future_part.replicated_task_lock_payload = std::move(payload);
        return true;
    }

    LOG_TRACE(
        log,
        "Cannot reserve AuxiliaryIndex {} task {} for part {}: Keeper task {} already exists",
        materializedIndexTaskKindName(future_part.kind),
        future_part.task_id,
        future_part.new_part_name,
        key);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
    return false;
}

void StorageANN::releaseReplicatedTaskReservation(FutureAuxiliaryIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (replicated)
        replicated->releaseAuxiliaryIndexTask(future_part.replicated_task_lock_path, future_part.replicated_task_lock_payload);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
}

void StorageANN::releaseReplicatedLeaderLease(FutureAuxiliaryIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (replicated)
        replicated->releaseAuxiliaryIndexLeaderLease(
            future_part.replicated_leader_lease_path,
            future_part.replicated_leader_lease_payload);
    future_part.replicated_leader_lease_path.clear();
    future_part.replicated_leader_lease_payload.clear();
}

void StorageANN::assertReplicatedTaskReservation(const FutureAuxiliaryIndexPart & future_part) const
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (!replicated)
        return;

    replicated->assertAuxiliaryIndexLeaderLease(
        future_part.replicated_leader_lease_path,
        future_part.replicated_leader_lease_payload);
    replicated->assertAuxiliaryIndexTaskReservation(
        future_part.replicated_task_lock_path,
        future_part.replicated_task_lock_payload);
}

void StorageANN::refreshBuildBacklog(
    const DataPartsVector & uncovered_source_parts,
    const std::unordered_set<UUID> & covered_source_uuids)
{
    std::unordered_set<UUID> active_uncovered_uuids;
    active_uncovered_uuids.reserve(uncovered_source_parts.size());

    const auto now = std::chrono::steady_clock::now();
    for (const auto & part : uncovered_source_parts)
    {
        if (!part)
            continue;

        active_uncovered_uuids.insert(part->uuid);
        UncoveredSourceBacklogEntry entry;
        entry.part = part;
        entry.first_seen = now;
        entry.rows = part->rows_count;
        entry.bytes = part->getBytesOnDisk();

        auto [it, inserted] = uncovered_source_backlog.emplace(
            part->uuid,
            std::move(entry));

        if (!inserted)
        {
            it->second.part = part;
            it->second.rows = part->rows_count;
            it->second.bytes = part->getBytesOnDisk();
        }
    }

    for (auto it = uncovered_source_backlog.begin(); it != uncovered_source_backlog.end();)
    {
        if (covered_source_uuids.contains(it->first) || !active_uncovered_uuids.contains(it->first))
            it = uncovered_source_backlog.erase(it);
        else
            ++it;
    }

    AuxiliaryIndexSchedulerState::BacklogStats stats;
    stats.parts = uncovered_source_backlog.size();
    for (const auto & [_, entry] : uncovered_source_backlog)
    {
        stats.rows += entry.rows;
        stats.bytes += entry.bytes;
    }

    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.setBacklogStats(stats);
}

StorageANN::DataPartsVector StorageANN::selectBuildBatchFromBacklog(
    const DataPartsVector & candidate_source_parts,
    bool initial_build,
    bool force_build)
{
    if (candidate_source_parts.empty() || uncovered_source_backlog.empty())
        return {};

    std::vector<BuildBatchCandidateView> views;
    views.reserve(candidate_source_parts.size());
    std::unordered_map<UUID, DataPartPtr> part_by_uuid;
    part_by_uuid.reserve(candidate_source_parts.size());

    for (const auto & part : candidate_source_parts)
    {
        if (!part)
            continue;

        auto it = uncovered_source_backlog.find(part->uuid);
        if (it == uncovered_source_backlog.end())
            continue;

        views.push_back(BuildBatchCandidateView{
            part->uuid,
            part->info.getPartitionId(),
            part->info.min_block,
            part->info.max_block,
            it->second.rows,
            it->second.bytes,
            it->second.first_seen,
        });
        part_by_uuid.emplace(part->uuid, part);
    }

    auto selection = pickContiguousBatchInOldestPartition(std::move(views));
    if (selection.picked_uuids.empty())
        return {};

    DataPartsVector batch;
    batch.reserve(selection.picked_uuids.size());
    for (const auto & uuid : selection.picked_uuids)
        batch.push_back(part_by_uuid.at(uuid));

    if (initial_build || force_build)
        return batch;

    const auto settings = getSettings();
    const UInt64 min_rows = (*settings)[MergeTreeSetting::auxiliary_index_build_min_rows];
    const UInt64 min_bytes = (*settings)[MergeTreeSetting::auxiliary_index_build_min_bytes];
    const UInt64 min_parts = (*settings)[MergeTreeSetting::auxiliary_index_build_min_parts];
    const UInt64 max_delay_seconds = (*settings)[MergeTreeSetting::auxiliary_index_build_max_delay].totalSeconds();

    bool should_build = false;
    should_build |= min_rows != 0 && selection.rows >= min_rows;
    should_build |= min_bytes != 0 && selection.bytes >= min_bytes;
    should_build |= min_parts != 0 && batch.size() >= min_parts;

    if (!should_build && max_delay_seconds != 0 && selection.oldest_first_seen)
    {
        const auto max_delay = std::chrono::seconds(max_delay_seconds);
        should_build = std::chrono::steady_clock::now() - *selection.oldest_first_seen >= max_delay;
    }

    if (!should_build)
        return {};

    return batch;
}

bool StorageANN::tryReserveFuturePart(FutureAuxiliaryIndexPart & future_part)
{
    fiu_do_on(FailPoints::auxiliary_index_throw_in_try_reserve_future_part,
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Failpoint in tryReserveFuturePart");
    });

    if (future_part.task_id.empty())
        future_part.task_id = toString(future_part.new_part_uuid);

    std::lock_guard lock(currently_processing_in_background_mutex);
    if (!inner_table)
        return false;
    future_part.inner_table_snapshot = inner_table;

    if (isTaskFailureBackoffActive(future_part))
    {
        LOG_WARNING(
            log,
            "Postponing AuxiliaryIndex {} task for part {} because the same task failed repeatedly",
            materializedIndexTaskKindName(future_part.kind),
            future_part.new_part_name);
        return false;
    }
    if (currently_building_auxiliary_index_parts.contains(future_part.new_part_name))
    {
        LOG_DEBUG(
            log,
            "Cannot reserve AuxiliaryIndex {} task {} for part {}: part name is already reserved",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }
    if (future_part.kind == FutureAuxiliaryIndexPart::Kind::Build)
    {
        if (scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve AuxiliaryIndex Build task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (future_part.kind == FutureAuxiliaryIndexPart::Kind::Remap)
    {
        if (scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve AuxiliaryIndex Remap task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (scheduler_state.hasActiveTasks())
    {
        LOG_DEBUG(
            log,
            "Cannot reserve AuxiliaryIndex {} task {} for part {}: another task is already active",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }

    bool reserved = false;
    if (future_part.kind == FutureAuxiliaryIndexPart::Kind::Build)
    {
        reserved = scheduler_state.reserveBuildBatch(
            future_part.task_id,
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else if (future_part.kind == FutureAuxiliaryIndexPart::Kind::Compact)
    {
        reserved = scheduler_state.reserveCompactRebuild(
            future_part.task_id,
            collectPartUuids(future_part.affected_auxiliary_index_parts),
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else
    {
        reserved = scheduler_state.reserveRemapLineage(
            future_part.task_id,
            collectPartUuids(future_part.affected_auxiliary_index_parts),
            future_part.delta_out_source_uuids,
            future_part.new_part_uuid);
    }

    if (!reserved)
    {
        LOG_DEBUG(
            log,
            "Cannot reserve AuxiliaryIndex {} task {} for part {}: scheduler state rejected the reservation "
            "(source_parts={}, auxiliary_index_parts={}, delta_in_parts={}, delta_out_sources={})",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name,
            future_part.source_parts_snapshot.size(),
            future_part.affected_auxiliary_index_parts.size(),
            future_part.delta_in_source_parts.size(),
            future_part.delta_out_source_uuids.size());
        return false;
    }

    future_part.scheduler_reserved = true;
    if (!tryReserveReplicatedTask(future_part))
    {
        scheduler_state.releaseTask(future_part.task_id);
        future_part.scheduler_reserved = false;
        future_part.inner_table_snapshot.reset();
        return false;
    }
    future_part.replicated_leader_lease_path.swap(pending_replicated_leader_lease_path);
    future_part.replicated_leader_lease_payload.swap(pending_replicated_leader_lease_payload);
    if (pending_replicated_leader_inner_table)
        future_part.inner_table_snapshot.swap(pending_replicated_leader_inner_table);
    return reserved;
}

bool StorageANN::shouldScheduleCompactRebuild(
    const DataPartsVector & source_snapshot,
    const DataPartsVector & auxiliary_index_snapshot,
    const std::unordered_set<UUID> & covered_source_uuids) const
{
    const UInt64 min_parts = (*getSettings())[MergeTreeSetting::auxiliary_index_compact_min_parts];
    const UInt64 tombstone_ratio_percent = (*getSettings())[MergeTreeSetting::auxiliary_index_compact_tombstone_ratio_percent];
    if ((min_parts == 0 || auxiliary_index_snapshot.size() < min_parts) && tombstone_ratio_percent == 0)
        return false;

    std::vector<PartIdentityView> source_views;
    source_views.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
    {
        if (!part)
            return false;
        source_views.push_back(PartIdentityView{part->uuid, part->info.getPartitionId()});
    }

    std::vector<PartIdentityView> mi_views;
    mi_views.reserve(auxiliary_index_snapshot.size());
    for (const auto & part : auxiliary_index_snapshot)
    {
        if (!part)
            return false;
        mi_views.push_back(PartIdentityView{part->uuid, part->info.getPartitionId()});
    }

    if (!compactRebuildPartitionConditionMet(source_views, mi_views, covered_source_uuids))
        return false;

    if (min_parts != 0 && auxiliary_index_snapshot.size() >= min_parts)
        return true;

    UInt64 total_rows = 0;
    UInt64 tombstone_rows = 0;
    for (const auto & part : auxiliary_index_snapshot)
    {
        total_rows += part->rows_count;
        tombstone_rows += readTombstoneRowsFromHeader(part->getDataPartStorage());
    }

    return total_rows != 0 && tombstone_rows * 100 >= total_rows * tombstone_ratio_percent;
}

bool StorageANN::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return false;

    cleanup_thread.wakeupEarlierIfNeeded();

    auto context = getContext();
    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, context);
    if (!source_storage)
        return false;
    const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
    if (!source_mt)
        return false;

    auto inner_table_snapshot = getInnerTable();
    if (!inner_table_snapshot)
        return false;
    auto & inner = getInnerMergeTreeData(inner_table_snapshot);
    const auto inner_format_version = inner.format_version;

    /// I-BG-14: pull source / auxiliary_index snapshots once per cycle.
    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    auto auxiliary_index_snapshot = inner.getDataPartsVectorForInternalUsage(
        {DataPartState::Active},
        {MergeTreePartInfo::Kind::AuxiliaryIndex});

    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_auxiliary_index_part_uuid;
    std::unordered_set<UUID> active_build_source_uuids;
    bool has_active_tasks = false;
    {
        /// Build and Remap tasks can overlap: Build writes new MI parts while
        /// Remap replaces already-ready MI parts. Remap tasks can also
        /// overlap when their reserved MI/source UUIDs do not intersect.
        /// Compact still stays exclusive because it rewrites a whole ready MI part set.
        std::lock_guard lock(currently_processing_in_background_mutex);
        if (scheduler_state.isResourceBackoffActive()
            || scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(AuxiliaryIndexSchedulerState::TaskKind::CompactMerge))
            return false;

        has_active_tasks = scheduler_state.hasActiveTasks() || !currently_building_auxiliary_index_parts.empty();

        scheduler_state.refreshSources(source_snapshot);
        active_build_source_uuids = scheduler_state.activeBuildSourceUuids();
        coverage_by_source_uuid = coverage_map.coverageEntriesBySourceUuid();
        coverage_by_auxiliary_index_part_uuid = coverage_map.coverageEntriesByMiPartUuid();
    }

    if (!active_build_source_uuids.empty())
    {
        auto & pending_build_entries = coverage_by_auxiliary_index_part_uuid[UUID{}];
        for (const auto & part : source_snapshot)
        {
            if (!part || !active_build_source_uuids.contains(part->uuid) || coverage_by_source_uuid.contains(part->uuid))
                continue;

            CoverageEntry entry;
            entry.source_part_uuid = part->uuid;
            entry.rows = part->rows_count;
            entry.source_part_name = part->name;
            entry.partition_id = part->info.getPartitionId();
            entry.min_block = part->info.min_block;
            entry.max_block = part->info.max_block;
            entry.level = part->info.level;
            entry.mutation = part->info.mutation;
            entry.has_part_info = true;
            coverage_by_source_uuid.emplace(entry.source_part_uuid, entry);
            pending_build_entries.push_back(std::move(entry));
        }
    }

    /// Scheduler coverage view: active MI manifests plus source UUIDs reserved
    /// by running Build tasks. Pending Build coverage is synthetic here so
    /// Remap can proceed concurrently; query-time coverage still comes only
    /// from committed `coverage.json` manifests.
    std::unordered_set<UUID> coverage;
    coverage.reserve(coverage_by_source_uuid.size());
    for (const auto & [uuid, _] : coverage_by_source_uuid)
        coverage.insert(uuid);

    auto reconciled = SnapshotDiffReconciler::run(source_snapshot, auxiliary_index_snapshot, coverage_by_auxiliary_index_part_uuid);
    auto build_candidates = reconciled.build_batch.source_parts;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        build_candidates.erase(
            std::remove_if(
                build_candidates.begin(),
                build_candidates.end(),
                [&](const auto & part)
                {
                    return !part || scheduler_state.isSourceReserved(part->uuid);
                }),
            build_candidates.end());
    }
    refreshBuildBacklog(build_candidates, coverage);

    const size_t starvation_threshold
        = (*getSettings())[MergeTreeSetting::auxiliary_index_starvation_protection_cycles];
    const bool force_build
        = starvation_threshold != 0
        && consecutive_remap_count.load(std::memory_order_relaxed) >= starvation_threshold
        && reconciled.candidate_kind == ReconcileCandidateKind::BuildBatch;
    const bool initial_build = auxiliary_index_snapshot.empty() && !source_snapshot.empty();
    auto build_batch = selectBuildBatchFromBacklog(
        build_candidates,
        initial_build,
        force_build);
    const bool compact_rebuild_candidate
        = !has_active_tasks
        && reconciled.candidate_kind == ReconcileCandidateKind::Nothing
        && shouldScheduleCompactRebuild(source_snapshot, auxiliary_index_snapshot, coverage);
    auto decision = AuxiliaryIndexSchedulerPolicy::choose(
        reconciled,
        std::move(build_batch),
        compact_rebuild_candidate,
        source_snapshot,
        auxiliary_index_snapshot);
    const bool compact_decision = decision.kind == AuxiliaryIndexSchedulerDecisionKind::RebuildSourcePart
        || decision.kind == AuxiliaryIndexSchedulerDecisionKind::CompactRebuild
        || decision.kind == AuxiliaryIndexSchedulerDecisionKind::CompactMerge;
    if (has_active_tasks && compact_decision)
        return false;

    auto build_callback = [](bool /*delay*/) {};
    auto metadata_snapshot = getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
    auto storage_snapshot
        = source_mt->getStorageSnapshotWithoutData(
            source_mt->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false), context);

    auto submit_build = [&](DataPartsVector selected_source_parts)
    {
        auto fp = std::make_shared<FutureAuxiliaryIndexPart>();
        fp->kind = FutureAuxiliaryIndexPart::Kind::Build;
        fp->new_part_name = makeAuxiliaryIndexBuildPartNameFromSourceParts(
            selected_source_parts,
            inner_format_version);
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);
        fp->source_parts_snapshot = selected_source_parts;

        const auto [input_rows, input_bytes] = sumRowsAndBytes(selected_source_parts);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        bool reservation_committed = false;
        SCOPE_EXIT_SAFE({
            if (reservation_committed)
                return;
            rollbackUncommittedTaskReservation(*fp);
        });

        if (!tryReserveFuturePart(*fp))
            return false;

        auto tagger = std::make_unique<CurrentlyBuildingAuxiliaryIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<AuxiliaryIndexBuildSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        consecutive_remap_count.store(0, std::memory_order_relaxed);

        auto task = std::make_shared<AuxiliaryIndexBuildTask>(
            *this,
            shared_from_this(),
            source_storage,
            std::move(entry),
            selected_source_parts,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            estimateBuildOutputBytes(input_rows, input_bytes),
            build_callback);

        const bool scheduled = assignee.scheduleCommonTask(task, /*need_trigger=*/true);
        if (scheduled)
        {
            for (const auto & part : selected_source_parts)
            {
                if (part)
                    uncovered_source_backlog.erase(part->uuid);
            }
        }
        return scheduled;
    };

    auto submit_remap = [&](
        AuxiliaryIndexRemapKind remap_kind,
        DataPartsVector affected_auxiliary_index_parts,
        DataPartsVector delta_in_source_parts,
        std::vector<UUID> delta_out_source_uuids)
    {
        auto fp = std::make_shared<FutureAuxiliaryIndexPart>();
        fp->kind = FutureAuxiliaryIndexPart::Kind::Remap;
        fp->remap_kind = remap_kind;
        /// MergeLineage / MutationLineage runs as N→N: each retired MI part is
        /// replaced by its own `bumpLevelInPartName(old)` output (see
        /// `RemapTask::PlanAffectedSegmentsStage`). The compact-style merged
        /// part name from `makeAuxiliaryIndexCompactPartName` would describe
        /// a single output covering the union range — the wrong shape for N→N
        /// commit, where each new part covers exactly one old. Use the first
        /// affected part as the placeholder name; per-output names come from
        /// the task's stage 1 and are committed individually.
        if (affected_auxiliary_index_parts.size() == 1)
        {
            fp->new_part_name = makeAuxiliaryIndexCompactPartName(
                affected_auxiliary_index_parts,
                inner_format_version);
        }
        else
        {
            auto first_info = MergeTreePartInfo::fromPartName(
                affected_auxiliary_index_parts.front()->name,
                inner_format_version);
            first_info.level += 1;
            fp->new_part_name = first_info.getPartNameAndCheckFormat(inner_format_version);
        }
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);
        fp->affected_auxiliary_index_parts = affected_auxiliary_index_parts;
        fp->delta_in_source_parts = delta_in_source_parts;
        fp->delta_out_source_uuids = delta_out_source_uuids;

        const auto [input_rows, input_bytes] = sumRowsAndBytes(fp->affected_auxiliary_index_parts);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        bool reservation_committed = false;
        SCOPE_EXIT_SAFE({
            if (reservation_committed)
                return;
            rollbackUncommittedTaskReservation(*fp);
        });

        if (!tryReserveFuturePart(*fp))
            return false;

        auto tagger = std::make_unique<CurrentlyBuildingAuxiliaryIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<AuxiliaryIndexRemapSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        consecutive_remap_count.fetch_add(1, std::memory_order_relaxed);

        auto task = std::make_shared<AuxiliaryIndexRemapTask>(
            *this,
            shared_from_this(),
            source_storage,
            std::move(entry),
            fp->affected_auxiliary_index_parts,
            fp->delta_in_source_parts,
            fp->delta_out_source_uuids,
            fp->remap_kind,
            source_mt,
            storage_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    auto submit_compact = [&](DataPartsVector selected_source_parts, DataPartsVector affected_auxiliary_index_parts)
    {
        /// Defensive: a compact requires at least one materialized-index part to
        /// fold into the new compact name. Upstream paths (Reconciler /
        /// SchedulerPolicy) should never hand us an empty set, but if they ever
        /// do, decline rather than tripping `makeAuxiliaryIndexCompactPartName`'s
        /// LOGICAL_ERROR contract and aborting the server.
        if (affected_auxiliary_index_parts.empty())
            return false;

        auto fp = std::make_shared<FutureAuxiliaryIndexPart>();
        fp->kind = FutureAuxiliaryIndexPart::Kind::Compact;
        fp->source_parts_snapshot = selected_source_parts;
        fp->affected_auxiliary_index_parts = affected_auxiliary_index_parts;
        fp->new_part_name = makeAuxiliaryIndexCompactPartName(
            fp->affected_auxiliary_index_parts,
            inner_format_version);
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);

        const auto [input_rows, input_bytes] = sumRowsAndBytes(fp->source_parts_snapshot);
        if (!tryAcquireTaskResources(*fp, input_rows, input_bytes))
            return false;

        bool reservation_committed = false;
        SCOPE_EXIT_SAFE({
            if (reservation_committed)
                return;
            rollbackUncommittedTaskReservation(*fp);
        });

        if (!tryReserveFuturePart(*fp))
            return false;

        auto tagger = std::make_unique<CurrentlyBuildingAuxiliaryIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<AuxiliaryIndexBuildSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        auto task = std::make_shared<AuxiliaryIndexCompactTask>(
            *this,
            shared_from_this(),
            source_storage,
            std::move(entry),
            fp->source_parts_snapshot,
            fp->affected_auxiliary_index_parts,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            estimateBuildOutputBytes(input_rows, input_bytes),
            build_callback);

        return assignee.scheduleCommonTask(task, /*need_trigger=*/true);
    };

    switch (decision.kind)
    {
        case AuxiliaryIndexSchedulerDecisionKind::BuildBatch:
            return submit_build(std::move(decision.source_parts));
        case AuxiliaryIndexSchedulerDecisionKind::RemapLineage:
        case AuxiliaryIndexSchedulerDecisionKind::ObsoleteCoverageCleanup:
            return submit_remap(
                decision.remap_kind,
                std::move(decision.auxiliary_index_parts),
                std::move(decision.source_parts),
                std::move(decision.delta_out_source_uuids));
        case AuxiliaryIndexSchedulerDecisionKind::RebuildSourcePart:
        case AuxiliaryIndexSchedulerDecisionKind::CompactRebuild:
        case AuxiliaryIndexSchedulerDecisionKind::CompactMerge:
            return submit_compact(
                std::move(decision.source_parts),
                std::move(decision.auxiliary_index_parts));
        case AuxiliaryIndexSchedulerDecisionKind::Nothing:
            return false;
    }

    return false;
}

bool StorageANN::partIsAssignedToBackgroundOperation(const DataPartPtr & part) const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return currently_building_auxiliary_index_parts.contains(part->name);
}

DataPartsVector StorageANN::getAccessPathPartsVectorForInternalUsage() const
{
    auto inner_table_snapshot = getInnerTable();
    /// `system.auxiliary_indexes` enumerates every MI storage on the server,
    /// so a concurrent `DROP` on an unrelated MI can race here: by the time
    /// the iterator reaches that storage, `dropInnerTableIfAny` has already
    /// reset `inner_table`. Treat that transient state as "no active parts"
    /// rather than raising LOGICAL_ERROR — both observability queries and
    /// scheduler internals already handle an empty vector correctly, and
    /// raising would otherwise pollute stderr with benign `<Error>` lines
    /// that fail the stateless test stderr check.
    if (!inner_table_snapshot)
        return {};
    return getInnerMergeTreeData(inner_table_snapshot).getDataPartsVectorForInternalUsage(
        {DataPartState::Active},
        {MergeTreePartInfo::Kind::AuxiliaryIndex});
}

void StorageANN::read(
    QueryPlan & /*query_plan*/,
    const Names & /*column_names*/,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & /*query_info*/,
    ContextPtr /*context*/,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct SELECT from a AuxiliaryIndex table is not supported");
}

SinkToStoragePtr StorageANN::write(const ASTPtr & /*query*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*context*/, bool /*async_insert*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Direct INSERT into a AuxiliaryIndex table is not supported");
}

StorageANN::MutationsSnapshotPtr StorageANN::getMutationsSnapshot(const IMutationsSnapshot::Params & params) const
{
    // An empty snapshot is enough: stage-1 never produces mutations for a
    // AuxiliaryIndex. The concrete EmptyMutationsSnapshot subclass below
    // stubs the three pure-virtual methods with empty returns.
    return std::make_shared<EmptyMutationsSnapshot>(params, MutationCounters{}, DataPartsVector{});
}

void StorageANN::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const Settings & settings,
    ContextPtr query_context) const
{
    auto inner_storage_table = getInnerTable();
    if (!inner_storage_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "AuxiliaryIndex {} does not have an inner table", getStorageID().getNameForLogs());

    PartitionCommands inner_commands;
    inner_commands.reserve(commands.size());
    for (const auto & command : commands)
        inner_commands.push_back(mapAuxiliaryIndexPartitionCommandForInner(*this, command, query_context));

    const auto & inner = getInnerMergeTreeData(inner_storage_table);
    auto inner_metadata = inner.getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    inner_storage_table->checkAlterPartitionIsPossible(inner_commands, inner_metadata, settings, query_context);
}

Pipe StorageANN::alterPartition(
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const PartitionCommands & commands,
    ContextPtr query_context)
{
    waitForOutdatedPartsToBeLoaded();
    auto inner_table_snapshot = getInnerTable();
    getInnerMergeTreeData(inner_table_snapshot).waitForOutdatedPartsToBeLoaded();

    for (const auto & command : commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
            {
                if (command.part)
                    dropPart(getAuxiliaryIndexPartNameFromAST(command.partition), command.detach, query_context);
                else
                    dropPartition(command.partition, command.detach, query_context);
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "{} is not supported for AuxiliaryIndex yet",
                    command.typeToString());
        }
    }

    return {};
}

size_t StorageANN::clearEmptyParts()
{
    /// `MergeTreeCleanupThread` calls `clearEmptyParts` on the catalog-shell
    /// `MergeTreeData` object. Active materialized-index-parts are owned by the
    /// inner MergeTree, so delegate housekeeping there.
    auto inner_table_snapshot = getInnerTable();
    if (!inner_table_snapshot)
        return 0;
    return getInnerMergeTreeData(inner_table_snapshot).clearEmptyParts();
}

size_t StorageANN::clearUnusedPatchParts()
{
    auto inner_table_snapshot = getInnerTable();
    if (!inner_table_snapshot)
        return 0;
    return getInnerMergeTreeData(inner_table_snapshot).clearUnusedPatchParts();
}

void StorageANN::dropPartNoWaitNoThrow(const String & part_name)
{
    /// This hook is used by `MergeTreeData::clearEmptyParts` and similar
    /// background cleanup (fire-and-forget, must not throw). It must not be
    /// confused with `dropPart`, which runs `ALTER TABLE ... DROP PART` on the
    /// inner MergeTree and refreshes `coverage_map`.
    ///
    /// The catalog-shell `MergeTreeData` for a AuxiliaryIndex normally has
    /// no data parts; when a name matches an active inner materialized-index-part,
    /// forward to the inner table so cleanup can actually remove it.
    auto inner_table_snapshot = getInnerTable();
    if (!inner_table_snapshot)
    {
        LOG_DEBUG(
            log,
            "Ignoring no-wait drop request for AuxiliaryIndex part {}: inner table is not loaded",
            part_name);
        return;
    }

    auto & inner = getInnerMergeTreeData(inner_table_snapshot);
    if (inner.getPartIfExists(part_name, {DataPartState::Active}))
    {
        inner.dropPartNoWaitNoThrow(part_name);
        return;
    }

    LOG_DEBUG(
        log,
        "Ignoring no-wait drop request for AuxiliaryIndex catalog-shell part {}",
        part_name);
}

void StorageANN::dropPart(const String & part_name, bool detach, ContextPtr query_context)
{
    /// User-facing DROP PART: route to the inner MergeTree and refresh coverage.
    PartitionCommand command;
    command.type = PartitionCommand::DROP_PARTITION;
    command.partition = make_intrusive<ASTLiteral>(part_name);
    command.detach = detach;
    command.part = true;
    executeInnerPartitionCommand(*this, std::move(command), query_context);
    refreshCoverageFromActiveParts();
}

void StorageANN::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    PartitionCommand command;
    command.type = PartitionCommand::DROP_PARTITION;
    command.partition = mapAuxiliaryIndexPartitionForInner(*this, partition, query_context);
    command.detach = detach;
    executeInnerPartitionCommand(*this, std::move(command), query_context);
    refreshCoverageFromActiveParts();
}

PartitionCommandsResultInfo StorageANN::attachPartition(const PartitionCommand & /*command*/, const StorageMetadataPtr & /*metadata_snapshot*/, ContextPtr /*query_context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "ATTACH PARTITION is not supported for AuxiliaryIndex yet");
}

void StorageANN::replacePartitionFrom(const StoragePtr & /*source_table*/, const ASTPtr & /*partition*/, bool /*replace*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "REPLACE PARTITION is not supported for AuxiliaryIndex yet");
}

void StorageANN::movePartitionToTable(const StoragePtr & /*dest_table*/, const ASTPtr & /*partition*/, ContextPtr /*context*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "MOVE PARTITION is not supported for AuxiliaryIndex yet");
}

void StorageANN::attachRestoredParts(MutableDataPartsVector && /*parts*/)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Restoring parts is not supported for AuxiliaryIndex yet");
}

std::unique_ptr<MergeTreeSettings> StorageANN::getDefaultSettings() const
{
    return std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
}

std::vector<CoverageEntry> StorageANN::parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part)
{
    const auto & part_storage = part.getDataPartStorage();
    if (!part_storage.existsFile("coverage.json"))
        return {};

    auto reader = part_storage.readFile("coverage.json", ReadSettings{}, /*read_hint=*/std::nullopt);
    String body;
    readStringUntilEOF(body, *reader);

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed;
    try
    {
        parsed = parser.parse(body);
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Failed to parse coverage.json for materialized-index-part {}: {}", part.name, e.displayText());
    }

    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "coverage.json for materialized-index-part {} is not a JSON object", part.name);

    if (root->getValue<int>("format_version") != 1)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "Unsupported coverage.json format_version for materialized-index-part {}", part.name);

    std::optional<String> root_source_partition_id;
    if (root->has("source_partition_id"))
        root_source_partition_id = root->getValue<std::string>("source_partition_id");

    std::vector<CoverageEntry> result;
    auto covered = root->getArray("covered");
    if (!covered)
        return result;

    result.reserve(covered->size());
    for (size_t i = 0; i < covered->size(); ++i)
    {
        auto item = covered->getObject(static_cast<unsigned int>(i));
        if (!item)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "coverage.json[{}] for materialized-index-part {} is not an object", i, part.name);

        const auto uuid_text = item->getValue<std::string>("source_part_uuid");
        UUID uuid;
        if (!tryParse(uuid, uuid_text))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "coverage.json[{}].source_part_uuid is not a valid UUID for materialized-index-part {}", i, part.name);

        CoverageEntry entry;
        entry.source_part_uuid = uuid;
        entry.rows = static_cast<UInt64>(item->getValue<Int64>("rows"));
        if (item->has("source_part_name"))
            entry.source_part_name = item->getValue<std::string>("source_part_name");

        if (item->has("partition_id")
            && item->has("min_block")
            && item->has("max_block")
            && item->has("level")
            && item->has("mutation"))
        {
            entry.partition_id = item->getValue<std::string>("partition_id");
            entry.min_block = item->getValue<Int64>("min_block");
            entry.max_block = item->getValue<Int64>("max_block");
            entry.level = item->getValue<UInt32>("level");
            entry.mutation = item->getValue<Int64>("mutation");
            entry.has_part_info = true;

            if (root_source_partition_id && entry.partition_id != *root_source_partition_id)
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA,
                    "coverage.json[{}].partition_id={} differs from root source_partition_id={} for materialized-index-part {}",
                    i,
                    entry.partition_id,
                    *root_source_partition_id,
                    part.name);
        }

        result.push_back(std::move(entry));
    }
    return result;
}

bool StorageANN::waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr query_context)
{
    refreshCoverageFromActiveParts();

    auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, query_context);
    if (!source_storage)
        return false;

    const auto * source_mt = dynamic_cast<const MergeTreeData *>(source_storage.get());
    if (!source_mt)
        return false;

    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    std::unordered_set<UUID> active_uuids;
    active_uuids.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
    {
        if (part)
            active_uuids.insert(part->uuid);
    }

    const auto deadline = std::chrono::steady_clock::now() + timeout;
    constexpr auto refresh_interval = std::chrono::seconds{1};
    while (true)
    {
        if (coverage_map.isFullyCovering(active_uuids))
            return true;

        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
            return false;

        const auto wait_time = std::min(
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now),
            std::chrono::duration_cast<std::chrono::milliseconds>(refresh_interval));
        if (coverage_map.waitForFullCoverage(active_uuids, wait_time))
            return true;

        refreshCoverageFromActiveParts();
    }
}

}
