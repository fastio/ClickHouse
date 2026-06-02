#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>
#include <Storages/Reflection/ANNIndex/ANNIndexBuildTask.h>
#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexCompactTask.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartitionScheduling.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSchedulerPolicy.h>
#include <Storages/Reflection/ANNIndex/ANNIndexSelectedEntry.h>
#include <Storages/Reflection/ANNIndex/ANNIndexRemapTask.h>
#include <Storages/Reflection/ANNIndex/SnapshotDiffReconciler.h>

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
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Core/Defines.h>
#include <Common/SettingsChanges.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Storages/AlterCommands.h>
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
    extern const MergeTreeSettingsUInt64 ann_index_build_min_bytes;
    extern const MergeTreeSettingsUInt64 ann_index_build_min_parts;
    extern const MergeTreeSettingsUInt64 ann_index_build_min_rows;
    extern const MergeTreeSettingsSeconds ann_index_build_max_delay;
    extern const MergeTreeSettingsUInt64 ann_index_compact_min_parts;
    extern const MergeTreeSettingsUInt64 ann_index_compact_tombstone_ratio_percent;
    extern const MergeTreeSettingsUInt64 ann_index_commit_min_valuable_rows_ratio_percent;
    extern const MergeTreeSettingsUInt64 ann_index_max_background_tasks_per_source_table;
    extern const MergeTreeSettingsUInt64 ann_index_max_global_background_tasks;
    extern const MergeTreeSettingsSeconds ann_index_resource_failure_backoff;
    extern const MergeTreeSettingsUInt64 ann_index_size_ratio_percent;
    extern const MergeTreeSettingsUInt64 ann_index_starvation_protection_cycles;
    extern const MergeTreeSettingsUInt64 ann_index_sync_timeout;
    extern const MergeTreeSettingsUInt64 ann_index_task_max_input_bytes;
    extern const MergeTreeSettingsUInt64 ann_index_task_max_input_rows;
    extern const MergeTreeSettingsUInt64 ann_index_task_memory_budget_bytes;
}

namespace FailPoints
{
    extern const char ann_index_throw_in_try_reserve_future_part[];
    extern const char ann_index_build_pause_in_finish[];
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}


namespace
{

String makeANNIndexCompactPartName(
    const MergeTreeData::DataPartsVector & ann_index_parts,
    MergeTreeDataFormatVersion format_version)
{
    std::vector<MergeTreePartInfo> part_infos;
    part_infos.reserve(ann_index_parts.size());
    for (const auto & part : ann_index_parts)
    {
        if (!part)
            continue;
        part_infos.push_back(part->info);
    }
    return makeANNIndexCompactPartNameFromInfos(part_infos, format_version);
}

String getANNIndexPartNameFromAST(const ASTPtr & partition)
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

ASTPtr mapANNIndexPartitionForInner(
    const ReflectionANNIndex & storage,
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
        /// Route through the inner MergeTree: it owns the partition key
        /// schema. The outer Reflection is no longer a `MergeTreeData`.
        auto inner_storage = storage.getInnerTable();
        if (!inner_storage)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "ANNIndex {} has no inner table for partition id resolution",
                storage.getStorageID().getNameForLogs());
        partition_id = storage.getInnerMergeTreeData(inner_storage).getPartitionIDFromQuery(partition, context);
        return makePartitionIdAst(partition_id);
    }
    else
    {
        auto source_storage = DatabaseCatalog::instance().tryGetTable(storage.getSourceTableID(), context);
        const auto * source_mt = source_storage ? dynamic_cast<const MergeTreeData *>(source_storage.get()) : nullptr;
        if (!source_mt)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot resolve source partition for ANNIndex {} because source table {} is not available as MergeTree",
                storage.getStorageID().getNameForLogs(),
                storage.getSourceTableID().getNameForLogs());
        partition_id = source_mt->getPartitionIDFromQuery(partition, context);
    }

    return makePartitionIdAst(getANNIndexPhysicalPartitionId(partition_id));
}

void executeInnerPartitionCommand(
    const ReflectionANNIndex & storage,
    PartitionCommand command,
    ContextPtr query_context)
{
    auto inner_storage_table = storage.getInnerTable();
    if (!inner_storage_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex {} does not have an inner table", storage.getStorageID().getNameForLogs());

    PartitionCommands commands{std::move(command)};
    auto & inner = storage.getInnerMergeTreeData(inner_storage_table);
    auto inner_metadata = inner.getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    inner_storage_table->alterPartition(inner_metadata, commands, query_context);
}

PartitionCommand mapANNIndexPartitionCommandForInner(
    const ReflectionANNIndex & storage,
    const PartitionCommand & command,
    ContextPtr query_context)
{
    if (command.type != PartitionCommand::DROP_PARTITION)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "{} is not supported for ANNIndex yet",
            command.typeToString());

    PartitionCommand inner_command = command;
    if (!command.part)
        inner_command.partition = mapANNIndexPartitionForInner(storage, command.partition, query_context);
    return inner_command;
}

/// Minimal concrete subclass of MutationsSnapshotBase so stage-1 can return
/// an "empty, read-only" snapshot without depending on StorageMergeTree's
/// private MutationsSnapshot layout. The three pure-virtual methods answer
/// with empties because ANNIndex never produces mutations in
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

String makeReplicationTaskKey(const FutureANNIndexPart & future_part, const String & family, const String & impl)
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
    update_part_vector(future_part.affected_ann_index_parts);
    update_part_vector(future_part.delta_in_source_parts);

    auto delta_out = future_part.delta_out_source_uuids;
    std::sort(delta_out.begin(), delta_out.end());
    for (const auto & uuid : delta_out)
        updateHashWithUuid(hash, uuid);

    return getSipHash128AsHexString(hash);
}

std::string_view materializedIndexTaskKindName(FutureANNIndexPart::Kind kind)
{
    switch (kind)
    {
        case FutureANNIndexPart::Kind::Build:
            return "Build";
        case FutureANNIndexPart::Kind::Remap:
            return "Remap";
        case FutureANNIndexPart::Kind::Compact:
            return "Compact";
    }
    UNREACHABLE();
}

String makeTaskFailureKey(const FutureANNIndexPart & future_part, const String & family, const String & impl)
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

std::mutex ann_index_task_counters_mutex;
UInt64 global_ann_index_task_count = 0;
std::unordered_map<String, UInt64> ann_index_tasks_by_source_table;

ASTPtr makeInnerColumnList()
{
    auto columns = make_intrusive<ASTColumns>();
    auto column_list = make_intrusive<ASTExpressionList>();

    auto add_column = [&](const String & name, const String & type, std::string_view codec_str)
    {
        auto col = make_intrusive<ASTColumnDeclaration>();
        col->name = name;
        col->setType(make_intrusive<ASTIdentifier>(type));
        if (!codec_str.empty())
        {
            /// `ParserCodec` expects the parenthesised argument list only (e.g.
            /// `(ZSTD)`), NOT the leading `CODEC` keyword — it synthesises the
            /// `CODEC(...)` function node itself.
            ParserCodec codec_parser;
            auto codec_ast = parseQuery(
                codec_parser,
                codec_str.data(),
                codec_str.data() + codec_str.size(),
                "ANN locator column codec",
                /*max_query_size=*/0,
                DBMS_DEFAULT_MAX_PARSER_DEPTH,
                DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
            col->setCodec(std::move(codec_ast));
        }
        column_list->children.push_back(col);
    };

    /// Partition column: an ANN-index part covers exactly one source partition.
    /// `PARTITION BY _source_partition_id` (see makeInnerStorageDefinition) makes
    /// the standard MergeTree write path emit partition.dat / minmax for free.
    add_column(ANN_INDEX_SOURCE_PARTITION_ID_COLUMN, "String", "");

    /// 4-column row locator. The ANN-index part is a standard Wide MergeTree
    /// part whose algorithm-internal id (= part-local row number, the part is
    /// unsorted) maps back to a source-table row through these columns.
    add_column(ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN, "UUID", "(ZSTD)");
    add_column(ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN, "UInt64", "(DoubleDelta, LZ4)");
    add_column(ANN_INDEX_LOCATOR_BLOCK_NUMBER_COLUMN, "UInt64", "(DoubleDelta, LZ4)");
    add_column(ANN_INDEX_LOCATOR_BLOCK_OFFSET_COLUMN, "UInt64", "(DoubleDelta, LZ4)");

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
    storage->set(storage->partition_by, make_intrusive<ASTIdentifier>(ANN_INDEX_SOURCE_PARTITION_ID_COLUMN));
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


String makeANNIndexReplicationTaskKeyForTest(
    const FutureANNIndexPart & future_part,
    const String & family,
    const String & impl)
{
    return makeReplicationTaskKey(future_part, family, impl);
}

ReflectionANNIndex::ReflectionANNIndex(
    const StorageID & table_id_,
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
    : IReflection(table_id_, std::make_unique<StorageInMemoryMetadata>(metadata_))
    , WithMutableContext(context_)
    , source_table_id(source_table_id_)
    , indexed_columns(indexed_columns_)
    , family(family_)
    , impl(impl_)
    , build_params(build_params_)
    , inner_zookeeper_path(inner_zookeeper_path_)
    , inner_replica_name(inner_replica_name_)
    , ann_settings(std::move(settings_))
    , background_operations_assignee(*this, table_id_, BackgroundJobsAssignee::Type::DataProcessing, context_)
    , matcher(*this)
    , scheduler(*this)
    , log(getLogger("ReflectionANNIndex (" + table_id_.getNameForLogs() + ")"))
{
    initializeInnerTable(mode);

    ANNIndexContext ctx;
    ctx.source_table_id = source_table_id;
    ctx.indexed_columns = indexed_columns;
    ctx.family = family;
    ctx.impl = impl;
    ctx.query_context = context_;
    ctx.reflection_settings = ann_settings;

    algorithm = ANNAlgorithmFactory::instance().get(family, impl, build_params, ctx);
    if (algorithm)
        algorithm->initialize(ctx);
}

String ReflectionANNIndex::generateInnerTableName(const StorageID & index_id)
{
    if (index_id.hasUUID())
        return ".inner_id." + toString(index_id.uuid);
    return ".inner." + index_id.getTableName();
}

void ReflectionANNIndex::initializeInnerTable(LoadingStrictnessLevel mode)
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
            "Inner table {} for ANNIndex {} is not loaded",
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
            "Inner table {} for ANNIndex {} is not MergeTree-family storage",
            inner_id.getNameForLogs(),
            index_id.getNameForLogs());

    /// All parts stored in this inner table are ANNIndex parts
    /// (`MergeTreeDataPartBuilder::build` stamps them with
    /// `Kind::ANNIndex`). Tell `MergeTreeData` so that lookups by
    /// `part_name` / `partition_id` string — which cannot recover the kind
    /// from the string — produce keys whose kind matches the stored parts.
    /// Without this, ALTER ... DROP PART / DROP PARTITION on the inner table
    /// transparently miss every part. See `parsePartName`,
    /// `makePartitionID`, `makeDataPartStateAndPartitionID` in MergeTreeData.h.
    inner_data->default_part_kind_for_name_lookup = MergeTreePartInfo::Kind::ANNIndex;
}

StoragePtr ReflectionANNIndex::getInnerTable() const
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    return inner_table;
}

MergeTreeData & ReflectionANNIndex::getInnerMergeTreeData(const StoragePtr & inner_table_snapshot) const
{
    auto * inner_data = dynamic_cast<MergeTreeData *>(inner_table_snapshot.get());
    if (!inner_data)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex {} has no inner MergeTree storage", getStorageID().getNameForLogs());
    return *inner_data;
}


void ReflectionANNIndex::startup()
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
                "Source table {} no longer has assign_part_uuids = 1; ANNIndex {} will be degraded.",
                source_table_id.getNameForLogs(),
                getStorageID().getNameForLogs());
    }

    auto inner_table_snapshot = getInnerTable();
    auto & inner = getInnerMergeTreeData(inner_table_snapshot);
    inner.clearOldTemporaryDirectories(
        /*custom_directories_lifetime_seconds=*/0,
        {"tmp_ann_index_build_", "tmp_ann_index_remap_"});

    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_state.markRunningTasksAsFailed("interrupted by server restart");
    }

    refreshCoverageFromActiveParts();
    try
    {
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

void ReflectionANNIndex::refreshCoverageFromActiveParts()
{
    /// Walk every active materialized-index-part and ingest its `coverage.json` manifest
    /// into the in-memory `CoverageMap`. Startup and replicated followers
    /// call this so the reconciler does not re-trigger Build / Remap for parts
    /// that already fully cover the source. A malformed manifest is logged but
    /// does not abort refresh — `cleanup_thread` will eventually GC truly
    /// broken parts; until then the reconciler may schedule extra work.
    std::vector<std::pair<UUID, std::vector<CoverageEntry>>> snapshot;
    auto ann_index_parts = getAccessPathPartsVectorForInternalUsage();
    snapshot.reserve(ann_index_parts.size());
    for (const auto & part : ann_index_parts)
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
                    /// level here would make every DROP query on a ANNIndex
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

void ReflectionANNIndex::shutdown(bool is_drop)
{
    if (shutdown_called.exchange(true))
        return;
    /// Wake any background task currently blocked at a test-only pause hook
    /// (e.g. `ann_index_build_pause_in_finish`). Without this,
    /// `DROP TABLE ... SYNC` would deadlock in `waitTableFinallyDropped`
    /// because the paused task still holds a `StoragePtr` to this storage.
    /// The task itself observes `isShuttingDown()` after the pause returns
    /// and aborts cleanly via its scope-guard cleanup path.
    try
    {
        FailPointInjection::notifyFailPoint(FailPoints::ann_index_build_pause_in_finish);
    }
    catch (...) // NOLINT(bugprone-empty-catch): expected when failpoint is not enabled
    {
    }
    background_operations_assignee.finish();
    if (is_drop)
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        coverage_map.clear();
        scheduler_state.clear();
    }
}

void ReflectionANNIndex::renameInMemory(const StorageID & new_table_id)
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
                renamed_inner_data->default_part_kind_for_name_lookup = MergeTreePartInfo::Kind::ANNIndex;
            {
                std::lock_guard lock(currently_processing_in_background_mutex);
                if (inner_table == inner_table_snapshot)
                    inner_table = std::move(renamed_inner_table);
            }
        }
    }

    IStorage::renameInMemory(new_table_id);
    background_operations_assignee.updateStorageID(new_table_id);
}

void ReflectionANNIndex::checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const
{
    const auto & build_affecting_settings = getANNIndexBuildAffectingSettings();
    auto check_setting = [&](std::string_view name)
    {
        if (!build_affecting_settings.contains(name))
            return;

        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Setting {} cannot be altered: it requires rebuilding the ANNIndex. "
            "Drop and recreate the reflection instead.",
            name);
    };

    for (const auto & command : commands)
    {
        if (command.type == AlterCommand::MODIFY_QUERY || command.type == AlterCommand::MODIFY_REFRESH)
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Alter of type '{}' is not supported by storage {}",
                command.type,
                getName());

        if (command.type == AlterCommand::MODIFY_SETTING)
        {
            for (const auto & change : command.settings_changes)
                check_setting(change.name);
        }
        else if (command.type == AlterCommand::RESET_SETTING)
        {
            for (const auto & name : command.settings_resets)
                check_setting(name);
        }
    }

    /// Defer column / structure validation to the inner MergeTree: it owns the
    /// real schema and applies the same MergeTree-family rules.
    if (auto inner = getInnerTable())
        inner->checkAlterIsPossible(commands, local_context);
}

void ReflectionANNIndex::drop()
{
    dropInnerTableIfAny(/*sync=*/false, getContext());
}

void ReflectionANNIndex::dropInnerTableIfAny(bool sync, ContextPtr local_context)
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
            replicated->releaseANNIndexLeaderLease(pending_lease_path, pending_lease_payload);
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

/// backupData / restoreDataFromBackup / supportsBackupPartition: handled by
/// `IReflection` defaults — back up the source table; the Reflection rebuilds.

void ReflectionANNIndex::recordBuildCommit(UUID ann_index_part_uuid, const std::vector<CoverageEntry> & entries)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.appendFromBuild(ann_index_part_uuid, entries);
    scheduler_state.appendReadyCoverage(ann_index_part_uuid, entries);
}

void ReflectionANNIndex::recordRemapCommit(
    UUID new_ann_index_part_uuid,
    UUID retired_ann_index_part_uuid,
    const std::vector<CoverageEntry> & incoming,
    const std::vector<UUID> & outgoing_source_uuids)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyRemap(new_ann_index_part_uuid, retired_ann_index_part_uuid, incoming, outgoing_source_uuids);
    scheduler_state.applyRemap(new_ann_index_part_uuid, retired_ann_index_part_uuid, incoming, outgoing_source_uuids);
}

void ReflectionANNIndex::recordRemapBatchCommit(std::vector<CoverageMap::RemapCommit> commits)
{
    std::vector<CoverageMap::RemapCommit> scheduler_commits;
    scheduler_commits.reserve(commits.size());
    for (const auto & commit : commits)
        scheduler_commits.push_back(commit);

    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyRemapBatch(std::move(commits));
    scheduler_state.applyRemapBatch(scheduler_commits);
}

void ReflectionANNIndex::recordCompactCommit(
    UUID new_ann_index_part_uuid,
    const std::vector<UUID> & retired_ann_index_part_uuids,
    const std::vector<CoverageEntry> & incoming)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    coverage_map.applyCompact(new_ann_index_part_uuid, retired_ann_index_part_uuids, incoming);
    scheduler_state.applyCompact(new_ann_index_part_uuid, retired_ann_index_part_uuids, incoming);
}

bool ReflectionANNIndex::shouldCommitBuildOrCompactOutput(
    const std::vector<CoverageEntry> & entries,
    const String & task_kind,
    String & reason) const
{
    const UInt64 threshold_percent = (*getSettings())[MergeTreeSetting::ann_index_commit_min_valuable_rows_ratio_percent];
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
            "Committing ANNIndex {} output: valuable_rows={}, total_rows={}, valuable_ratio={}%, threshold={}%",
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

bool ReflectionANNIndex::shouldCommitRemapOutput(const FutureANNIndexPart & future_part, String & reason) const
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
    for (const auto & part : future_part.affected_ann_index_parts)
    {
        if (!part || !active_mi_uuids.contains(part->uuid))
        {
            reason = "Remap output skipped because an input materialized-index-part is no longer active";
            return false;
        }
    }

    return true;
}

void ReflectionANNIndex::setReplicatedLeaderLeaseForNextTask(String lease_path, String lease_payload, StoragePtr inner_table_snapshot)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    pending_replicated_leader_lease_path = std::move(lease_path);
    pending_replicated_leader_lease_payload = std::move(lease_payload);
    pending_replicated_leader_inner_table = std::move(inner_table_snapshot);
}

void ReflectionANNIndex::releasePendingReplicatedLeaderLease() noexcept
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
        replicated->releaseANNIndexLeaderLease(lease_path, lease_payload);
}

ReflectionANNIndex::ObservabilitySnapshot ReflectionANNIndex::getObservabilitySnapshot() const
{
    /// Take the scheduler snapshot under the lock, then drop the lock before
    /// touching parts — `getAccessPathPartsVectorForInternalUsage` re-enters
    /// the same non-recursive mutex through `getInnerTable`, which would
    /// self-deadlock.
    ANNIndexSchedulerState::ObservabilitySnapshot scheduler_snapshot;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_snapshot = scheduler_state.getObservabilitySnapshot();
    }

    ObservabilitySnapshot snapshot;
    snapshot.backlog_rows = scheduler_snapshot.backlog.rows;
    snapshot.backlog_bytes = scheduler_snapshot.backlog.bytes;
    snapshot.backlog_parts = scheduler_snapshot.backlog.parts;
    snapshot.pending_task_count = scheduler_snapshot.pending_task_count;
    snapshot.ready_ann_index_part_count = scheduler_snapshot.ready_ann_index_part_count;
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
        tryLogCurrentException(log, "Cannot read ANNIndex tombstone observability");
    }
    return snapshot;
}

std::map<String, String> ReflectionANNIndex::getAlgorithmObservabilityFields() const
{
    if (!algorithm)
        return {};
    return algorithm->getAlgorithmObservabilityFields();
}

std::vector<ReflectionANNIndex::SchedulerTaskSnapshot> ReflectionANNIndex::getSchedulerTasksSnapshot() const
{
    std::vector<ANNIndexSchedulerState::TaskSnapshot> scheduler_snapshot;
    {
        std::lock_guard lock(currently_processing_in_background_mutex);
        scheduler_snapshot = scheduler_state.getTasksSnapshot();
    }

    std::vector<SchedulerTaskSnapshot> result;
    result.reserve(scheduler_snapshot.size());
    for (const auto & task : scheduler_snapshot)
    {
        result.push_back(SchedulerTaskSnapshot{
            .task_id = task.task_id,
            .kind = task.kind,
            .input_source_uuids = task.input_source_uuids,
            .input_ann_index_part_uuids = task.input_ann_index_part_uuids,
            .output_ann_index_part_uuid = task.output_ann_index_part_uuid,
            .state = task.state,
            .retry_count = task.retry_count,
            .next_retry_time = task.next_retry_time,
            .last_error = task.last_error,
            .quarantined = task.quarantined,
            .created_at = task.created_at,
            .started_at = task.started_at,
            .finished_at = task.finished_at,
        });
    }
    return result;
}

void ReflectionANNIndex::triggerSchedulerTick()
{
    background_operations_assignee.trigger();
}

void ReflectionANNIndex::setBuildsEnabled(bool enabled)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.setBuildsEnabled(enabled);
    if (enabled)
        background_operations_assignee.trigger();
}

void ReflectionANNIndex::setRemapsEnabled(bool enabled)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.setRemapsEnabled(enabled);
    if (enabled)
        background_operations_assignee.trigger();
}

ReflectionObservabilitySnapshot ReflectionANNIndex::getReflectionObservabilitySnapshot() const
{
    const auto source = getObservabilitySnapshot();
    ReflectionObservabilitySnapshot result;
    result.backlog_rows = source.backlog_rows;
    result.backlog_bytes = source.backlog_bytes;
    result.backlog_parts = source.backlog_parts;
    result.pending_task_count = source.pending_task_count;
    result.ready_part_count = source.ready_ann_index_part_count;
    result.obsolete_ready_source_count = source.obsolete_ready_source_count;
    result.repeated_failure_count = source.repeated_failure_count;
    result.tombstone_rows = source.tombstone_rows;
    result.tombstone_ratio = source.tombstone_ratio;
    result.retry_count = source.retry_count;
    result.next_retry_time = source.next_retry_time;
    result.last_error = source.last_error;
    return result;
}

ReflectionSchedulerSnapshot ReflectionANNIndex::getSchedulerSnapshot() const
{
    ReflectionSchedulerSnapshot result;
    result.source_table_id = source_table_id;
    try
    {
        auto source_storage = DatabaseCatalog::instance().tryGetTable(source_table_id, getContext());
        if (const auto * source_mt = source_storage ? dynamic_cast<const MergeTreeData *>(source_storage.get()) : nullptr)
            result.source_part_count = source_mt->getDataPartsVectorForInternalUsage().size();
        result.reflection_part_count = getAccessPathPartsVectorForInternalUsage().size();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot collect Reflection scheduler snapshot");
    }
    return result;
}

UInt64 ReflectionANNIndex::getTaskMemoryBudgetBytes() const
{
    return (*getSettings())[MergeTreeSetting::ann_index_task_memory_budget_bytes];
}

UInt64 ReflectionANNIndex::estimateBuildOutputBytes(UInt64 input_rows, UInt64 input_bytes) const
{
    UInt64 estimate = 0;
    if (algorithm)
        estimate = algorithm->estimateBuildBytes(input_bytes, input_rows);
    if (estimate != 0)
        return estimate;

    const UInt64 ratio_percent = (*getSettings())[MergeTreeSetting::ann_index_size_ratio_percent];
    if (ratio_percent == 0 || input_bytes == 0)
        return input_bytes;
    if (input_bytes > std::numeric_limits<UInt64>::max() / ratio_percent)
        return input_bytes;
    return std::max<UInt64>(1, input_bytes * ratio_percent / 100);
}

void ReflectionANNIndex::postponeForResourceFailure(const String & reason)
{
    const UInt64 backoff_seconds = (*getSettings())[MergeTreeSetting::ann_index_resource_failure_backoff].totalSeconds();
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.postponeForResourceFailure(reason, std::chrono::seconds(backoff_seconds));
}

void ReflectionANNIndex::recordTaskFailure(const FutureANNIndexPart & future_part, const String & reason)
{
    static constexpr UInt64 MAX_REPEATED_TASK_FAILURES = 10;
    const UInt64 backoff_seconds = (*getSettings())[MergeTreeSetting::ann_index_resource_failure_backoff].totalSeconds();
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.recordTaskFailure(
        makeTaskFailureKey(future_part, family, impl),
        reason,
        std::chrono::seconds(backoff_seconds),
        MAX_REPEATED_TASK_FAILURES);
}

void ReflectionANNIndex::clearTaskFailure(const FutureANNIndexPart & future_part)
{
    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.clearTaskFailure(makeTaskFailureKey(future_part, family, impl));
}

bool ReflectionANNIndex::isTaskFailureBackoffActive(const FutureANNIndexPart & future_part)
{
    return scheduler_state.isTaskFailureBackoffActive(makeTaskFailureKey(future_part, family, impl));
}

bool ReflectionANNIndex::tryAcquireTaskResources(FutureANNIndexPart & future_part, UInt64 input_rows, UInt64 input_bytes)
{
    const auto settings = getSettings();
    const UInt64 max_rows = (*settings)[MergeTreeSetting::ann_index_task_max_input_rows];
    const UInt64 max_bytes = (*settings)[MergeTreeSetting::ann_index_task_max_input_bytes];
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

    const UInt64 max_global = (*settings)[MergeTreeSetting::ann_index_max_global_background_tasks];
    const UInt64 max_per_source = (*settings)[MergeTreeSetting::ann_index_max_background_tasks_per_source_table];
    const String source_key = source_table_id.getFullTableName();

    String failure_reason;
    {
        std::lock_guard counters_lock(ann_index_task_counters_mutex);
        if (max_global != 0 && global_ann_index_task_count >= max_global)
            failure_reason = fmt::format("global ANNIndex task limit {} reached", max_global);
        else
        {
            const UInt64 current_for_source = ann_index_tasks_by_source_table[source_key];
            if (max_per_source != 0 && current_for_source >= max_per_source)
                failure_reason = fmt::format("ANNIndex task limit {} reached for source table {}", max_per_source, source_key);
            else
            {
                ++global_ann_index_task_count;
                ++ann_index_tasks_by_source_table[source_key];
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

void ReflectionANNIndex::releaseTaskResources(FutureANNIndexPart & future_part) noexcept
{
    if (!future_part.resource_accounted)
        return;

    try
    {
        std::lock_guard lock(ann_index_task_counters_mutex);
        if (global_ann_index_task_count != 0)
            --global_ann_index_task_count;

        auto it = ann_index_tasks_by_source_table.find(future_part.source_table_key);
        if (it != ann_index_tasks_by_source_table.end())
        {
            if (it->second != 0)
                --it->second;
            if (it->second == 0)
                ann_index_tasks_by_source_table.erase(it);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to release ANNIndex resource counters");
    }

    future_part.resource_accounted = false;
}

void ReflectionANNIndex::rollbackUncommittedTaskReservation(FutureANNIndexPart & future_part) noexcept
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
        tryLogCurrentException(log, "Failed to release ANNIndex scheduler reservation during rollback");
    }
    releaseReplicatedLeaderLease(future_part);
    releaseReplicatedTaskReservation(future_part);
    releaseTaskResources(future_part);
}

bool ReflectionANNIndex::tryReserveReplicatedTask(FutureANNIndexPart & future_part)
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
    if (replicated->tryReserveANNIndexTask(key, payload, future_part.replicated_task_lock_path))
    {
        future_part.replicated_task_lock_payload = std::move(payload);
        return true;
    }

    LOG_TRACE(
        log,
        "Cannot reserve ANNIndex {} task {} for part {}: Keeper task {} already exists",
        materializedIndexTaskKindName(future_part.kind),
        future_part.task_id,
        future_part.new_part_name,
        key);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
    return false;
}

void ReflectionANNIndex::releaseReplicatedTaskReservation(FutureANNIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (replicated)
        replicated->releaseANNIndexTask(future_part.replicated_task_lock_path, future_part.replicated_task_lock_payload);
    future_part.replicated_task_lock_path.clear();
    future_part.replicated_task_lock_payload.clear();
}

void ReflectionANNIndex::releaseReplicatedLeaderLease(FutureANNIndexPart & future_part) noexcept
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (replicated)
        replicated->releaseANNIndexLeaderLease(
            future_part.replicated_leader_lease_path,
            future_part.replicated_leader_lease_payload);
    future_part.replicated_leader_lease_path.clear();
    future_part.replicated_leader_lease_payload.clear();
}

void ReflectionANNIndex::assertReplicatedTaskReservation(const FutureANNIndexPart & future_part) const
{
    auto * replicated = dynamic_cast<StorageReplicatedMergeTree *>(future_part.inner_table_snapshot.get());
    if (!replicated)
        return;

    replicated->assertANNIndexLeaderLease(
        future_part.replicated_leader_lease_path,
        future_part.replicated_leader_lease_payload);
    replicated->assertANNIndexTaskReservation(
        future_part.replicated_task_lock_path,
        future_part.replicated_task_lock_payload);
}

void ReflectionANNIndex::refreshBuildBacklog(
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

    ANNIndexSchedulerState::BacklogStats stats;
    stats.parts = uncovered_source_backlog.size();
    for (const auto & [_, entry] : uncovered_source_backlog)
    {
        stats.rows += entry.rows;
        stats.bytes += entry.bytes;
    }

    std::lock_guard lock(currently_processing_in_background_mutex);
    scheduler_state.setBacklogStats(stats);
}

MergeTreeData::DataPartsVector ReflectionANNIndex::selectBuildBatchFromBacklog(
    const MergeTreeData::DataPartsVector & candidate_source_parts,
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
    const UInt64 min_rows = (*settings)[MergeTreeSetting::ann_index_build_min_rows];
    const UInt64 min_bytes = (*settings)[MergeTreeSetting::ann_index_build_min_bytes];
    const UInt64 min_parts = (*settings)[MergeTreeSetting::ann_index_build_min_parts];
    const UInt64 max_delay_seconds = (*settings)[MergeTreeSetting::ann_index_build_max_delay].totalSeconds();

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

bool ReflectionANNIndex::tryReserveFuturePart(FutureANNIndexPart & future_part)
{
    fiu_do_on(FailPoints::ann_index_throw_in_try_reserve_future_part,
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
            "Postponing ANNIndex {} task for part {} because the same task failed repeatedly",
            materializedIndexTaskKindName(future_part.kind),
            future_part.new_part_name);
        return false;
    }
    if (currently_building_ann_index_parts.contains(future_part.new_part_name))
    {
        LOG_DEBUG(
            log,
            "Cannot reserve ANNIndex {} task {} for part {}: part name is already reserved",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }
    if (future_part.kind == FutureANNIndexPart::Kind::Build)
    {
        if (scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve ANNIndex Build task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (future_part.kind == FutureANNIndexPart::Kind::Remap)
    {
        if (scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactMerge))
        {
            LOG_DEBUG(
                log,
                "Cannot reserve ANNIndex Remap task {} for part {}: a Compact task is already active",
                future_part.task_id,
                future_part.new_part_name);
            return false;
        }
    }
    else if (scheduler_state.hasActiveTasks())
    {
        LOG_DEBUG(
            log,
            "Cannot reserve ANNIndex {} task {} for part {}: another task is already active",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name);
        return false;
    }

    bool reserved = false;
    if (future_part.kind == FutureANNIndexPart::Kind::Build)
    {
        reserved = scheduler_state.reserveBuildBatch(
            future_part.task_id,
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else if (future_part.kind == FutureANNIndexPart::Kind::Compact)
    {
        reserved = scheduler_state.reserveCompactRebuild(
            future_part.task_id,
            collectPartUuids(future_part.affected_ann_index_parts),
            collectPartUuids(future_part.source_parts_snapshot),
            future_part.new_part_uuid);
    }
    else
    {
        reserved = scheduler_state.reserveRemapLineage(
            future_part.task_id,
            collectPartUuids(future_part.affected_ann_index_parts),
            future_part.delta_out_source_uuids,
            future_part.new_part_uuid);
    }

    if (!reserved)
    {
        LOG_DEBUG(
            log,
            "Cannot reserve ANNIndex {} task {} for part {}: scheduler state rejected the reservation "
            "(source_parts={}, ann_index_parts={}, delta_in_parts={}, delta_out_sources={})",
            materializedIndexTaskKindName(future_part.kind),
            future_part.task_id,
            future_part.new_part_name,
            future_part.source_parts_snapshot.size(),
            future_part.affected_ann_index_parts.size(),
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

bool ReflectionANNIndex::shouldScheduleCompactRebuild(
    const DataPartsVector & source_snapshot,
    const DataPartsVector & ann_index_snapshot,
    const std::unordered_set<UUID> & covered_source_uuids) const
{
    const UInt64 min_parts = (*getSettings())[MergeTreeSetting::ann_index_compact_min_parts];
    const UInt64 tombstone_ratio_percent = (*getSettings())[MergeTreeSetting::ann_index_compact_tombstone_ratio_percent];
    if ((min_parts == 0 || ann_index_snapshot.size() < min_parts) && tombstone_ratio_percent == 0)
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
    mi_views.reserve(ann_index_snapshot.size());
    for (const auto & part : ann_index_snapshot)
    {
        if (!part)
            return false;
        mi_views.push_back(PartIdentityView{part->uuid, part->info.getPartitionId()});
    }

    if (!compactRebuildPartitionConditionMet(source_views, mi_views, covered_source_uuids))
        return false;

    if (min_parts != 0 && ann_index_snapshot.size() >= min_parts)
        return true;

    UInt64 total_rows = 0;
    UInt64 tombstone_rows = 0;
    for (const auto & part : ann_index_snapshot)
    {
        total_rows += part->rows_count;
        tombstone_rows += readTombstoneRowsFromHeader(part->getDataPartStorage());
    }

    return total_rows != 0 && tombstone_rows * 100 >= total_rows * tombstone_ratio_percent;
}

bool ReflectionANNIndex::scheduleDataProcessingJob(BackgroundJobsAssignee & assignee)
{
    if (shutdown_called.load(std::memory_order_relaxed))
        return false;

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

    /// I-BG-14: pull source / ann_index snapshots once per cycle.
    auto source_snapshot = source_mt->getDataPartsVectorForInternalUsage();
    auto ann_index_snapshot = inner.getDataPartsVectorForInternalUsage(
        {MergeTreeData::DataPartState::Active},
        {MergeTreePartInfo::Kind::ANNIndex});

    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_ann_index_part_uuid;
    std::unordered_set<UUID> active_build_source_uuids;
    bool has_active_tasks = false;
    bool builds_enabled = true;
    bool remaps_enabled = true;
    {
        /// Build and Remap tasks can overlap: Build writes new MI parts while
        /// Remap replaces already-ready MI parts. Remap tasks can also
        /// overlap when their reserved MI/source UUIDs do not intersect.
        /// Compact still stays exclusive because it rewrites a whole ready MI part set.
        std::lock_guard lock(currently_processing_in_background_mutex);
        if (scheduler_state.isResourceBackoffActive()
            || scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactRebuild)
            || scheduler_state.hasActiveTaskKind(ANNIndexSchedulerState::TaskKind::CompactMerge))
            return false;

        has_active_tasks = scheduler_state.hasActiveTasks() || !currently_building_ann_index_parts.empty();
        builds_enabled = scheduler_state.areBuildsEnabled();
        remaps_enabled = scheduler_state.areRemapsEnabled();

        scheduler_state.refreshSources(source_snapshot);
        active_build_source_uuids = scheduler_state.activeBuildSourceUuids();
        coverage_by_source_uuid = coverage_map.coverageEntriesBySourceUuid();
        coverage_by_ann_index_part_uuid = coverage_map.coverageEntriesByMiPartUuid();
    }

    if (!active_build_source_uuids.empty())
    {
        auto & pending_build_entries = coverage_by_ann_index_part_uuid[UUID{}];
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

    auto reconciled = SnapshotDiffReconciler::run(source_snapshot, ann_index_snapshot, coverage_by_ann_index_part_uuid);
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
        = (*getSettings())[MergeTreeSetting::ann_index_starvation_protection_cycles];
    const bool force_build
        = starvation_threshold != 0
        && consecutive_remap_count.load(std::memory_order_relaxed) >= starvation_threshold
        && reconciled.candidate_kind == ReconcileCandidateKind::BuildBatch;
    const bool initial_build = ann_index_snapshot.empty() && !source_snapshot.empty();
    auto build_batch = builds_enabled
        ? selectBuildBatchFromBacklog(
            build_candidates,
            initial_build,
            force_build)
        : DataPartsVector{};
    const bool compact_rebuild_candidate
        = builds_enabled
        && !has_active_tasks
        && reconciled.candidate_kind == ReconcileCandidateKind::Nothing
        && shouldScheduleCompactRebuild(source_snapshot, ann_index_snapshot, coverage);
    auto scheduler_reconciled = reconciled;
    if (!remaps_enabled
        && (scheduler_reconciled.candidate_kind == ReconcileCandidateKind::RemapLineage
            || scheduler_reconciled.candidate_kind == ReconcileCandidateKind::RebuildSourcePart
            || scheduler_reconciled.candidate_kind == ReconcileCandidateKind::ObsoleteCoverage))
        scheduler_reconciled.candidate_kind = ReconcileCandidateKind::Nothing;
    auto decision = ANNIndexSchedulerPolicy::choose(
        scheduler_reconciled,
        std::move(build_batch),
        compact_rebuild_candidate,
        source_snapshot,
        ann_index_snapshot);
    const bool remap_decision = decision.kind == ANNIndexSchedulerDecisionKind::RemapLineage
        || decision.kind == ANNIndexSchedulerDecisionKind::RebuildSourcePart
        || decision.kind == ANNIndexSchedulerDecisionKind::ObsoleteCoverageCleanup;
    if (!remaps_enabled && remap_decision)
        return false;
    const bool compact_decision = decision.kind == ANNIndexSchedulerDecisionKind::RebuildSourcePart
        || decision.kind == ANNIndexSchedulerDecisionKind::CompactRebuild
        || decision.kind == ANNIndexSchedulerDecisionKind::CompactMerge;
    if (has_active_tasks && compact_decision)
        return false;

    auto build_callback = [](bool /*delay*/) {};
    auto metadata_snapshot = getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
    auto storage_snapshot
        = source_mt->getStorageSnapshotWithoutData(
            source_mt->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false), context);

    auto submit_build = [&](DataPartsVector selected_source_parts)
    {
        auto fp = std::make_shared<FutureANNIndexPart>();
        fp->kind = FutureANNIndexPart::Kind::Build;
        fp->new_part_name = makeANNIndexBuildPartNameFromSourceParts(
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

        auto tagger = std::make_unique<CurrentlyBuildingANNIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<ANNIndexBuildSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        consecutive_remap_count.store(0, std::memory_order_relaxed);

        auto task = std::make_shared<ANNIndex::BuildTask>(
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

        const bool scheduled = assignee.scheduleReflectionTask(task, /*need_trigger=*/true);
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
        ANNIndexRemapKind remap_kind,
        DataPartsVector affected_ann_index_parts,
        DataPartsVector delta_in_source_parts,
        std::vector<UUID> delta_out_source_uuids)
    {
        auto fp = std::make_shared<FutureANNIndexPart>();
        fp->kind = FutureANNIndexPart::Kind::Remap;
        fp->remap_kind = remap_kind;
        /// MergeLineage / MutationLineage runs as N→N: each retired MI part is
        /// replaced by its own `bumpLevelInPartName(old)` output (see
        /// `ANNIndex::RemapTaskImpl::PlanAffectedSegmentsStage`). The compact-style merged
        /// part name from `makeANNIndexCompactPartName` would describe
        /// a single output covering the union range — the wrong shape for N→N
        /// commit, where each new part covers exactly one old. Use the first
        /// affected part as the placeholder name; per-output names come from
        /// the task's stage 1 and are committed individually.
        if (affected_ann_index_parts.size() == 1)
        {
            fp->new_part_name = makeANNIndexCompactPartName(
                affected_ann_index_parts,
                inner_format_version);
        }
        else
        {
            auto first_info = MergeTreePartInfo::fromPartName(
                affected_ann_index_parts.front()->name,
                inner_format_version);
            first_info.level += 1;
            fp->new_part_name = first_info.getPartNameAndCheckFormat(inner_format_version);
        }
        fp->new_part_uuid = UUIDHelpers::generateV4();
        fp->task_id = toString(fp->new_part_uuid);
        fp->affected_ann_index_parts = affected_ann_index_parts;
        fp->delta_in_source_parts = delta_in_source_parts;
        fp->delta_out_source_uuids = delta_out_source_uuids;

        const auto [input_rows, input_bytes] = sumRowsAndBytes(fp->affected_ann_index_parts);
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

        auto tagger = std::make_unique<CurrentlyBuildingANNIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<ANNIndexRemapSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        consecutive_remap_count.fetch_add(1, std::memory_order_relaxed);

        auto task = std::make_shared<ANNIndex::RemapTask>(
            *this,
            shared_from_this(),
            source_storage,
            std::move(entry),
            fp->affected_ann_index_parts,
            fp->delta_in_source_parts,
            fp->delta_out_source_uuids,
            fp->remap_kind,
            source_mt,
            storage_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            build_callback);

        return assignee.scheduleReflectionTask(task, /*need_trigger=*/true);
    };

    auto submit_compact = [&](DataPartsVector selected_source_parts, DataPartsVector affected_ann_index_parts)
    {
        /// Defensive: a compact requires at least one materialized-index part to
        /// fold into the new compact name. Upstream paths (Reconciler /
        /// SchedulerPolicy) should never hand us an empty set, but if they ever
        /// do, decline rather than tripping `makeANNIndexCompactPartName`'s
        /// LOGICAL_ERROR contract and aborting the server.
        if (affected_ann_index_parts.empty())
            return false;

        auto fp = std::make_shared<FutureANNIndexPart>();
        fp->kind = FutureANNIndexPart::Kind::Compact;
        fp->source_parts_snapshot = selected_source_parts;
        fp->affected_ann_index_parts = affected_ann_index_parts;
        fp->new_part_name = makeANNIndexCompactPartName(
            fp->affected_ann_index_parts,
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

        auto tagger = std::make_unique<CurrentlyBuildingANNIndexPartTagger>(fp, *this);
        auto entry = std::make_shared<ANNIndexBuildSelectedEntry>(fp, std::move(tagger));
        reservation_committed = true;

        auto task = std::make_shared<ANNIndex::CompactTask>(
            *this,
            shared_from_this(),
            source_storage,
            std::move(entry),
            fp->source_parts_snapshot,
            fp->affected_ann_index_parts,
            source_mt,
            storage_snapshot,
            metadata_snapshot,
            context,
            getTaskMemoryBudgetBytes(),
            estimateBuildOutputBytes(input_rows, input_bytes),
            build_callback);

        return assignee.scheduleReflectionTask(task, /*need_trigger=*/true);
    };

    switch (decision.kind)
    {
        case ANNIndexSchedulerDecisionKind::BuildBatch:
            return submit_build(std::move(decision.source_parts));
        case ANNIndexSchedulerDecisionKind::RemapLineage:
        case ANNIndexSchedulerDecisionKind::ObsoleteCoverageCleanup:
            return submit_remap(
                decision.remap_kind,
                std::move(decision.ann_index_parts),
                std::move(decision.source_parts),
                std::move(decision.delta_out_source_uuids));
        case ANNIndexSchedulerDecisionKind::RebuildSourcePart:
        case ANNIndexSchedulerDecisionKind::CompactRebuild:
        case ANNIndexSchedulerDecisionKind::CompactMerge:
            return submit_compact(
                std::move(decision.source_parts),
                std::move(decision.ann_index_parts));
        case ANNIndexSchedulerDecisionKind::Nothing:
            return false;
    }

    return false;
}

MergeTreeData::DataPartsVector ReflectionANNIndex::getAccessPathPartsVectorForInternalUsage() const
{
    auto inner_table_snapshot = getInnerTable();
    /// `system.ann_indexes` enumerates every MI storage on the server,
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
        {MergeTreeData::DataPartState::Active},
        {MergeTreePartInfo::Kind::ANNIndex});
}

/// read / write: handled by `IReflection` defaults (Stage-1 Constraint C1).
/// `getMutationsSnapshot` is no longer required on the Reflection itself: the
/// optimizer never invokes read/write on a Reflection so the snapshot is
/// unreachable.

void ReflectionANNIndex::checkAlterPartitionIsPossible(
    const PartitionCommands & commands,
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const Settings & settings,
    ContextPtr query_context) const
{
    auto inner_storage_table = getInnerTable();
    if (!inner_storage_table)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex {} does not have an inner table", getStorageID().getNameForLogs());

    PartitionCommands inner_commands;
    inner_commands.reserve(commands.size());
    for (const auto & command : commands)
        inner_commands.push_back(mapANNIndexPartitionCommandForInner(*this, command, query_context));

    const auto & inner = getInnerMergeTreeData(inner_storage_table);
    auto inner_metadata = inner.getInMemoryMetadataPtr(query_context, /*bypass_metadata_cache=*/false);
    inner_storage_table->checkAlterPartitionIsPossible(inner_commands, inner_metadata, settings, query_context);
}

Pipe ReflectionANNIndex::alterPartition(
    const StorageMetadataPtr & /*metadata_snapshot*/,
    const PartitionCommands & commands,
    ContextPtr query_context)
{
    auto inner_table_snapshot = getInnerTable();
    getInnerMergeTreeData(inner_table_snapshot).waitForOutdatedPartsToBeLoaded();

    for (const auto & command : commands)
    {
        switch (command.type)
        {
            case PartitionCommand::DROP_PARTITION:
            {
                if (command.part)
                    dropPart(getANNIndexPartNameFromAST(command.partition), command.detach, query_context);
                else
                    dropPartition(command.partition, command.detach, query_context);
                break;
            }
            default:
                throw Exception(
                    ErrorCodes::NOT_IMPLEMENTED,
                    "{} is not supported for ANNIndex yet",
                    command.typeToString());
        }
    }

    return {};
}

/// clearEmptyParts / clearUnusedPatchParts / dropPartNoWaitNoThrow: removed.
/// All MergeTree-side cleanup runs on the inner table's own
/// `MergeTreeCleanupThread`; nothing else now calls these on the Reflection.

void ReflectionANNIndex::dropPart(const String & part_name, bool detach, ContextPtr query_context)
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

void ReflectionANNIndex::dropPartition(const ASTPtr & partition, bool detach, ContextPtr query_context)
{
    PartitionCommand command;
    command.type = PartitionCommand::DROP_PARTITION;
    command.partition = mapANNIndexPartitionForInner(*this, partition, query_context);
    command.detach = detach;
    executeInnerPartitionCommand(*this, std::move(command), query_context);
    refreshCoverageFromActiveParts();
}

/// attachPartition / replacePartitionFrom / movePartitionToTable /
/// attachRestoredParts / getDefaultSettings: dropped along with the
/// MergeTreeData base. They were either MergeTreeData pure-virtual stubs that
/// have no non-MergeTree caller, or were called only from the MergeTree-family
/// hot-restore / partition-move path that does not apply to a Reflection.

std::vector<CoverageEntry> ReflectionANNIndex::parseCoverageJsonFromMiPart(const IMergeTreeDataPart & part)
{
    return parseCoverageWithVersionFromMiPart(part).entries;
}

ReflectionANNIndex::ParsedCoverage ReflectionANNIndex::parseCoverageWithVersionFromMiPart(const IMergeTreeDataPart & part)
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

    /// `payload_format_version` was added when ANN payload records were
    /// compacted from 24 B (UUID + UInt64 offset) to 8/12 B (UInt32 part_id
    /// + UInt32/64 offset). A missing field means the manifest predates the
    /// compact format; callers treat the MI-part as having no inline
    /// payload and route every hit through the cold path.
    UInt8 payload_format_version = 0;
    if (root->has("payload_format_version"))
        payload_format_version = static_cast<UInt8>(root->getValue<int>("payload_format_version"));

    std::optional<String> root_source_partition_id;
    if (root->has("source_partition_id"))
        root_source_partition_id = root->getValue<std::string>("source_partition_id");

    ParsedCoverage out;
    out.payload_format_version = payload_format_version;
    auto covered = root->getArray("covered");
    if (!covered)
        return out;

    out.entries.reserve(covered->size());
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

        /// Per-entry compact-payload local id. Optional for backward
        /// compatibility with legacy manifests; absence keeps the sentinel
        /// value installed by `CoverageEntry`'s default ctor and the
        /// matcher will fall back to the cold path.
        if (item->has("payload_part_id"))
            entry.payload_part_id = static_cast<UInt32>(item->getValue<Int64>("payload_part_id"));

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

        out.entries.push_back(std::move(entry));
    }
    return out;
}

bool ReflectionANNIndex::waitForCoverageOfSourceOrTimeout(std::chrono::seconds timeout, ContextPtr query_context)
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
