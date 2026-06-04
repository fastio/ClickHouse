#include <Storages/Reflection/ANNIndex/ANNIndexMatcher.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/assert_cast.h>
#include <Common/logger_useful.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/Reflection/ANNIndex/ANNIndexReadHint.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

#include <fmt/format.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>


namespace ProfileEvents
{
    extern const Event ANNIndexHotPathHits;
    extern const Event ANNIndexColdPathHits;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool force_using_ann_index;
    extern const SettingsBool ann_index_disable_hot_path;
}

namespace
{

/// True when the source table carries a `vector_similarity` skip index over the
/// search column. Such a source serves the query natively, so the ANN engine
/// yields to it unless the user forces the Reflection path. Moved here from the
/// generic optimizer: recognising a competing vector index is engine knowledge.
bool sourceHasVectorSimilarityIndex(const StorageMetadataPtr & metadata, const String & search_column)
{
    if (!metadata)
        return false;
    for (const auto & index : metadata->getSecondaryIndices())
    {
        if (index.type != "vector_similarity")
            continue;
        if (!index.expression)
            continue;
        const auto required = index.expression->getRequiredColumns();
        if (required.size() == 1 && required.front() == search_column)
            return true;
    }
    return false;
}

struct ActiveSourcePartMetadata
{
    UInt64 rows = 0;
    String partition_id;
    Int64 min_block = 0;
    Int64 max_block = 0;
    UInt32 level = 0;
    Int64 mutation = 0;
};

std::unordered_map<UUID, ActiveSourcePartMetadata> activeSourceMetadataByUuid(const RangesInDataParts & active_parts)
{
    std::unordered_map<UUID, ActiveSourcePartMetadata> metadata_by_uuid;
    metadata_by_uuid.reserve(active_parts.size());
    for (const auto & part : active_parts)
    {
        ActiveSourcePartMetadata metadata;
        metadata.rows = part.data_part->rows_count;
        metadata.partition_id = part.data_part->info.getPartitionId();
        metadata.min_block = part.data_part->info.min_block;
        metadata.max_block = part.data_part->info.max_block;
        metadata.level = part.data_part->info.level;
        metadata.mutation = part.data_part->info.mutation;
        metadata_by_uuid.emplace(part.data_part->uuid, std::move(metadata));
    }
    return metadata_by_uuid;
}

std::unordered_set<String> activeSourcePartitions(
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid)
{
    std::unordered_set<String> partitions;
    partitions.reserve(active_metadata_by_uuid.size());
    for (const auto & [_, metadata] : active_metadata_by_uuid)
        partitions.insert(metadata.partition_id);
    return partitions;
}

bool coveredEntryMatchesActiveSourcePart(
    const CoveredSourcePart & entry,
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid)
{
    auto active_it = active_metadata_by_uuid.find(entry.source_part_uuid);
    if (active_it == active_metadata_by_uuid.end())
        return false;

    const auto & active = active_it->second;
    if (entry.rows != active.rows)
        return false;
    if (entry.has_part_info
        && (entry.partition_id != active.partition_id
            || entry.min_block != active.min_block
            || entry.max_block != active.max_block
            || entry.level != active.level
            || entry.mutation != active.mutation))
        return false;
    return true;
}

bool readyPartMatchesActivePartitions(
    const ReadyANNIndexPart & ready_part,
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid,
    const std::unordered_set<String> & active_partitions)
{
    bool has_active_entry = false;
    std::unordered_set<String> ready_partitions;
    for (const auto & entry : ready_part.covered_source_parts)
    {
        if (entry.has_part_info)
            ready_partitions.insert(entry.partition_id);
        if (coveredEntryMatchesActiveSourcePart(entry, active_metadata_by_uuid))
            has_active_entry = true;
    }

    if (!has_active_entry)
        return false;

    /// New-format MI parts have exactly one source partition. Legacy/global
    /// parts either have multiple partitions or no partition root; search them
    /// only when their covered partitions are a subset of the active source
    /// partitions selected by `ReadFromMergeTree`.
    if (ready_partitions.empty())
        return active_partitions.size() != 1;

    for (const auto & partition_id : ready_partitions)
    {
        if (!active_partitions.contains(partition_id))
            return false;
    }
    return true;
}

ReadyANNIndexPartSnapshot buildReadySnapshot(
    const MergeTreeData::DataPartsVector & ready_ann_index_parts_data,
    const IANNIndexAlgorithm * algorithm,
    LoggerPtr log)
{
    ReadyANNIndexPartSnapshot snapshot;
    snapshot.parts.reserve(ready_ann_index_parts_data.size());
    for (const auto & part : ready_ann_index_parts_data)
    {
        if (!part)
            continue;

        ReadyANNIndexPart ready_part;
        ready_part.storage = part->getDataPartStoragePtr();
        try
        {
            if (algorithm)
            {
                auto compatibility = algorithm->checkPartCompatibility(part->getDataPartStorage());
                if (!compatibility.compatible)
                {
                    LOG_WARNING(
                        log,
                        "Skipping materialized-index-part {} because it is incompatible with algorithm {}/{}: {}",
                        part->name,
                        algorithm->getFamily(),
                        algorithm->getName(),
                        compatibility.reason);
                    continue;
                }
            }
            const auto parsed = ReflectionANNIndex::parseCoverageWithVersionFromMiPart(*part);
            /// `payload_format_version == 0` means the manifest predates the
            /// compact payload schema; we keep the default `V1_OFFSET32`
            /// width but the algorithm-side sidecar size check will reject
            /// the legacy file and route hits through the cold path.
            if (parsed.payload_format_version
                == static_cast<UInt8>(ANNIndexPayloadCodec::Version::V2_OFFSET64))
                ready_part.payload_format_version = ANNIndexPayloadCodec::Version::V2_OFFSET64;
            else
                ready_part.payload_format_version = ANNIndexPayloadCodec::Version::V1_OFFSET32;

            for (const auto & entry : parsed.entries)
            {
                CoveredSourcePart covered_part;
                covered_part.source_part_uuid = entry.source_part_uuid;
                covered_part.rows = entry.rows;
                covered_part.partition_id = entry.partition_id;
                covered_part.min_block = entry.min_block;
                covered_part.max_block = entry.max_block;
                covered_part.level = entry.level;
                covered_part.mutation = entry.mutation;
                covered_part.has_part_info = entry.has_part_info;
                covered_part.payload_part_id = entry.payload_part_id;
                ready_part.covered_source_parts.push_back(std::move(covered_part));
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to load coverage.json for materialized-index-part {}", part->name));
            continue;
        }

        snapshot.parts.push_back(std::move(ready_part));
    }
    return snapshot;
}

ReadyANNIndexPartSnapshot pruneReadySnapshotForActivePartitions(
    ReadyANNIndexPartSnapshot ready_snapshot,
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid)
{
    const auto active_partitions = activeSourcePartitions(active_metadata_by_uuid);
    ready_snapshot.parts.erase(
        std::remove_if(
            ready_snapshot.parts.begin(),
            ready_snapshot.parts.end(),
            [&](const auto & ready_part)
            {
                return !readyPartMatchesActivePartitions(ready_part, active_metadata_by_uuid, active_partitions);
            }),
        ready_snapshot.parts.end());
    return ready_snapshot;
}

std::unordered_set<UUID> coveredActiveSourceParts(
    const ReadyANNIndexPartSnapshot & ready_snapshot,
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid)
{
    std::unordered_set<UUID> covered;
    for (const auto & ready_part : ready_snapshot.parts)
    {
        for (const auto & entry : ready_part.covered_source_parts)
        {
            /// Known limitation: a ready ANN part may also contain stale rows
            /// from source parts that are no longer active. Those rows are
            /// filtered out after search, so they can still consume the
            /// algorithm candidate budget until engine-side validity filtering
            /// is implemented.
            if (!coveredEntryMatchesActiveSourcePart(entry, active_metadata_by_uuid))
                continue;

            covered.insert(entry.source_part_uuid);
        }
    }
    return covered;
}

CoverageSnapshot buildCoverageSnapshot(
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid,
    const ReadyANNIndexPartSnapshot & ready_snapshot,
    size_t candidate_limit)
{
    CoverageSnapshot snapshot;
    snapshot.active_source_parts = active_metadata_by_uuid.size();
    snapshot.ready_ann_index_parts = ready_snapshot.parts.size();
    snapshot.candidate_limit = candidate_limit;

    for (const auto & [_, metadata] : active_metadata_by_uuid)
        snapshot.active_source_rows += metadata.rows;

    const auto covered = coveredActiveSourceParts(ready_snapshot, active_metadata_by_uuid);
    snapshot.covered_source_parts = covered.size();
    for (const auto & uuid : covered)
        snapshot.covered_source_rows += active_metadata_by_uuid.at(uuid).rows;

    snapshot.uncovered_source_rows = snapshot.active_source_rows - snapshot.covered_source_rows;
    snapshot.full_coverage = snapshot.active_source_parts != 0
        && snapshot.covered_source_parts == snapshot.active_source_parts;
    return snapshot;
}

/// One resolved locator row: where an algorithm-internal id maps in the source table.
struct LocatorRow
{
    UUID source_uuid;
    UInt64 part_offset = 0;
};

/// Batch-read the locator columns (`source_uuid`, `part_offset`) for the given
/// algorithm-internal ids out of a single ANN-index part. The part is an
/// ordinary unsorted Wide part, so the internal id equals the part-local row
/// number; we translate ids into mark ranges via the part's index granularity
/// and read only the touched granules. The ANN part's own `_part_offset`
/// virtual column carries the row number, which we use to key the result back
/// to the requested ids.
std::unordered_map<UInt64, LocatorRow> readLocatorRows(
    const MergeTreeData & inner_storage,
    const StorageSnapshotPtr & snapshot,
    const DataPartPtr & part,
    const std::vector<UInt64> & internal_ids)
{
    std::unordered_map<UInt64, LocatorRow> result;
    if (internal_ids.empty())
        return result;

    std::vector<UInt64> ids(internal_ids.begin(), internal_ids.end());
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());

    const auto & granularity = part->index_granularity;
    MarkRanges mark_ranges;
    for (UInt64 id : ids)
    {
        if (id >= part->rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ANNIndex internal id {} is out of range for part {} with {} rows",
                id, part->name, part->rows_count);

        MarkRange range = granularity->getMarkRangeForRowOffset(id);
        if (!mark_ranges.empty() && mark_ranges.back().end >= range.begin)
            mark_ranges.back().end = std::max(mark_ranges.back().end, range.end);
        else
            mark_ranges.push_back(range);
    }

    Names columns_to_read{
        ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN,
        ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN,
        "_part_offset"};

    Pipe pipe = createMergeTreeSequentialSource(
        MergeTreeSequentialSourceType::Merge,
        inner_storage,
        snapshot,
        RangesInDataPart(part),
        /*alter_conversions=*/std::make_shared<AlterConversions>(),
        /*merged_part_offsets=*/nullptr,
        std::move(columns_to_read),
        std::move(mark_ranges),
        /*filtered_rows_count=*/nullptr,
        /*apply_deleted_mask=*/false,
        /*read_with_direct_io=*/false,
        /*prefetch=*/false);

    QueryPipeline pipeline(std::move(pipe));
    PullingPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        const size_t rows = block.rows();
        if (!rows)
            continue;

        const auto & uuid_data
            = assert_cast<const ColumnUUID &>(*block.getByName(ANN_INDEX_LOCATOR_SOURCE_UUID_COLUMN).column).getData();
        const auto & offset_data
            = assert_cast<const ColumnUInt64 &>(*block.getByName(ANN_INDEX_LOCATOR_PART_OFFSET_COLUMN).column).getData();
        const auto & part_offset_data
            = assert_cast<const ColumnUInt64 &>(*block.getByName("_part_offset").column).getData();

        for (size_t i = 0; i < rows; ++i)
            result[part_offset_data[i]] = LocatorRow{uuid_data[i], offset_data[i]};
    }

    return result;
}

SourceSearchResult translateInternalHitsToSourceRows(
    const InternalSearchResult & internal_result,
    const MergeTreeData::DataPartsVector & inner_parts,
    const ReadyANNIndexPartSnapshot & ready_snapshot,
    const std::unordered_set<UUID> & active_source_uuids,
    bool disable_hot_path,
    ContextPtr query_context)
{
    std::unordered_map<UUID, SourceRowSet> per_uuid;

    if (!inner_parts.empty())
    {
        std::unordered_map<String, DataPartPtr> part_by_path;
        part_by_path.reserve(inner_parts.size());
        for (const auto & part : inner_parts)
            part_by_path.emplace(part->getDataPartStorage().getRelativePath(), part);

        /// Per-MI-part `payload_part_id → source UUID` lookup, keyed by
        /// the MI-part's relative storage path so we can find it from the
        /// `InternalHitSet::ann_index_part_storage`. Built once per call;
        /// each MI-part typically has a few hundred entries at most.
        std::unordered_map<String, std::unordered_map<UInt32, UUID>>
            uuid_by_part_id_per_mi_part;
        uuid_by_part_id_per_mi_part.reserve(ready_snapshot.parts.size());
        for (const auto & ready_part : ready_snapshot.parts)
        {
            if (!ready_part.storage)
                continue;
            auto & lookup = uuid_by_part_id_per_mi_part[ready_part.storage->getRelativePath()];
            lookup.reserve(ready_part.covered_source_parts.size());
            for (const auto & covered : ready_part.covered_source_parts)
            {
                if (ANNIndexPayloadCodec::isTombstone(covered.payload_part_id))
                    continue;
                lookup.emplace(covered.payload_part_id, covered.source_part_uuid);
            }
        }

        const MergeTreeData & inner_storage = inner_parts.front()->storage;
        auto snapshot = inner_storage.getStorageSnapshot(
            inner_storage.getInMemoryMetadataPtr(inner_storage.getContext(), /*bypass_metadata_cache=*/false),
            query_context);

        for (const auto & hit_set : internal_result.per_ann_index_part)
        {
            if (!hit_set.ann_index_part_storage)
                continue;
            if (hit_set.internal_ids.size() != hit_set.distances.size())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "ANNIndex search returned {} internal ids but {} distances",
                    hit_set.internal_ids.size(), hit_set.distances.size());

            /// Per-`InternalHitSet` invariant: hot_part_ids / hot_part_offsets
            /// are either both empty or both equal in size to internal_ids.
            const bool hit_set_has_payload = !disable_hot_path && hit_set.hasPayload();
            if (hit_set_has_payload
                && (hit_set.hot_part_ids.size() != hit_set.internal_ids.size()
                    || hit_set.hot_part_offsets.size() != hit_set.internal_ids.size()))
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "ANNIndex search payload size mismatch: {} ids, {} hot part_ids, {} hot offsets",
                    hit_set.internal_ids.size(),
                    hit_set.hot_part_ids.size(),
                    hit_set.hot_part_offsets.size());
            }

            auto part_it = part_by_path.find(hit_set.ann_index_part_storage->getRelativePath());
            if (part_it == part_by_path.end())
                continue;

            const auto * part_id_lookup = hit_set_has_payload
                ? &uuid_by_part_id_per_mi_part[hit_set.ann_index_part_storage->getRelativePath()]
                : nullptr;

            /// Hot path: payload tells us {part_id, part_offset} directly.
            /// Translate part_id → source UUID via the MI-part lookup, then
            /// trust the result only when the UUID is still in the active
            /// source set and the offset is not the tombstone sentinel.
            ///
            /// Cold path: collect the indices that need locator-column
            /// lookup (no payload, unknown / tombstone part_id, stale UUID,
            /// or sentinel offset). One Pipe + one IO scan per part is
            /// reused for the whole cold subset.
            std::vector<UInt64> cold_internal_ids;
            std::vector<float> cold_distances;
            cold_internal_ids.reserve(hit_set.internal_ids.size());
            cold_distances.reserve(hit_set.distances.size());

            for (size_t i = 0; i < hit_set.internal_ids.size(); ++i)
            {
                if (hit_set_has_payload)
                {
                    const UInt32 hot_part_id = hit_set.hot_part_ids[i];
                    const UInt64 hot_offset = hit_set.hot_part_offsets[i];
                    if (hot_offset != ANN_INDEX_LOCATOR_TOMBSTONE_PART_OFFSET
                        && !ANNIndexPayloadCodec::isTombstone(hot_part_id))
                    {
                        auto uuid_it = part_id_lookup ? part_id_lookup->find(hot_part_id) : decltype(part_id_lookup->find(0)){};
                        if (part_id_lookup && uuid_it != part_id_lookup->end()
                            && active_source_uuids.contains(uuid_it->second))
                        {
                            const UUID & hot_uuid = uuid_it->second;
                            auto & bucket = per_uuid[hot_uuid];
                            bucket.source_part_uuid = hot_uuid;
                            bucket.part_offsets.push_back(hot_offset);
                            bucket.distances.push_back(hit_set.distances[i]);
                            ProfileEvents::increment(ProfileEvents::ANNIndexHotPathHits);
                            continue;
                        }
                    }
                }
                cold_internal_ids.push_back(hit_set.internal_ids[i]);
                cold_distances.push_back(hit_set.distances[i]);
            }

            if (cold_internal_ids.empty())
                continue;

            const auto locator = readLocatorRows(inner_storage, snapshot, part_it->second, cold_internal_ids);

            for (size_t i = 0; i < cold_internal_ids.size(); ++i)
            {
                auto row_it = locator.find(cold_internal_ids[i]);
                if (row_it == locator.end())
                    continue;
                /// A remapped-away (deleted) source row carries the sentinel offset.
                if (row_it->second.part_offset == ANN_INDEX_LOCATOR_TOMBSTONE_PART_OFFSET)
                    continue;

                auto & bucket = per_uuid[row_it->second.source_uuid];
                bucket.source_part_uuid = row_it->second.source_uuid;
                bucket.part_offsets.push_back(row_it->second.part_offset);
                bucket.distances.push_back(cold_distances[i]);
                ProfileEvents::increment(ProfileEvents::ANNIndexColdPathHits);
            }
        }
    }

    SourceSearchResult result;
    result.hits_per_part.reserve(per_uuid.size());
    for (auto & [_, bucket] : per_uuid)
        result.hits_per_part.push_back(std::move(bucket));
    return result;
}

ANNIndexHints buildHintsForCoveredSourceParts(
    const SourceSearchResult & source_result,
    const std::unordered_set<UUID> & covered_source_parts)
{
    ANNIndexHints hints;
    hints.covered_source_parts.insert(covered_source_parts.begin(), covered_source_parts.end());
    hints.hits_per_part.reserve(source_result.hits_per_part.size());

    for (const auto & set : source_result.hits_per_part)
    {
        if (!hints.covered_source_parts.contains(set.source_part_uuid))
            continue;

        NearestNeighbours neighbours;
        neighbours.rows = set.part_offsets;
        neighbours.distances = set.distances;
        hints.hits_per_part.emplace(set.source_part_uuid, std::move(neighbours));
    }

    return hints;
}

/// Engine-private state ferried from `matchReadHint` (offer) into
/// `realizeReadHint` (winner) via `ReflectionReadHintOffer::private_handle`.
struct ANNMatchHandle
{
    MatchDescriptor desc;
    ReadyANNIndexPartSnapshot ready_snapshot;
    std::unordered_set<UUID> covered_source_parts;
};

}


ANNIndexMatcher::ANNIndexMatcher(ReflectionANNIndex & storage_)
    : storage(storage_)
{
}


std::optional<ReflectionReadHintOffer> ANNIndexMatcher::matchReadHint(
    const ReflectionPlanShape & shape, ContextPtr query_context)
{
    if (!shape.active_source_parts)
        return std::nullopt;

    /// Yield to a source-side `vector_similarity` index unless the user forces
    /// the Reflection path. This arbitration is engine-specific (it knows ANN
    /// competes with a vector index) and therefore lives here rather than in the
    /// generic optimizer.
    const bool force = query_context && query_context->getSettingsRef()[Setting::force_using_ann_index];
    if (!force && sourceHasVectorSimilarityIndex(shape.source_metadata, shape.search_column))
        return std::nullopt;

    auto * algo = storage.getAlgorithm();
    if (!algo)
        return std::nullopt;

    QueryFeatures features;
    features.query_vector = shape.query_vector;
    features.distance_function = shape.distance_function;
    features.k = shape.top_k;

    /// The algorithm owns the distance-function recognition: `match` returns
    /// nullopt for a function it does not accelerate (or an incompatible query,
    /// e.g. a mismatched reference-vector dimension).
    auto desc = algo->match(features);
    if (!desc.has_value())
        return std::nullopt;

    /// The algorithm also declares the metric's ordering semantics; the query's
    /// ORDER BY direction must agree (ascending for smaller-is-better metrics
    /// such as `L2Distance`/`cosineDistance`, descending for `dotProduct`).
    const int expected_direction = desc->distance.smaller_is_better ? 1 : -1;
    if (shape.sort_direction != expected_direction)
        return std::nullopt;

    auto ready_ann_index_parts_data = storage.getAccessPathPartsVectorForInternalUsage();
    if (ready_ann_index_parts_data.empty())
        return std::nullopt;

    auto log = getLogger("ANNIndexMatcher");
    auto ready_snapshot = buildReadySnapshot(ready_ann_index_parts_data, algo, log);

    const auto active_metadata_by_uuid = activeSourceMetadataByUuid(*shape.active_source_parts);
    ready_snapshot = pruneReadySnapshotForActivePartitions(std::move(ready_snapshot), active_metadata_by_uuid);
    if (ready_snapshot.parts.empty())
        return std::nullopt;

    auto covered_source_parts = coveredActiveSourceParts(ready_snapshot, active_metadata_by_uuid);
    if (covered_source_parts.empty())
        return std::nullopt;

    const auto coverage = buildCoverageSnapshot(active_metadata_by_uuid, ready_snapshot, shape.candidate_limit);
    const auto cost_estimate = algo->estimateCost(*desc, coverage);

    auto handle = std::make_shared<ANNMatchHandle>();
    handle->desc = std::move(*desc);
    handle->ready_snapshot = std::move(ready_snapshot);
    handle->covered_source_parts = std::move(covered_source_parts);

    ReflectionReadHintOffer offer;
    offer.engine_search_cost = cost_estimate.algorithm_search_cost;
    offer.uncovered_source_rows = coverage.uncovered_source_rows;
    offer.ready_reflection_parts = coverage.ready_ann_index_parts;
    offer.has_source_parts = coverage.active_source_parts != 0;
    offer.full_coverage = coverage.full_coverage;
    offer.reflection_name = storage.getStorageID().getTableName();
    offer.engine_name = storage.getImpl();
    offer.private_handle = std::move(handle);
    return offer;
}


ReflectionReadHintRealization ANNIndexMatcher::realizeReadHint(
    const ReflectionPlanShape & shape,
    const ReflectionReadHintOffer & offer,
    ContextPtr query_context)
{
    auto handle = std::static_pointer_cast<ANNMatchHandle>(offer.private_handle);
    if (!handle)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexMatcher::realizeReadHint received a null private handle");

    auto * algo = storage.getAlgorithm();
    if (!algo)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexMatcher::realizeReadHint invoked without an initialized algorithm");

    const auto internal_result = algo->search(handle->desc, handle->ready_snapshot, shape.candidate_limit, query_context);
    const auto inner_parts = storage.getAccessPathPartsVectorForInternalUsage();
    const bool disable_hot_path = query_context && query_context->getSettingsRef()[Setting::ann_index_disable_hot_path];
    const auto source_result = translateInternalHitsToSourceRows(
        internal_result, inner_parts, handle->ready_snapshot, handle->covered_source_parts, disable_hot_path, query_context);

    ReflectionReadHintRealization realization;
    realization.covered_source_parts = handle->covered_source_parts;
    realization.virtual_column = "_distance";
    realization.distance.exact_function_name = handle->desc.distance.exact_function_name;
    realization.distance.metric_id = handle->desc.distance.metric_id;
    realization.distance.dim = handle->desc.distance.dim;

    /// Ferry the engine-specific hint application behind a generic callback so
    /// the framework optimizer never sees the ANN hint type. The hints are
    /// consumed exactly once (the full-coverage read step, or the covered
    /// branch of a partial-coverage Union).
    auto covered_hints = buildHintsForCoveredSourceParts(source_result, handle->covered_source_parts);
    const String search_column = shape.search_column;
    realization.apply_to_covered_read_step
        = [hints = std::move(covered_hints), search_column](ReadFromMergeTree & read_step, bool keep_search_column) mutable
    {
        applyANNIndexHintsToReadStep(read_step, std::move(hints), keep_search_column, search_column);
    };
    return realization;
}

}
