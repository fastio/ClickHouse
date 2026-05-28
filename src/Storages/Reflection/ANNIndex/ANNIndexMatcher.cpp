#include <Storages/Reflection/ANNIndex/ANNIndexMatcher.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartReverseLookup.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

#include <fmt/format.h>

#include <algorithm>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

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
            for (const auto & entry : ReflectionANNIndex::parseCoverageJsonFromMiPart(*part))
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

SourceSearchResult translateInternalHitsToSourceRows(const InternalSearchResult & internal_result)
{
    std::unordered_map<UUID, SourceRowSet> per_uuid;

    for (const auto & hit_set : internal_result.per_ann_index_part)
    {
        if (!hit_set.ann_index_part_storage)
            continue;
        if (hit_set.internal_ids.size() != hit_set.distances.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "ANNIndex search returned {} internal ids but {} distances",
                hit_set.internal_ids.size(), hit_set.distances.size());

        ANNIndexPartReverseLookup lookup(*hit_set.ann_index_part_storage);
        for (size_t i = 0; i < hit_set.internal_ids.size(); ++i)
        {
            auto src = lookup.lookup(hit_set.internal_ids[i]);
            if (src.is_tombstone)
                continue;

            auto & bucket = per_uuid[src.part_uuid];
            bucket.source_part_uuid = src.part_uuid;
            bucket.part_offsets.push_back(src.part_offset);
            bucket.distances.push_back(hit_set.distances[i]);
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
    hints.covered_source_parts = covered_source_parts;
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
    const ReflectionPlanShape & shape, ContextPtr /*query_context*/)
{
    if (!shape.active_source_parts)
        return std::nullopt;

    auto * algo = storage.getAlgorithm();
    if (!algo)
        return std::nullopt;

    QueryFeatures features;
    features.query_vector = shape.query_vector;
    features.distance_function = shape.distance_function;
    features.k = shape.top_k;

    auto desc = algo->match(features);
    if (!desc.has_value())
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
    const auto source_result = translateInternalHitsToSourceRows(internal_result);

    ReflectionReadHintRealization realization;
    realization.hits = buildHintsForCoveredSourceParts(source_result, handle->covered_source_parts);
    realization.distance.exact_function_name = handle->desc.distance.exact_function_name;
    realization.distance.metric_id = handle->desc.distance.metric_id;
    realization.distance.dim = handle->desc.distance.dim;
    realization.virtual_column = "_distance";
    return realization;
}

}
