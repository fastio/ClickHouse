#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <algorithm>
#include <tuple>


namespace DB
{

namespace
{

ReconcileSourcePart makeSourcePartView(const MergeTreeData::DataPartPtr & part)
{
    ReconcileSourcePart view;
    if (!part)
        return view;

    view.uuid = part->uuid;
    view.partition_id = part->info.getPartitionId();
    view.min_block = part->info.min_block;
    view.max_block = part->info.max_block;
    view.level = part->info.level;
    view.mutation = part->info.mutation;
    view.rows = part->rows_count;
    view.has_part_info = true;
    view.part = part;
    return view;
}

std::vector<ReconcileSourcePart> makeSourcePartViews(const MergeTreeData::DataPartsVector & source_snapshot)
{
    std::vector<ReconcileSourcePart> views;
    views.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
        if (part)
            views.push_back(makeSourcePartView(part));
    return views;
}

bool entryCanPrecedeMergePart(const CoverageEntry & entry, const ReconcileSourcePart & part)
{
    return entry.has_part_info
        && part.has_part_info
        && entry.partition_id == part.partition_id
        && entry.mutation == part.mutation
        && entry.min_block >= part.min_block
        && entry.max_block <= part.max_block
        && entry.level < part.level;
}

bool entryCanPrecedeMutationPart(const CoverageEntry & entry, const ReconcileSourcePart & part)
{
    return entry.has_part_info
        && part.has_part_info
        && part.indexed_columns_unchanged_by_mutation
        && entry.partition_id == part.partition_id
        && entry.min_block == part.min_block
        && entry.max_block == part.max_block
        && entry.level == part.level
        && entry.mutation < part.mutation;
}

bool entriesFullyCoverPartRange(std::vector<CoverageEntry> entries, const ReconcileSourcePart & part)
{
    if (entries.empty())
        return false;

    std::sort(
        entries.begin(),
        entries.end(),
        [](const auto & lhs, const auto & rhs)
        {
            return lhs.min_block < rhs.min_block;
        });

    Int64 expected_min_block = part.min_block;
    for (const auto & entry : entries)
    {
        if (entry.min_block != expected_min_block)
            return false;
        if (entry.max_block < entry.min_block)
            return false;
        expected_min_block = entry.max_block + 1;
    }

    return expected_min_block == part.max_block + 1;
}

std::unordered_map<UUID, CoverageEntry> buildCoverageBySourceUuid(
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    for (const auto & [_, entries] : coverage_by_materialized_index_part_uuid)
    {
        for (const auto & entry : entries)
        {
            auto [it, inserted] = coverage_by_source_uuid.emplace(entry.source_part_uuid, entry);
            if (!inserted && entry.rows > it->second.rows)
                it->second = entry;
        }
    }
    return coverage_by_source_uuid;
}

std::vector<UUID> collectAffectedMaterializedIndexPartUuids(
    const std::vector<UUID> & source_uuids,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    std::unordered_set<UUID> source_uuid_set(source_uuids.begin(), source_uuids.end());
    std::vector<UUID> affected;

    for (const auto & [materialized_index_part_uuid, entries] : coverage_by_materialized_index_part_uuid)
    {
        for (const auto & entry : entries)
        {
            if (source_uuid_set.contains(entry.source_part_uuid))
            {
                affected.push_back(materialized_index_part_uuid);
                break;
            }
        }
    }

    return affected;
}

void restrictObsoleteCoverageToSingleSourcePartition(
    ReconcileResult & result,
    const std::unordered_map<UUID, CoverageEntry> & coverage_by_source_uuid,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    if (result.candidate_kind != ReconcileCandidateKind::ObsoleteCoverage
        || result.obsolete_coverage.obsolete_source_part_uuids.empty())
        return;

    std::unordered_map<String, std::vector<UUID>> obsolete_by_partition;
    std::vector<UUID> legacy_obsolete_source_uuids;
    for (const auto & uuid : result.obsolete_coverage.obsolete_source_part_uuids)
    {
        auto it = coverage_by_source_uuid.find(uuid);
        if (it == coverage_by_source_uuid.end() || !it->second.has_part_info)
        {
            legacy_obsolete_source_uuids.push_back(uuid);
            continue;
        }

        obsolete_by_partition[it->second.partition_id].push_back(uuid);
    }

    if (obsolete_by_partition.empty() || (obsolete_by_partition.size() == 1 && legacy_obsolete_source_uuids.empty()))
        return;

    auto best_it = std::max_element(
        obsolete_by_partition.begin(),
        obsolete_by_partition.end(),
        [&](const auto & lhs, const auto & rhs)
        {
            UInt64 lhs_rows = 0;
            for (const auto & uuid : lhs.second)
                lhs_rows += coverage_by_source_uuid.at(uuid).rows;

            UInt64 rhs_rows = 0;
            for (const auto & uuid : rhs.second)
                rhs_rows += coverage_by_source_uuid.at(uuid).rows;

            return std::make_tuple(lhs.second.size(), lhs_rows, lhs.first)
                < std::make_tuple(rhs.second.size(), rhs_rows, rhs.first);
        });

    if (best_it == obsolete_by_partition.end())
        return;

    result.delta_out = best_it->second;
    result.obsolete_coverage.obsolete_source_part_uuids = best_it->second;
    result.obsolete_coverage.affected_materialized_index_part_uuids
        = collectAffectedMaterializedIndexPartUuids(best_it->second, coverage_by_materialized_index_part_uuid);

    result.obsolete_coverage.obsolete_rows = 0;
    for (const auto & uuid : best_it->second)
        result.obsolete_coverage.obsolete_rows += coverage_by_source_uuid.at(uuid).rows;
}

template <typename Predicate>
std::vector<UUID> collectFullyCoveringLineageMiParts(
    const std::vector<UUID> & delta_out,
    const ReconcileSourcePart & new_source_part,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid,
    Predicate && entry_can_precede)
{
    std::vector<UUID> selected;
    for (const auto & [materialized_index_part_uuid, entries] : coverage_by_materialized_index_part_uuid)
    {
        std::vector<CoverageEntry> predecessor_entries;
        predecessor_entries.reserve(delta_out.size());

        bool part_fully_covers_lineage = true;
        for (const auto & source_uuid : delta_out)
        {
            auto it = std::find_if(
                entries.begin(),
                entries.end(),
                [&](const CoverageEntry & entry)
                {
                    return entry.source_part_uuid == source_uuid;
                });
            if (it == entries.end() || !entry_can_precede(*it, new_source_part))
            {
                part_fully_covers_lineage = false;
                break;
            }
            predecessor_entries.push_back(*it);
        }

        if (part_fully_covers_lineage && entriesFullyCoverPartRange(std::move(predecessor_entries), new_source_part))
            selected.push_back(materialized_index_part_uuid);
    }
    return selected;
}

bool tryBuildMergeLineageRemap(
    const std::vector<ReconcileSourcePart> & delta_in,
    const std::vector<UUID> & delta_out,
    bool materialized_index_snapshot_non_empty,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid,
    ReconcileResult & result)
{
    if (delta_in.size() != 1 || delta_out.empty() || !materialized_index_snapshot_non_empty)
        return false;

    const auto & new_source_part = delta_in.front();
    if (!new_source_part.has_part_info)
        return false;

    auto selected_materialized_index_part_uuids = collectFullyCoveringLineageMiParts(
        delta_out,
        new_source_part,
        coverage_by_materialized_index_part_uuid,
        entryCanPrecedeMergePart);
    if (selected_materialized_index_part_uuids.empty())
        return false;

    result.candidate_kind = ReconcileCandidateKind::RemapLineage;
    result.remap_kind = MaterializedIndexRemapKind::MergeLineage;
    result.remap_lineage.remap_kind = MaterializedIndexRemapKind::MergeLineage;
    result.remap_lineage.old_materialized_index_part_uuids = std::move(selected_materialized_index_part_uuids);
    result.remap_lineage.old_source_part_uuids = delta_out;
    result.remap_lineage.new_source_part = new_source_part.part;
    result.remap_lineage.new_source_part_uuid = new_source_part.uuid;
    result.has_remap_target = true;
    result.build_batch.source_parts.clear();
    result.build_batch.source_part_uuids.clear();
    return true;
}

bool tryBuildMutationLineageRemap(
    const std::vector<ReconcileSourcePart> & delta_in,
    const std::vector<UUID> & delta_out,
    bool materialized_index_snapshot_non_empty,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid,
    ReconcileResult & result)
{
    if (delta_in.size() != 1 || delta_out.size() != 1 || !materialized_index_snapshot_non_empty)
        return false;

    const auto & new_source_part = delta_in.front();
    if (!new_source_part.has_part_info || !new_source_part.indexed_columns_unchanged_by_mutation)
        return false;

    auto selected_materialized_index_part_uuids = collectFullyCoveringLineageMiParts(
        delta_out,
        new_source_part,
        coverage_by_materialized_index_part_uuid,
        entryCanPrecedeMutationPart);
    if (selected_materialized_index_part_uuids.empty())
        return false;

    result.candidate_kind = ReconcileCandidateKind::RemapLineage;
    result.remap_kind = MaterializedIndexRemapKind::MutationLineage;
    result.remap_lineage.remap_kind = MaterializedIndexRemapKind::MutationLineage;
    result.remap_lineage.old_materialized_index_part_uuids = std::move(selected_materialized_index_part_uuids);
    result.remap_lineage.old_source_part_uuids = delta_out;
    result.remap_lineage.new_source_part = new_source_part.part;
    result.remap_lineage.new_source_part_uuid = new_source_part.uuid;
    result.has_remap_target = true;
    result.build_batch.source_parts.clear();
    result.build_batch.source_part_uuids.clear();
    return true;
}

}

ReconcileResult SnapshotDiffReconciler::runOnUuids(
    const std::vector<UUID> & source_uuid_list,
    bool materialized_index_snapshot_non_empty,
    const std::unordered_set<UUID> & coverage)
{
    ReconcileResult result;

    std::unordered_set<UUID> source_uuids(source_uuid_list.begin(), source_uuid_list.end());

    /// delta_out: covered UUIDs that no longer exist in the source.
    for (const auto & uuid : coverage)
    {
        if (!source_uuids.contains(uuid))
            result.delta_out.push_back(uuid);
    }

    for (const auto & uuid : source_uuid_list)
    {
        if (!coverage.contains(uuid))
            result.build_batch.source_part_uuids.push_back(uuid);
    }

    if (!result.build_batch.source_part_uuids.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::BuildBatch;
        result.has_build_candidate = true;
    }
    else if (materialized_index_snapshot_non_empty && !result.delta_out.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::ObsoleteCoverage;
        result.remap_kind = MaterializedIndexRemapKind::ObsoleteCoverageCleanup;
        result.obsolete_coverage.obsolete_source_part_uuids = result.delta_out;
    }

    return result;
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_set<UUID> & coverage)
{
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_materialized_index_part_uuid;
    auto & entries = coverage_by_materialized_index_part_uuid[UUID{}];
    for (const auto & uuid : coverage)
    {
        CoverageEntry entry;
        entry.source_part_uuid = uuid;
        entries.emplace_back(std::move(entry));
    }
    return run(source_snapshot, materialized_index_snapshot, coverage_by_materialized_index_part_uuid);
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_map<UUID, CoverageEntry> & coverage_by_source_uuid)
{
    std::unordered_map<UUID, std::vector<CoverageEntry>> coverage_by_materialized_index_part_uuid;
    auto & entries = coverage_by_materialized_index_part_uuid[UUID{}];
    entries.reserve(coverage_by_source_uuid.size());
    for (const auto & [_, entry] : coverage_by_source_uuid)
        entries.push_back(entry);

    return run(source_snapshot, materialized_index_snapshot, coverage_by_materialized_index_part_uuid);
}

ReconcileResult SnapshotDiffReconciler::runOnPartViews(
    const std::vector<ReconcileSourcePart> & source_snapshot,
    bool materialized_index_snapshot_non_empty,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    ReconcileResult result;
    std::vector<ReconcileSourcePart> delta_in_views;

    const auto coverage_by_source_uuid = buildCoverageBySourceUuid(coverage_by_materialized_index_part_uuid);

    std::unordered_set<UUID> source_uuids;
    source_uuids.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
        source_uuids.insert(part.uuid);

    for (const auto & part : source_snapshot)
    {
        if (!coverage_by_source_uuid.contains(part.uuid))
        {
            delta_in_views.push_back(part);
            if (part.part)
            {
                result.delta_in.push_back(part.part);
                result.build_batch.source_parts.push_back(part.part);
            }
            result.build_batch.source_part_uuids.push_back(part.uuid);
        }
    }

    for (const auto & [uuid, _] : coverage_by_source_uuid)
    {
        if (!source_uuids.contains(uuid))
            result.delta_out.push_back(uuid);
    }

    if (tryBuildMergeLineageRemap(
        delta_in_views,
        result.delta_out,
        materialized_index_snapshot_non_empty,
        coverage_by_materialized_index_part_uuid,
        result))
        return result;

    if (tryBuildMutationLineageRemap(
        delta_in_views,
        result.delta_out,
        materialized_index_snapshot_non_empty,
        coverage_by_materialized_index_part_uuid,
        result))
        return result;

    if (delta_in_views.size() == 1 && !result.delta_out.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::RebuildSourcePart;
        result.rebuild_source_part.source_part = delta_in_views.front().part;
        result.rebuild_source_part.source_part_uuid = delta_in_views.front().uuid;
        result.rebuild_source_part.affected_materialized_index_part_uuids
            = collectAffectedMaterializedIndexPartUuids(result.delta_out, coverage_by_materialized_index_part_uuid);
        result.rebuild_source_part.reason = "source part lineage is not fully covered by ready materialized-index-parts";
        result.build_batch.source_parts.clear();
        result.build_batch.source_part_uuids.clear();
        return result;
    }

    if (!result.build_batch.source_part_uuids.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::BuildBatch;
        result.has_build_candidate = true;
    }
    else if (!result.delta_out.empty() && materialized_index_snapshot_non_empty)
    {
        result.candidate_kind = ReconcileCandidateKind::ObsoleteCoverage;
        result.remap_kind = MaterializedIndexRemapKind::ObsoleteCoverageCleanup;
        result.obsolete_coverage.obsolete_source_part_uuids = result.delta_out;
        result.obsolete_coverage.affected_materialized_index_part_uuids
            = collectAffectedMaterializedIndexPartUuids(result.delta_out, coverage_by_materialized_index_part_uuid);
        for (const auto & uuid : result.delta_out)
        {
            auto it = coverage_by_source_uuid.find(uuid);
            if (it != coverage_by_source_uuid.end())
                result.obsolete_coverage.obsolete_rows += it->second.rows;
        }
        restrictObsoleteCoverageToSingleSourcePartition(result, coverage_by_source_uuid, coverage_by_materialized_index_part_uuid);
    }

    return result;
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    auto result = runOnPartViews(
        makeSourcePartViews(source_snapshot),
        !materialized_index_snapshot.empty(),
        coverage_by_materialized_index_part_uuid);

    std::unordered_map<UUID, MergeTreeData::DataPartPtr> materialized_index_parts_by_uuid;
    materialized_index_parts_by_uuid.reserve(materialized_index_snapshot.size());
    for (const auto & part : materialized_index_snapshot)
        if (part)
            materialized_index_parts_by_uuid.emplace(part->uuid, part);

    auto collect_parts = [&](const std::vector<UUID> & uuids)
    {
        MergeTreeData::DataPartsVector parts;
        parts.reserve(uuids.size());
        for (const auto & materialized_index_part_uuid : uuids)
        {
            auto it = materialized_index_parts_by_uuid.find(materialized_index_part_uuid);
            if (it != materialized_index_parts_by_uuid.end())
                parts.push_back(it->second);
        }
        return parts;
    };

    result.remap_lineage.old_materialized_index_parts
        = collect_parts(result.remap_lineage.old_materialized_index_part_uuids);
    result.obsolete_coverage.affected_materialized_index_parts
        = collect_parts(result.obsolete_coverage.affected_materialized_index_part_uuids);
    result.rebuild_source_part.affected_materialized_index_parts
        = collect_parts(result.rebuild_source_part.affected_materialized_index_part_uuids);

    return result;
}

}
