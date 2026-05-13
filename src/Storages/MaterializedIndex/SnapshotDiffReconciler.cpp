#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include <algorithm>


namespace DB
{

namespace
{

bool entryCanPrecedePart(const CoverageEntry & entry, const IMergeTreeDataPart & part)
{
    return entry.has_part_info
        && entry.partition_id == part.info.getPartitionId()
        && entry.mutation == part.info.mutation
        && entry.min_block >= part.info.min_block
        && entry.max_block <= part.info.max_block
        && entry.level < part.info.level;
}

bool entriesFullyCoverPartRange(std::vector<CoverageEntry> entries, const IMergeTreeDataPart & part)
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

    Int64 expected_min_block = part.info.min_block;
    for (const auto & entry : entries)
    {
        if (entry.min_block != expected_min_block)
            return false;
        if (entry.max_block < entry.min_block)
            return false;
        expected_min_block = entry.max_block + 1;
    }

    return expected_min_block == part.info.max_block + 1;
}

bool tryBuildStrictRemapLineage(
    const MergeTreeData::DataPartsVector & delta_in,
    const std::vector<UUID> & delta_out,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_map<UUID, CoverageEntry> & coverage_by_source_uuid,
    ReconcileResult & result)
{
    if (delta_in.size() != 1 || delta_out.empty() || materialized_index_snapshot.empty())
        return false;

    const auto & new_source_part = delta_in.front();
    if (!new_source_part)
        return false;

    std::vector<CoverageEntry> predecessor_entries;
    predecessor_entries.reserve(delta_out.size());
    for (const auto & uuid : delta_out)
    {
        auto it = coverage_by_source_uuid.find(uuid);
        if (it == coverage_by_source_uuid.end() || !entryCanPrecedePart(it->second, *new_source_part))
            return false;
        predecessor_entries.push_back(it->second);
    }

    if (!entriesFullyCoverPartRange(std::move(predecessor_entries), *new_source_part))
        return false;

    result.candidate_kind = ReconcileCandidateKind::RemapLineage;
    result.remap_lineage.old_materialized_index_parts = materialized_index_snapshot;
    result.remap_lineage.old_materialized_index_part_uuids.reserve(materialized_index_snapshot.size());
    for (const auto & part : materialized_index_snapshot)
        if (part)
            result.remap_lineage.old_materialized_index_part_uuids.push_back(part->uuid);
    result.remap_lineage.old_source_part_uuids = delta_out;
    result.remap_lineage.new_source_part = new_source_part;
    result.remap_lineage.new_source_part_uuid = new_source_part->uuid;
    result.has_remap_target = true;
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
        result.obsolete_coverage.obsolete_source_part_uuids = result.delta_out;
    }

    return result;
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_set<UUID> & coverage)
{
    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    coverage_by_source_uuid.reserve(coverage.size());
    for (const auto & uuid : coverage)
    {
        CoverageEntry entry;
        entry.source_part_uuid = uuid;
        coverage_by_source_uuid.emplace(uuid, std::move(entry));
    }
    return run(source_snapshot, materialized_index_snapshot, coverage_by_source_uuid);
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_map<UUID, CoverageEntry> & coverage_by_source_uuid)
{
    ReconcileResult result;

    /// Build a UUID set over the live source snapshot once so that delta_out
    /// (uuids in coverage but absent from source) is computed in linear
    /// time.
    std::unordered_set<UUID> source_uuids;
    source_uuids.reserve(source_snapshot.size());
    for (const auto & part : source_snapshot)
        source_uuids.insert(part->uuid);

    /// delta_in: source parts whose UUID has no covering materialized-index-part yet.
    for (const auto & part : source_snapshot)
    {
        if (!coverage_by_source_uuid.contains(part->uuid))
        {
            result.delta_in.push_back(part);
            result.build_batch.source_parts.push_back(part);
            result.build_batch.source_part_uuids.push_back(part->uuid);
        }
    }

    /// delta_out: covered UUIDs that no longer exist in the source.
    for (const auto & [uuid, _] : coverage_by_source_uuid)
    {
        if (!source_uuids.contains(uuid))
            result.delta_out.push_back(uuid);
    }

    if (tryBuildStrictRemapLineage(
        result.delta_in,
        result.delta_out,
        materialized_index_snapshot,
        coverage_by_source_uuid,
        result))
    {
        result.build_batch.source_parts.clear();
        result.build_batch.source_part_uuids.clear();
        return result;
    }

    if (result.delta_in.size() == 1 && !result.delta_out.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::RebuildSourcePart;
        result.rebuild_source_part.source_part = result.delta_in.front();
        result.rebuild_source_part.reason = "source part lineage is not fully covered by ready materialized-index-parts";
        result.build_batch.source_parts.clear();
        result.build_batch.source_part_uuids.clear();
        return result;
    }

    if (!result.build_batch.source_parts.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::BuildBatch;
        result.has_build_candidate = true;
    }
    else if (!result.delta_out.empty() && !materialized_index_snapshot.empty())
    {
        result.candidate_kind = ReconcileCandidateKind::ObsoleteCoverage;
        result.obsolete_coverage.obsolete_source_part_uuids = result.delta_out;
        for (const auto & uuid : result.delta_out)
        {
            auto it = coverage_by_source_uuid.find(uuid);
            if (it != coverage_by_source_uuid.end())
                result.obsolete_coverage.obsolete_rows += it->second.rows;
        }
    }

    return result;
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_map<UUID, std::vector<CoverageEntry>> & coverage_by_materialized_index_part_uuid)
{
    std::unordered_map<UUID, CoverageEntry> coverage_by_source_uuid;
    std::unordered_map<UUID, std::vector<UUID>> source_to_materialized_index_part_uuids;

    for (const auto & [materialized_index_part_uuid, entries] : coverage_by_materialized_index_part_uuid)
    {
        for (const auto & entry : entries)
        {
            auto [it, inserted] = coverage_by_source_uuid.emplace(entry.source_part_uuid, entry);
            if (!inserted && entry.rows > it->second.rows)
                it->second = entry;
            source_to_materialized_index_part_uuids[entry.source_part_uuid].push_back(materialized_index_part_uuid);
        }
    }

    auto result = run(source_snapshot, materialized_index_snapshot, coverage_by_source_uuid);
    if (result.candidate_kind != ReconcileCandidateKind::ObsoleteCoverage
        && result.candidate_kind != ReconcileCandidateKind::RemapLineage
        && result.candidate_kind != ReconcileCandidateKind::RebuildSourcePart)
        return result;

    std::unordered_map<UUID, MergeTreeData::DataPartPtr> materialized_index_parts_by_uuid;
    materialized_index_parts_by_uuid.reserve(materialized_index_snapshot.size());
    for (const auto & part : materialized_index_snapshot)
        if (part)
            materialized_index_parts_by_uuid.emplace(part->uuid, part);

    /// `delta_out` is the canonical "obsolete source uuids" list computed by the
    /// inner run(). The per-kind `obsolete_coverage.obsolete_source_part_uuids`
    /// field is populated only on the ObsoleteCoverage branch, so reading
    /// `delta_out` here lets RemapLineage and RebuildSourcePart resolve their
    /// affected materialized-index parts too.
    std::unordered_set<UUID> affected_materialized_index_part_uuids;
    for (const auto & source_uuid : result.delta_out)
    {
        auto source_it = source_to_materialized_index_part_uuids.find(source_uuid);
        if (source_it == source_to_materialized_index_part_uuids.end())
            continue;

        for (const auto & materialized_index_part_uuid : source_it->second)
            affected_materialized_index_part_uuids.insert(materialized_index_part_uuid);
    }

    auto collect_affected_parts = [&]
    {
        MergeTreeData::DataPartsVector parts;
        std::vector<UUID> uuids(affected_materialized_index_part_uuids.begin(), affected_materialized_index_part_uuids.end());
        parts.reserve(uuids.size());
        for (const auto & materialized_index_part_uuid : uuids)
        {
            auto it = materialized_index_parts_by_uuid.find(materialized_index_part_uuid);
            if (it != materialized_index_parts_by_uuid.end())
                parts.push_back(it->second);
        }
        return std::pair{std::move(uuids), std::move(parts)};
    };

    auto [affected_uuids, affected_parts] = collect_affected_parts();
    if (result.candidate_kind == ReconcileCandidateKind::ObsoleteCoverage)
    {
        result.obsolete_coverage.affected_materialized_index_part_uuids = affected_uuids;
        result.obsolete_coverage.affected_materialized_index_parts = affected_parts;
    }
    else if (result.candidate_kind == ReconcileCandidateKind::RemapLineage)
    {
        result.remap_lineage.old_materialized_index_part_uuids = affected_uuids;
        result.remap_lineage.old_materialized_index_parts = affected_parts;
    }
    else if (result.candidate_kind == ReconcileCandidateKind::RebuildSourcePart)
    {
        result.rebuild_source_part.affected_materialized_index_part_uuids = affected_uuids;
        result.rebuild_source_part.affected_materialized_index_parts = affected_parts;
    }

    return result;
}

}
