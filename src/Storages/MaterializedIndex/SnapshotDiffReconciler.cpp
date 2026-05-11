#include <Storages/MaterializedIndex/SnapshotDiffReconciler.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

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

    /// has_build_candidate: the index is empty and the source has data.
    result.has_build_candidate = !source_uuids.empty() && !materialized_index_snapshot_non_empty;

    /// delta_in proxy for has_remap_target: any source UUID outside coverage.
    bool has_delta_in_uuid = false;
    for (const auto & uuid : source_uuids)
    {
        if (!coverage.contains(uuid))
        {
            has_delta_in_uuid = true;
            break;
        }
    }

    result.has_remap_target = materialized_index_snapshot_non_empty
        && (has_delta_in_uuid || !result.delta_out.empty());

    return result;
}

ReconcileResult SnapshotDiffReconciler::run(
    const MergeTreeData::DataPartsVector & source_snapshot,
    const MergeTreeData::DataPartsVector & materialized_index_snapshot,
    const std::unordered_set<UUID> & coverage)
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
        if (!coverage.contains(part->uuid))
            result.delta_in.push_back(part);
    }

    /// delta_out: covered UUIDs that no longer exist in the source.
    for (const auto & uuid : coverage)
    {
        if (!source_uuids.contains(uuid))
            result.delta_out.push_back(uuid);
    }

    /// has_build_candidate: the index is empty and the source has data, so a
    /// fresh Build is the right next step.
    result.has_build_candidate = !source_snapshot.empty() && materialized_index_snapshot.empty();

    /// has_remap_target: the index already has parts and the source moved on
    /// (something was added or removed), so a Remap should be considered.
    result.has_remap_target = !materialized_index_snapshot.empty()
        && (!result.delta_in.empty() || !result.delta_out.empty());

    return result;
}

}
