#pragma once

#include <Storages/Reflection/ANNIndex/SnapshotDiffReconciler.h>

#include <utility>


namespace DB
{

enum class ANNIndexSchedulerDecisionKind
{
    Nothing,
    BuildBatch,
    RemapLineage,
    RebuildSourcePart,
    ObsoleteCoverageCleanup,
    CompactRebuild,
    CompactMerge,
};

struct ANNIndexSchedulerDecision
{
    ANNIndexSchedulerDecisionKind kind = ANNIndexSchedulerDecisionKind::Nothing;
    ANNIndexRemapKind remap_kind = ANNIndexRemapKind::None;
    MergeTreeData::DataPartsVector source_parts;
    MergeTreeData::DataPartsVector ann_index_parts;
    std::vector<UUID> delta_out_source_uuids;
    String reason;
};

class ANNIndexSchedulerPolicy
{
public:
    static ANNIndexSchedulerDecision choose(
        const ReconcileResult & reconciled,
        MergeTreeData::DataPartsVector build_batch,
        bool compact_rebuild_candidate,
        MergeTreeData::DataPartsVector compact_source_parts,
        MergeTreeData::DataPartsVector compact_ann_index_parts)
    {
        ANNIndexSchedulerDecision decision;

        if (reconciled.candidate_kind == ReconcileCandidateKind::RemapLineage)
        {
            decision.kind = ANNIndexSchedulerDecisionKind::RemapLineage;
            decision.remap_kind = reconciled.remap_lineage.remap_kind;
            decision.source_parts = reconciled.delta_in;
            decision.ann_index_parts = reconciled.remap_lineage.old_ann_index_parts;
            decision.delta_out_source_uuids = reconciled.delta_out;
            decision.reason = "source part lineage can be remapped safely";
            return decision;
        }

        if (!build_batch.empty())
        {
            decision.kind = ANNIndexSchedulerDecisionKind::BuildBatch;
            decision.source_parts = std::move(build_batch);
            decision.reason = "uncovered source parts are ready for batch build";
            return decision;
        }

        if (reconciled.candidate_kind == ReconcileCandidateKind::RebuildSourcePart
            && reconciled.rebuild_source_part.source_part)
        {
            decision.kind = ANNIndexSchedulerDecisionKind::RebuildSourcePart;
            decision.source_parts.push_back(reconciled.rebuild_source_part.source_part);
            decision.ann_index_parts = reconciled.rebuild_source_part.affected_ann_index_parts;
            decision.delta_out_source_uuids = reconciled.delta_out;
            decision.reason = reconciled.rebuild_source_part.reason;
            return decision;
        }

        if (reconciled.candidate_kind == ReconcileCandidateKind::ObsoleteCoverage
            && !reconciled.obsolete_coverage.affected_ann_index_parts.empty())
        {
            decision.kind = ANNIndexSchedulerDecisionKind::ObsoleteCoverageCleanup;
            decision.remap_kind = ANNIndexRemapKind::ObsoleteCoverageCleanup;
            decision.ann_index_parts = reconciled.obsolete_coverage.affected_ann_index_parts;
            decision.delta_out_source_uuids = reconciled.obsolete_coverage.obsolete_source_part_uuids;
            decision.reason = "ready materialized-index-parts contain obsolete source coverage";
            return decision;
        }

        if (compact_rebuild_candidate)
        {
            decision.kind = ANNIndexSchedulerDecisionKind::CompactRebuild;
            decision.source_parts = std::move(compact_source_parts);
            decision.ann_index_parts = std::move(compact_ann_index_parts);
            decision.reason = "compact policy selected a rebuild of ready materialized-index-parts";
            return decision;
        }

        return decision;
    }
};

}
