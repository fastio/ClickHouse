#include <Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Common/logger_useful.h>
#include <Core/Field.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartReverseLookup.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <fmt/format.h>

#include <algorithm>
#include <limits>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace Setting
{
    extern const SettingsBool force_using_materialized_index;
    extern const SettingsString force_materialized_index;
    extern const SettingsString disable_materialized_index;
    extern const SettingsUInt64 materialized_index_overfetch_factor;
    extern const SettingsBool materialized_index_require_match;
}

namespace QueryPlanOptimizations
{

void attachMaterializedIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const MaterializedIndexHints & hints)
{
    if (!hints.covered_source_parts.contains(part_uuid))
        return;

    chassert(!read_hints.materialized_index_search_results.has_value());

    auto it = hints.hits_per_part.find(part_uuid);
    if (it != hints.hits_per_part.end())
    {
        read_hints.materialized_index_search_results = it->second;
        return;
    }

    NearestNeighbours empty;
    empty.distances = std::vector<float>{};
    read_hints.materialized_index_search_results = std::move(empty);
}

void applyMaterializedIndexHints(RangesInDataParts & parts, const MaterializedIndexHints & hints)
{
    for (auto & part : parts)
        attachMaterializedIndexHintForPart(part.data_part->uuid, part.read_hints, hints);
}


void setMaterializedIndexHintsAndApplyToAnalyzed(ReadFromMergeTree & rfmt, MaterializedIndexHints hints)
{
    /// The optimizer calls selectRangesToRead for coverage/cost before the
    /// winner is known. If that cached analysis already exists, setting hints
    /// on RFMT alone is too late: apply them to the cached parts as well.
    if (auto analyzed = rfmt.getAnalyzedResult())
        applyMaterializedIndexHints(analyzed->parts_with_ranges, hints);

    rfmt.setMaterializedIndexHints(std::move(hints));
}


/// Unit-testable cost helpers (T7/T8/T9).
///
/// "Equivalent scanned rows" is the shared currency: framework adds a
/// verify_cost (PREWHERE re-evaluation over candidate_limit rows) to the
/// algorithm-reported search cost, then compares against the fallback full-scan.

/// PREWHERE re-evaluation cost per candidate row. Held as a constant rather
/// than a setting until we have evidence calling for tuning per workload.
constexpr double VERIFY_COST_PER_ROW = 1.0;

size_t computeMaterializedIndexTotalCost(
    const AlgorithmCostEstimate & est,
    size_t candidate_limit,
    const CoverageSnapshot & coverage)
{
    constexpr size_t UNION_BRANCH_FIXED_COST = 1024;
    constexpr size_t MATERIALIZED_INDEX_PART_SEARCH_FIXED_COST = 64;

    const auto verify_cost = static_cast<size_t>(static_cast<double>(candidate_limit) * VERIFY_COST_PER_ROW);
    /// rerank_cost = 0 until a second pass introduces ALIAS rewriting.
    size_t total = est.algorithm_search_cost + verify_cost;
    total += coverage.uncovered_source_rows;
    if (coverage.active_source_parts != 0 && !coverage.full_coverage)
        total += UNION_BRANCH_FIXED_COST;
    total += coverage.ready_materialized_index_parts * MATERIALIZED_INDEX_PART_SEARCH_FIXED_COST;
    return total;
}

std::optional<size_t> computeMaterializedIndexCandidateLimit(size_t top_k, UInt64 overfetch_factor)
{
    /// 0 or > 1024 disables the fast path (mirrors useVectorSearch oversize
    /// handling); top_k * factor that would overflow size_t also disables.
    if (top_k == 0 || overfetch_factor == 0 || overfetch_factor > 1024)
        return std::nullopt;
    if (top_k > std::numeric_limits<size_t>::max() / overfetch_factor)
        return std::nullopt;
    return top_k * overfetch_factor;
}

std::optional<size_t> pickMaterializedIndexWinner(
    const std::vector<std::pair<String, size_t>> & scored_by_name,
    const String & force_name,
    size_t fallback_cost,
    LoggerPtr log)
{
    if (scored_by_name.empty())
        return std::nullopt;

    if (!force_name.empty())
    {
        for (size_t i = 0; i < scored_by_name.size(); ++i)
            if (scored_by_name[i].first == force_name)
                return i;
        if (log)
            LOG_WARNING(log,
                "force_materialized_index={} did not match any candidate; falling back to cost-based selection",
                force_name);
    }

    size_t best_idx = 0;
    for (size_t i = 1; i < scored_by_name.size(); ++i)
        if (scored_by_name[i].second < scored_by_name[best_idx].second)
            best_idx = i;

    if (scored_by_name[best_idx].second >= fallback_cost)
        return std::nullopt;
    return best_idx;
}


namespace
{

bool resultNameMatchesColumn(const String & result_name, const String & column_name)
{
    return result_name == column_name || result_name.ends_with("." + column_name);
}


bool nodeDependsOnColumn(const ActionsDAG::Node * node, const String & column_name)
{
    if (!node)
        return false;

    if ((node->type == ActionsDAG::ActionType::INPUT || node->type == ActionsDAG::ActionType::ALIAS)
        && resultNameMatchesColumn(node->result_name, column_name))
        return true;

    for (const auto * child : node->children)
        if (nodeDependsOnColumn(child, column_name))
            return true;

    return false;
}


/// Plan-shape walker: peel off LimitStep -> SortingStep -> ExpressionStep
/// and capture the ReadFromMergeTree node beneath. Returns nullptr on any
/// shape mismatch so the caller can early-return without rewriting the plan.
struct PlanShape
{
    LimitStep * limit_step = nullptr;
    SortingStep * sorting_step = nullptr;
    QueryPlan::Node * expression_node = nullptr;
    ExpressionStep * expression_step = nullptr;
    QueryPlan::Node * rfmt_node = nullptr;
    ReadFromMergeTree * rfmt_step = nullptr;
};

std::optional<PlanShape> walkPlanShape(QueryPlan::Node * parent_node)
{
    PlanShape shape;
    QueryPlan::Node * node = parent_node;

    shape.limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!shape.limit_step || node->children.size() != 1)
        return std::nullopt;
    node = node->children.front();

    shape.sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!shape.sorting_step || node->children.size() != 1)
        return std::nullopt;
    node = node->children.front();

    shape.expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (!shape.expression_step || node->children.size() != 1)
        return std::nullopt;
    shape.expression_node = node;
    node = node->children.front();

    shape.rfmt_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!shape.rfmt_step)
        return std::nullopt;
    shape.rfmt_node = node;
    return shape;
}


/// Output of the ORDER BY analyser: enough to call the algorithm's match
/// and search and to rewrite the ExpressionStep so it consumes the indexed
/// `_distance` column instead of recomputing the distance function.
struct QueryParams
{
    String distance_function;
    String search_column;
    String sort_column;                 /// the FUNCTION node's result name, used to rebuild the DAG
    std::vector<float> reference_vector;
    size_t top_k = 0;
    int sort_direction = 0;
    DataTypePtr sort_column_result_type;
    bool need_distance_cast = false;
};


bool expressionOutputsDependOnSearchColumn(const ExpressionStep & expression_step, const QueryParams & qp)
{
    const ActionsDAG & expression = expression_step.getExpression();
    for (const auto * output : expression.getOutputs())
    {
        /// The ORDER BY distance expression is the one we intentionally
        /// replace with `_distance`.
        if (output->result_name == qp.sort_column)
            continue;
        if (nodeDependsOnColumn(output, qp.search_column))
            return true;
    }
    return false;
}

std::optional<QueryParams> extractQueryParams(const PlanShape & shape)
{
    QueryParams qp;
    qp.top_k = shape.limit_step->getLimitForSorting();
    if (qp.top_k == 0)
        return std::nullopt;

    if (shape.sorting_step->getType() != SortingStep::Type::Full)
        return std::nullopt;

    const auto & sort_description = shape.sorting_step->getSortDescription();
    if (sort_description.size() != 1)
        return std::nullopt;
    qp.sort_column = sort_description.front().column_name;
    qp.sort_direction = sort_description.front().direction;

    ActionsDAG & expression = shape.expression_step->getExpression();
    const ActionsDAG::Node * sort_column_node = expression.tryFindInOutputs(qp.sort_column);
    if (!sort_column_node || sort_column_node->type != ActionsDAG::ActionType::FUNCTION)
        return std::nullopt;

    qp.sort_column_result_type = sort_column_node->result_type;
    qp.need_distance_cast = !WhichDataType(qp.sort_column_result_type).isFloat32();

    const String & function_name = sort_column_node->function_base->getName();
    if (function_name == "L2Distance" || function_name == "cosineDistance" || function_name == "dotProduct")
        qp.distance_function = function_name;
    else
        return std::nullopt;

    if ((qp.distance_function == "L2Distance" || qp.distance_function == "cosineDistance") && qp.sort_direction != 1)
        return std::nullopt;
    if (qp.distance_function == "dotProduct" && qp.sort_direction != -1)
        return std::nullopt;

    /// Pull the search column name + the reference-vector literal out of the
    /// FUNCTION node's children. Mirrors useVectorSearch's analyzer logic.
    for (const auto * child : sort_column_node->children)
    {
        if (child->type == ActionsDAG::ActionType::ALIAS)
        {
            const auto * inner = child->children.at(0);
            if (inner->type == ActionsDAG::ActionType::INPUT)
                qp.search_column = inner->result_name;
        }
        else if (child->type == ActionsDAG::ActionType::INPUT)
        {
            qp.search_column = child->result_name;
            const auto dot = qp.search_column.find('.');
            if (dot != String::npos)
                qp.search_column = qp.search_column.substr(dot + 1);
        }
        else if (child->type == ActionsDAG::ActionType::COLUMN)
        {
            const DataTypePtr & data_type = child->result_type;
            const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
            if (!data_type_array)
                continue;
            const ColumnPtr & column = child->column;
            const auto * literal_column = typeid_cast<const ColumnConst *>(column.get());
            if (!literal_column || literal_column->size() != 1)
                continue;
            Field field;
            literal_column->get(0, field);
            if (field.getType() != Field::Types::Array)
                continue;
            for (const auto & v : field.safeGet<Array>())
            {
                if (v.getType() != Field::Types::Float64)
                    return std::nullopt;
                qp.reference_vector.push_back(static_cast<float>(v.safeGet<Float64>()));
            }
        }
    }

    if (qp.search_column.empty() || qp.reference_vector.empty())
        return std::nullopt;
    return qp;
}


bool sourceHasVectorSimilarityIndex(const StorageMetadataPtr & metadata, const String & search_column)
{
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


std::vector<StorageMaterializedIndex *> findMaterializedIndexCandidates(
    const StorageID & source_id,
    const ContextPtr & context,
    const String & search_column,
    std::vector<StoragePtr> & owners_out)
{
    std::vector<StorageMaterializedIndex *> result;
    auto & catalog = DatabaseCatalog::instance();
    auto deps = catalog.getReferentialDependents(source_id);
    for (const auto & dep_id : deps)
    {
        auto dep_storage = catalog.tryGetTable(dep_id, context);
        if (!dep_storage)
            continue;
        auto * materialized_index = typeid_cast<StorageMaterializedIndex *>(dep_storage.get());
        if (!materialized_index)
            continue;
        const auto & indexed_columns = materialized_index->getIndexedColumns();
        if (indexed_columns.size() != 1 || indexed_columns.front() != search_column)
            continue;
        result.push_back(materialized_index);
        owners_out.push_back(std::move(dep_storage));
    }
    return result;
}


bool fullyCoversActiveSourceParts(const RangesInDataParts & active_parts, const std::unordered_set<UUID> & covered_source_parts)
{
    for (const auto & p : active_parts)
    {
        if (!covered_source_parts.contains(p.data_part->uuid))
            return false;
    }
    return true;
}


/// Replace the FUNCTION result feeding into ORDER BY with the indexed
/// `_distance` virtual column. Mirrors useVectorSearch's second pass so
/// the rest of the pipeline keeps working unchanged.
void rewriteExpressionForDistanceVirtual(
    QueryPlan::Node * expression_node,
    SortingStep & sorting_step,
    SharedHeader child_output_header,
    const QueryParams & qp)
{
    auto * expression_step = typeid_cast<ExpressionStep *>(expression_node->step.get());
    chassert(expression_step != nullptr);
    ActionsDAG & expression = expression_step->getExpression();

    expression.removeUnusedResult(qp.sort_column);
    expression.removeUnusedActions();

    const auto * distance_node = &expression.addInput("_distance", std::make_shared<DataTypeFloat32>());
    if (qp.need_distance_cast)
        distance_node = &expression.addCast(*distance_node, qp.sort_column_result_type, "_CAST_distance", nullptr);

    const auto * new_output = &expression.addAlias(*distance_node, qp.sort_column);
    expression.getOutputs().push_back(new_output);

    auto new_step = std::make_unique<ExpressionStep>(std::move(child_output_header), std::move(expression));
    new_step->setStepDescription(*expression_node->step);
    expression_node->step = std::move(new_step);

    sorting_step.updateInputHeader(expression_node->step->getOutputHeader());
}


/// Build a literal Array(Float32) constant column wrapping `values` so the
/// ActionsDAG can pass it as the second argument of the distance function.
ColumnPtr makeQueryVectorConstColumn(const std::vector<float> & values)
{
    auto inner = ColumnVector<Float32>::create();
    inner->getData().assign(values.begin(), values.end());

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(values.size());

    return ColumnConst::create(ColumnArray::create(std::move(inner), std::move(offsets)), 1);
}


/// Construct an ExpressionStep that takes the uncovered RFMT's output and
/// produces (user_columns - search_column, _distance) where _distance is
/// computed via the same distance function the user wrote in ORDER BY.
ExpressionStep buildDistanceExpressionForUncovered(
    SharedHeader input_header,
    const QueryParams & qp,
    const ContextPtr & context)
{
    ActionsDAG dag;

    std::unordered_map<String, const ActionsDAG::Node *> by_name;
    for (const auto & col : input_header->getColumnsWithTypeAndName())
        by_name[col.name] = &dag.addInput(col);

    auto search_it = by_name.find(qp.search_column);
    if (search_it == by_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndex partial-coverage: search column {} missing from uncovered RFMT output",
            qp.search_column);

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    auto literal_col = makeQueryVectorConstColumn(qp.reference_vector);
    const auto & literal_node = dag.addColumn({std::move(literal_col), array_type, "__mi_query_vector"});

    auto func = FunctionFactory::instance().get(qp.distance_function, context);
    const auto & raw_distance = dag.addFunction(func, {search_it->second, &literal_node}, "");

    const ActionsDAG::Node * distance_node = &raw_distance;
    if (!WhichDataType(distance_node->result_type).isFloat32())
        distance_node = &dag.addCast(*distance_node, std::make_shared<DataTypeFloat32>(), "__mi_cast_distance", context);

    const auto & distance_alias = dag.addAlias(*distance_node, "_distance");

    auto & outputs = dag.getOutputs();
    outputs.clear();
    for (const auto & col : input_header->getColumnsWithTypeAndName())
    {
        if (col.name == qp.search_column)
            continue;
        outputs.push_back(by_name[col.name]);
    }
    outputs.push_back(&distance_alias);

    return ExpressionStep(std::move(input_header), std::move(dag));
}


/// Clone `original` and override its analysed result so the clone reads only
/// the source parts that pass `keep`. The clone owns its own analysed result;
/// the caller may further mutate the returned RFMT (replaceVectorColumn,
/// setMaterializedIndexHints, ...).
std::unique_ptr<ReadFromMergeTree> cloneRfmtWithFilteredParts(
    const ReadFromMergeTree & original,
    const ReadFromMergeTree::AnalysisResult & full,
    const std::function<bool(const RangesInDataPart &)> & keep)
{
    auto cloned_step = original.clone();
    auto * cloned_rfmt = typeid_cast<ReadFromMergeTree *>(cloned_step.get());
    chassert(cloned_rfmt != nullptr);
    cloned_step.release();
    std::unique_ptr<ReadFromMergeTree> cloned(cloned_rfmt);

    auto filtered = std::make_shared<ReadFromMergeTree::AnalysisResult>(full);
    auto & parts = filtered->parts_with_ranges;
    parts.erase(
        std::remove_if(parts.begin(), parts.end(), [&keep](const auto & p) { return !keep(p); }),
        parts.end());
    cloned->setAnalyzedResult(std::move(filtered));

    return cloned;
}


ReadyMaterializedIndexPartSnapshot buildReadySnapshot(
    const MergeTreeData::DataPartsVector & ready_materialized_index_parts_data,
    LoggerPtr log)
{
    ReadyMaterializedIndexPartSnapshot snapshot;
    snapshot.parts.reserve(ready_materialized_index_parts_data.size());
    for (const auto & part : ready_materialized_index_parts_data)
    {
        if (!part)
            continue;

        ReadyMaterializedIndexPart ready_part;
        ready_part.storage = part->getDataPartStoragePtr();
        try
        {
            for (const auto & entry : StorageMaterializedIndex::parseCoverageJsonFromMiPart(*part))
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


std::unordered_set<UUID> coveredActiveSourceParts(
    const ReadyMaterializedIndexPartSnapshot & ready_snapshot,
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid)
{
    std::unordered_set<UUID> covered;
    for (const auto & ready_part : ready_snapshot.parts)
    {
        for (const auto & entry : ready_part.covered_source_parts)
        {
            auto active_it = active_metadata_by_uuid.find(entry.source_part_uuid);
            if (active_it == active_metadata_by_uuid.end())
                continue;

            const auto & active = active_it->second;
            if (entry.rows != active.rows)
                continue;
            if (entry.has_part_info
                && (entry.partition_id != active.partition_id
                    || entry.min_block != active.min_block
                    || entry.max_block != active.max_block
                    || entry.level != active.level
                    || entry.mutation != active.mutation))
                continue;

            covered.insert(entry.source_part_uuid);
        }
    }
    return covered;
}


CoverageSnapshot buildCoverageSnapshot(
    const std::unordered_map<UUID, ActiveSourcePartMetadata> & active_metadata_by_uuid,
    const ReadyMaterializedIndexPartSnapshot & ready_snapshot,
    size_t candidate_limit)
{
    CoverageSnapshot snapshot;
    snapshot.active_source_parts = active_metadata_by_uuid.size();
    snapshot.ready_materialized_index_parts = ready_snapshot.parts.size();
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

    for (const auto & hit_set : internal_result.per_materialized_index_part)
    {
        if (!hit_set.materialized_index_part_storage)
            continue;
        if (hit_set.internal_ids.size() != hit_set.distances.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MaterializedIndex search returned {} internal ids but {} distances",
                hit_set.internal_ids.size(), hit_set.distances.size());

        MaterializedIndexPartReverseLookup lookup(*hit_set.materialized_index_part_storage);
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


MaterializedIndexHints buildHintsForCoveredSourceParts(
    const SourceSearchResult & source_result,
    const std::unordered_set<UUID> & covered_source_parts)
{
    MaterializedIndexHints hints;
    hints.covered_source_parts = covered_source_parts;
    hints.hits_per_part.reserve(source_result.hits_per_part.size());

    for (const auto & set : source_result.hits_per_part)
    {
        if (!hints.covered_source_parts.contains(set.source_part_uuid))
            continue;

        NearestNeighbours hits;
        hits.rows = set.part_offsets;
        hits.distances = set.distances;
        hints.hits_per_part.emplace(set.source_part_uuid, std::move(hits));
    }

    return hints;
}

}


size_t tryUseMaterializedIndex(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & /*settings*/)
{
    constexpr size_t no_layers_updated = 0;

    auto shape = walkPlanShape(parent_node);
    if (!shape)
        return no_layers_updated;

    auto & rfmt = *shape->rfmt_step;

    /// `materialized_index_require_match` (strict mode) takes effect once we
    /// know we're looking at an ANN-shaped query — i.e. after walkPlanShape
    /// matched and extractQueryParams succeeded. Earlier filters (parallel
    /// replicas, PREWHERE) still throw under strict mode because the user's
    /// contract is "this query MUST go through a MaterializedIndex"; if we
    /// can't honour that, silently falling back to a brute-force scan is
    /// exactly what strict mode is meant to prevent.
    auto context = rfmt.getContext();
    const bool require_match = context && context->getSettingsRef()[Setting::materialized_index_require_match];
    auto give_up = [&](std::string_view reason) -> size_t
    {
        if (require_match)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "materialized_index_require_match is set but no MaterializedIndex rewrite was applied: {}",
                reason);
        return no_layers_updated;
    };

    if (rfmt.isParallelReadingFromReplicas())
        return give_up("parallel reading from replicas is enabled");
    if (rfmt.getPrewhereInfo())
        return give_up("query has PREWHERE which the MaterializedIndex rewrite does not support");

    auto qp = extractQueryParams(*shape);
    if (!qp)
        return no_layers_updated;
    if (expressionOutputsDependOnSearchColumn(*shape->expression_step, *qp))
        return give_up("query expression output depends on the search column");

    if (!context)
        return no_layers_updated;

    const bool force_mi = context->getSettingsRef()[Setting::force_using_materialized_index];
    const auto storage_metadata = rfmt.getStorageMetadata();

    if (!force_mi && sourceHasVectorSimilarityIndex(storage_metadata, qp->search_column))
        return give_up("source has a vector similarity index; set force_using_materialized_index=1 to prefer MI");

    std::vector<StoragePtr> materialized_index_owners;
    auto materialized_index_candidates = findMaterializedIndexCandidates(rfmt.getStorageID(), context, qp->search_column, materialized_index_owners);
    if (materialized_index_candidates.empty())
        return give_up("no MaterializedIndex is registered on the source for the search column");

    const auto & settings_ref = context->getSettingsRef();
    const String force_name = settings_ref[Setting::force_materialized_index];
    const String disable_name = settings_ref[Setting::disable_materialized_index];

    if (!disable_name.empty())
        std::erase_if(materialized_index_candidates, [&](auto * cand) { return cand->getStorageID().getTableName() == disable_name; });
    if (materialized_index_candidates.empty())
        return give_up("all MaterializedIndex candidates were excluded by disable_materialized_index");

    QueryFeatures features;
    features.query_vector = qp->reference_vector;
    features.k = qp->top_k;

    /// Cost-based winner selection. First match every candidate; collect the
    /// successful matches together with their algorithm-reported cost. Sort
    /// ascending by MI name so cost ties resolve deterministically.
    struct ScoredCandidate
    {
        StorageMaterializedIndex * materialized_index;
        MatchDescriptor desc;
        ReadyMaterializedIndexPartSnapshot ready_snapshot;
        CoverageSnapshot coverage;
        std::unordered_set<UUID> covered_source_parts;
        size_t cost;
    };
    std::vector<ScoredCandidate> scored;
    scored.reserve(materialized_index_candidates.size());

    const auto overfetch_factor = settings_ref[Setting::materialized_index_overfetch_factor];
    const auto candidate_limit = computeMaterializedIndexCandidateLimit(qp->top_k, overfetch_factor);
    if (!candidate_limit)
        return give_up("materialized_index_overfetch_factor is outside the supported range");

    /// Fallback cost is the source full-scan in rows. The analysed result is
    /// reused below for both coverage/cost modelling and branch splitting.
    auto analyzed = rfmt.selectRangesToRead();
    if (!analyzed)
        return give_up("could not analyse the source ranges to read");

    size_t fallback_cost = 0;
    for (const auto & p : analyzed->parts_with_ranges)
        fallback_cost += p.data_part->rows_count;
    if (fallback_cost == 0)
        fallback_cost = std::numeric_limits<size_t>::max();

    const auto active_metadata_by_uuid = activeSourceMetadataByUuid(analyzed->parts_with_ranges);
    auto log = getLogger("optimizeMaterializedIndex");

    for (auto * cand : materialized_index_candidates)
    {
        auto * algo = cand->getAlgorithm();
        if (!algo)
            continue;
        auto desc = algo->match(features);
        if (!desc.has_value())
            continue;

        auto ready_materialized_index_parts_data = cand->getAccessPathPartsVectorForInternalUsage();
        if (ready_materialized_index_parts_data.empty())
            continue;

        auto ready_snapshot = buildReadySnapshot(ready_materialized_index_parts_data, log);
        if (ready_snapshot.parts.empty())
            continue;

        auto covered_source_parts = coveredActiveSourceParts(ready_snapshot, active_metadata_by_uuid);
        if (covered_source_parts.empty())
            continue;

        auto coverage = buildCoverageSnapshot(active_metadata_by_uuid, ready_snapshot, *candidate_limit);
        const auto cost_estimate = algo->estimateCost(*desc, coverage);
        scored.push_back({
            cand,
            std::move(*desc),
            std::move(ready_snapshot),
            coverage,
            std::move(covered_source_parts),
            computeMaterializedIndexTotalCost(cost_estimate, *candidate_limit, coverage)});
    }

    if (scored.empty())
        return give_up("every MaterializedIndex candidate rejected the query or had no ready parts covering the source");

    std::sort(scored.begin(), scored.end(),
        [](const auto & a, const auto & b) { return a.materialized_index->getStorageID().getTableName() < b.materialized_index->getStorageID().getTableName(); });

    std::vector<std::pair<String, size_t>> scored_view;
    scored_view.reserve(scored.size());
    for (const auto & sc : scored)
        scored_view.emplace_back(sc.materialized_index->getStorageID().getTableName(), sc.cost);

    auto winner_idx = pickMaterializedIndexWinner(
        scored_view, force_name, fallback_cost, log);
    if (!winner_idx)
        return give_up("MaterializedIndex cost model declined every candidate (source scan was cheaper or force_materialized_index missed)");

    StorageMaterializedIndex * winner = scored[*winner_idx].materialized_index;
    auto winning_desc = std::move(scored[*winner_idx].desc);
    auto ready_snapshot = std::move(scored[*winner_idx].ready_snapshot);
    auto covered_source_parts = std::move(scored[*winner_idx].covered_source_parts);

    InternalSearchResult internal_result = winner->getAlgorithm()->search(winning_desc, ready_snapshot, *candidate_limit, context);
    SourceSearchResult source_result = translateInternalHitsToSourceRows(internal_result);
    MaterializedIndexHints hints = buildHintsForCoveredSourceParts(source_result, covered_source_parts);
    if (hints.covered_source_parts.empty())
        return give_up("MaterializedIndex search returned no hits for any covered source part");

    const bool full_coverage = fullyCoversActiveSourceParts(analyzed->parts_with_ranges, covered_source_parts);

    if (full_coverage)
    {
        rfmt.replaceVectorColumnWithDistanceColumn(qp->search_column);
        setMaterializedIndexHintsAndApplyToAnalyzed(rfmt, std::move(hints));
        rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, rfmt.getOutputHeader(), *qp);
        return no_layers_updated;
    }

    /// Partial coverage splits the read into an MI branch and a brute-force
    /// branch over uncovered source parts. The uncovered branch is exactly
    /// the source scan that strict mode is meant to forbid.
    if (require_match)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "materialized_index_require_match is set but MaterializedIndex only covers a subset of source parts; "
            "the remaining parts would be served by a brute-force scan");

    auto in_hints = [&covered_source_parts](const auto & p) { return covered_source_parts.contains(p.data_part->uuid); };
    auto not_in_hints = [&covered_source_parts](const auto & p) { return !covered_source_parts.contains(p.data_part->uuid); };

    auto covered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, in_hints);
    auto uncovered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, not_in_hints);

    covered_rfmt->replaceVectorColumnWithDistanceColumn(qp->search_column);
    setMaterializedIndexHintsAndApplyToAnalyzed(*covered_rfmt, std::move(hints));

    auto uncovered_input_header = uncovered_rfmt->getOutputHeader();
    auto uncovered_expression = buildDistanceExpressionForUncovered(uncovered_input_header, *qp, context);

    auto covered_header = covered_rfmt->getOutputHeader();

    /// Build the new node arena entries. std::list keeps pointers stable, so
    /// we can take addresses immediately and wire them into the rewritten
    /// shape->rfmt_node below.
    auto & covered_node = nodes.emplace_back();
    covered_node.step = std::move(covered_rfmt);

    auto & uncovered_rfmt_node = nodes.emplace_back();
    uncovered_rfmt_node.step = std::move(uncovered_rfmt);

    auto & uncovered_expr_node = nodes.emplace_back();
    uncovered_expr_node.step = std::make_unique<ExpressionStep>(std::move(uncovered_expression));
    uncovered_expr_node.children = {&uncovered_rfmt_node};

    auto uncovered_branch_header = uncovered_expr_node.step->getOutputHeader();

    SharedHeaders union_headers{covered_header, uncovered_branch_header};
    auto union_step = std::make_unique<UnionStep>(std::move(union_headers), 0);
    auto union_output_header = union_step->getOutputHeader();
    shape->rfmt_node->step = std::move(union_step);
    shape->rfmt_node->children = {&covered_node, &uncovered_expr_node};

    rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, union_output_header, *qp);
    return no_layers_updated;
}

}
}
