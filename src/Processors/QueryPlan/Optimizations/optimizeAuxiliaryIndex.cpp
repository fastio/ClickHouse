#include <Processors/QueryPlan/Optimizations/optimizeAuxiliaryIndex.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
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
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/AuxiliaryIndex/IAuxiliaryIndexAlgorithm.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartReverseLookup.h>
#include <Storages/AuxiliaryIndex/StorageANN.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/SelectQueryInfo.h>

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
    extern const SettingsBool force_using_auxiliary_index;
    extern const SettingsString force_auxiliary_index;
    extern const SettingsString disable_auxiliary_index;
    extern const SettingsUInt64 auxiliary_index_overfetch_factor;
    extern const SettingsBool auxiliary_index_require_match;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString auxiliary_index_preferred_algorithm;
}

namespace QueryPlanOptimizations
{

void attachAuxiliaryIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const AuxiliaryIndexHints & hints)
{
    if (!hints.covered_source_parts.contains(part_uuid))
        return;

    chassert(!read_hints.auxiliary_index_search_results.has_value());

    auto it = hints.hits_per_part.find(part_uuid);
    if (it != hints.hits_per_part.end())
    {
        read_hints.auxiliary_index_search_results = it->second;
        return;
    }

    NearestNeighbours empty;
    empty.distances = std::vector<float>{};
    read_hints.auxiliary_index_search_results = std::move(empty);
}

void applyAuxiliaryIndexHints(RangesInDataParts & parts, const AuxiliaryIndexHints & hints)
{
    for (auto & part : parts)
        attachAuxiliaryIndexHintForPart(part.data_part->uuid, part.read_hints, hints);
}


void setAuxiliaryIndexHintsAndApplyToAnalyzed(ReadFromMergeTree & rfmt, AuxiliaryIndexHints hints)
{
    /// The optimizer calls selectRangesToRead for coverage/cost before the
    /// winner is known. If that cached analysis already exists, setting hints
    /// on RFMT alone is too late: apply them to the cached parts as well.
    if (auto analyzed = rfmt.getAnalyzedResult())
        applyAuxiliaryIndexHints(analyzed->parts_with_ranges, hints);

    rfmt.setAuxiliaryIndexHints(std::move(hints));
}


/// Unit-testable cost helpers (T7/T8/T9).
///
/// "Equivalent scanned rows" is the shared currency: framework adds a
/// verify_cost (PREWHERE re-evaluation over candidate_limit rows) to the
/// algorithm-reported search cost, then compares against the fallback full-scan.

size_t computeAuxiliaryIndexTotalCost(
    const AlgorithmCostEstimate & est,
    size_t candidate_limit,
    const CoverageSnapshot & coverage)
{
    return computeReflectionReadHintTotalCost(ReflectionReadHintCost{
        .engine_search_cost = est.algorithm_search_cost,
        .candidate_limit = candidate_limit,
        .uncovered_source_rows = coverage.uncovered_source_rows,
        .ready_reflection_parts = coverage.ready_auxiliary_index_parts,
        .has_source_parts = coverage.active_source_parts != 0,
        .full_coverage = coverage.full_coverage});
}

std::optional<size_t> computeAuxiliaryIndexCandidateLimit(size_t top_k, UInt64 overfetch_factor)
{
    return computeReflectionReadHintCandidateLimit(top_k, overfetch_factor);
}

std::optional<size_t> pickAuxiliaryIndexWinner(
    const std::vector<AuxiliaryIndexCandidateScore> & scored,
    const String & force_name,
    const String & preferred_algorithm,
    size_t fallback_cost,
    LoggerPtr log)
{
    std::vector<ReflectionReadHintCandidateScore> reflection_scored;
    reflection_scored.reserve(scored.size());
    for (const auto & candidate : scored)
        reflection_scored.push_back({.name = candidate.name, .engine = candidate.algorithm, .cost = candidate.cost});
    return pickReflectionReadHintWinner(reflection_scored, force_name, preferred_algorithm, fallback_cost, log);
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


/// Plan-shape walker: peel off LimitStep -> SortingStep -> ExpressionStep ->
/// optional FilterStep / prewhere ExpressionStep and capture the ReadFromMergeTree
/// node beneath. Returns nullptr on any shape mismatch so the caller can
/// early-return without rewriting the plan.
struct PlanShape
{
    LimitStep * limit_step = nullptr;
    SortingStep * sorting_step = nullptr;
    QueryPlan::Node * expression_node = nullptr;
    ExpressionStep * expression_step = nullptr;
    std::vector<QueryPlan::Node *> intermediate_nodes;
    QueryPlan::Node * filter_node = nullptr;
    FilterStep * filter_step = nullptr;
    QueryPlan::Node * prewhere_expression_node = nullptr;
    ExpressionStep * prewhere_expression_step = nullptr;
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

    for (size_t i = 0; i < 3; ++i)
    {
        shape.rfmt_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
        if (shape.rfmt_step)
            break;

        if (auto * filter_step = typeid_cast<FilterStep *>(node->step.get()))
        {
            if (shape.filter_step || node->children.size() != 1)
                return std::nullopt;

            shape.filter_step = filter_step;
            shape.filter_node = node;
            shape.intermediate_nodes.push_back(node);
            node = node->children.front();
            continue;
        }

        if (auto * prewhere_expression_step = typeid_cast<ExpressionStep *>(node->step.get()))
        {
            if (shape.prewhere_expression_step || node->children.size() != 1)
                return std::nullopt;

            shape.prewhere_expression_step = prewhere_expression_step;
            shape.prewhere_expression_node = node;
            shape.intermediate_nodes.push_back(node);
            node = node->children.front();
            continue;
        }

        shape.rfmt_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
        break;
    }

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
    return materializedIndexExpressionNeedsSearchColumn(
        expression_step.getExpression(),
        qp.sort_column,
        qp.search_column);
}

bool filterPredicateDependsOnSearchColumn(const FilterStep * filter_step, const QueryParams & qp)
{
    if (!filter_step)
        return false;

    const auto * filter_node = filter_step->getExpression().tryFindInOutputs(filter_step->getFilterColumnName());
    return nodeDependsOnColumn(filter_node, qp.search_column);
}

bool prewherePredicateDependsOnSearchColumn(const PrewhereInfoPtr & prewhere_info, const QueryParams & qp)
{
    if (!prewhere_info)
        return false;

    const auto * prewhere_node = prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name);
    return nodeDependsOnColumn(prewhere_node, qp.search_column);
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


std::vector<StorageANN *> findAuxiliaryIndexCandidates(
    const StorageID & source_id,
    const ContextPtr & context,
    const String & search_column,
    std::vector<StoragePtr> & owners_out)
{
    std::vector<StorageANN *> result;
    auto & catalog = DatabaseCatalog::instance();
    auto deps = catalog.getReferentialDependents(source_id);
    for (const auto & dep_id : deps)
    {
        auto dep_storage = catalog.tryGetTable(dep_id, context);
        if (!dep_storage)
            continue;
        auto * auxiliary_index = typeid_cast<StorageANN *>(dep_storage.get());
        if (!auxiliary_index)
            continue;
        const auto & indexed_columns = auxiliary_index->getIndexedColumns();
        if (indexed_columns.size() != 1 || indexed_columns.front() != search_column)
            continue;
        result.push_back(auxiliary_index);
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


void removeSearchColumnOutput(ActionsDAG & expression, const String & search_column)
{
    String output_result_to_delete;

    for (const auto * output_node : expression.getOutputs())
    {
        if (output_node->type == ActionsDAG::ActionType::ALIAS
            && !output_node->children.empty()
            && resultNameMatchesColumn(output_node->children.front()->result_name, search_column))
        {
            output_result_to_delete = output_node->result_name;
            break;
        }

        if (resultNameMatchesColumn(output_node->result_name, search_column))
        {
            output_result_to_delete = output_node->result_name;
            break;
        }
    }

    if (!output_result_to_delete.empty())
        expression.removeUnusedResult(output_result_to_delete);

    expression.removeUnusedActions();
}


void rewriteFilterForDistanceVirtual(
    QueryPlan::Node * filter_node,
    SharedHeader child_output_header,
    const QueryParams & qp,
    bool keep_search_column)
{
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    chassert(filter_step != nullptr);

    ActionsDAG & filter_expression = filter_step->getExpression();
    if (!keep_search_column)
        removeSearchColumnOutput(filter_expression, qp.search_column);

    auto new_step = std::make_unique<FilterStep>(
        std::move(child_output_header),
        std::move(filter_expression),
        filter_step->getFilterColumnName(),
        filter_step->removesFilterColumn());
    new_step->setStepDescription(*filter_node->step);
    filter_node->step = std::move(new_step);
}


void rewritePrewhereExpressionForDistanceVirtual(
    QueryPlan::Node * expression_node,
    SharedHeader child_output_header,
    const QueryParams & qp,
    bool keep_search_column)
{
    auto * expression_step = typeid_cast<ExpressionStep *>(expression_node->step.get());
    chassert(expression_step != nullptr);

    ActionsDAG & expression = expression_step->getExpression();
    if (!keep_search_column)
        removeSearchColumnOutput(expression, qp.search_column);

    auto new_step = std::make_unique<ExpressionStep>(std::move(child_output_header), std::move(expression));
    new_step->setStepDescription(*expression_node->step);
    expression_node->step = std::move(new_step);
}


SharedHeader rewriteIntermediateStepsForDistanceVirtual(
    const PlanShape & shape,
    SharedHeader child_output_header,
    const QueryParams & qp,
    bool keep_search_column)
{
    for (auto it = shape.intermediate_nodes.rbegin(); it != shape.intermediate_nodes.rend(); ++it)
    {
        auto * intermediate_node = *it;
        if (typeid_cast<FilterStep *>(intermediate_node->step.get()))
            rewriteFilterForDistanceVirtual(intermediate_node, child_output_header, qp, keep_search_column);
        else
            rewritePrewhereExpressionForDistanceVirtual(intermediate_node, child_output_header, qp, keep_search_column);

        child_output_header = intermediate_node->step->getOutputHeader();
    }

    return child_output_header;
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
/// produces (user_columns, _distance) or (user_columns - search_column, _distance)
/// where _distance is computed with the winner algorithm's exact evaluator.
ExpressionStep buildDistanceExpressionForUncovered(
    SharedHeader input_header,
    const QueryParams & qp,
    const AlgorithmDistanceDescriptor & distance,
    const ContextPtr & context,
    bool keep_search_column)
{
    ActionsDAG dag;

    std::unordered_map<String, const ActionsDAG::Node *> by_name;
    for (const auto & col : input_header->getColumnsWithTypeAndName())
        by_name[col.name] = &dag.addInput(col);

    auto search_it = by_name.find(qp.search_column);
    if (search_it == by_name.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndex partial-coverage: search column {} missing from uncovered RFMT output",
            qp.search_column);

    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat32>());
    auto literal_col = makeQueryVectorConstColumn(qp.reference_vector);
    const auto & literal_node = dag.addColumn({std::move(literal_col), array_type, "__mi_query_vector"});

    auto uint64_type = std::make_shared<DataTypeUInt64>();
    auto metric_col = ColumnConst::create(ColumnUInt64::create(1, distance.metric_id), 1);
    const auto & metric_node = dag.addColumn({std::move(metric_col), uint64_type, "__mi_metric_id"});

    auto dim_col = ColumnConst::create(ColumnUInt64::create(1, distance.dim), 1);
    const auto & dim_node = dag.addColumn({std::move(dim_col), uint64_type, "__mi_dim"});

    auto func = FunctionFactory::instance().get(distance.exact_function_name, context);
    const auto & raw_distance = dag.addFunction(func, {search_it->second, &literal_node, &metric_node, &dim_node}, "");

    const ActionsDAG::Node * distance_node = &raw_distance;
    if (!WhichDataType(distance_node->result_type).isFloat32())
        distance_node = &dag.addCast(*distance_node, std::make_shared<DataTypeFloat32>(), "__mi_cast_distance", context);

    const auto & distance_alias = dag.addAlias(*distance_node, "_distance");

    auto & outputs = dag.getOutputs();
    outputs.clear();
    for (const auto & col : input_header->getColumnsWithTypeAndName())
    {
        if (!keep_search_column && col.name == qp.search_column)
            continue;
        outputs.push_back(by_name[col.name]);
    }
    outputs.push_back(&distance_alias);

    return ExpressionStep(std::move(input_header), std::move(dag));
}


void prepareRfmtForDistanceVirtual(ReadFromMergeTree & rfmt, const String & search_column, bool keep_search_column)
{
    if (keep_search_column)
        rfmt.addDistanceColumnForVectorSearch();
    else
        rfmt.replaceVectorColumnWithDistanceColumn(search_column);
}


/// Clone `original` and override its analysed result so the clone reads only
/// the source parts that pass `keep`. The clone owns its own analysed result;
/// the caller may further mutate the returned RFMT (replaceVectorColumn,
/// setAuxiliaryIndexHints, ...).
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


ReadyAuxiliaryIndexPartSnapshot buildReadySnapshot(
    const MergeTreeData::DataPartsVector & ready_auxiliary_index_parts_data,
    const IAuxiliaryIndexAlgorithm * algorithm,
    LoggerPtr log)
{
    ReadyAuxiliaryIndexPartSnapshot snapshot;
    snapshot.parts.reserve(ready_auxiliary_index_parts_data.size());
    for (const auto & part : ready_auxiliary_index_parts_data)
    {
        if (!part)
            continue;

        ReadyAuxiliaryIndexPart ready_part;
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
            for (const auto & entry : StorageANN::parseCoverageJsonFromMiPart(*part))
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
    const ReadyAuxiliaryIndexPart & ready_part,
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


ReadyAuxiliaryIndexPartSnapshot pruneReadySnapshotForActivePartitions(
    ReadyAuxiliaryIndexPartSnapshot ready_snapshot,
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
    const ReadyAuxiliaryIndexPartSnapshot & ready_snapshot,
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
    const ReadyAuxiliaryIndexPartSnapshot & ready_snapshot,
    size_t candidate_limit)
{
    CoverageSnapshot snapshot;
    snapshot.active_source_parts = active_metadata_by_uuid.size();
    snapshot.ready_auxiliary_index_parts = ready_snapshot.parts.size();
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

    for (const auto & hit_set : internal_result.per_auxiliary_index_part)
    {
        if (!hit_set.auxiliary_index_part_storage)
            continue;
        if (hit_set.internal_ids.size() != hit_set.distances.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AuxiliaryIndex search returned {} internal ids but {} distances",
                hit_set.internal_ids.size(), hit_set.distances.size());

        AuxiliaryIndexPartReverseLookup lookup(*hit_set.auxiliary_index_part_storage);
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


AuxiliaryIndexHints buildHintsForCoveredSourceParts(
    const SourceSearchResult & source_result,
    const std::unordered_set<UUID> & covered_source_parts)
{
    AuxiliaryIndexHints hints;
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


bool materializedIndexExpressionNeedsSearchColumn(
    const ActionsDAG & expression,
    const String & sort_column,
    const String & search_column)
{
    for (const auto * output : expression.getOutputs())
    {
        /// The ORDER BY distance expression is the one we intentionally
        /// replace with `_distance`.
        if (output->result_name == sort_column)
            continue;
        if (nodeDependsOnColumn(output, search_column))
            return true;
    }
    return false;
}


size_t tryUseAuxiliaryIndex(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & settings)
{
    constexpr size_t no_layers_updated = 0;

    auto shape = walkPlanShape(parent_node);
    if (!shape)
        return no_layers_updated;

    auto & rfmt = *shape->rfmt_step;

    /// `auxiliary_index_require_match` (strict mode) takes effect once we
    /// know we're looking at an ANN-shaped query — i.e. after walkPlanShape
    /// matched and extractQueryParams succeeded. Earlier guards (parallel
    /// replicas, unsupported filter/PREWHERE expressions) still throw under
    /// strict mode because the user's
    /// contract is "this query MUST go through a AuxiliaryIndex"; if we
    /// can't honour that, silently falling back to a brute-force scan is
    /// exactly what strict mode is meant to prevent.
    auto context = rfmt.getContext();
    const bool require_match = context && context->getSettingsRef()[Setting::auxiliary_index_require_match];
    auto give_up = [&](std::string_view reason) -> size_t
    {
        if (require_match)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "auxiliary_index_require_match is set but no AuxiliaryIndex rewrite was applied: {}",
                reason);
        return no_layers_updated;
    };

    if (rfmt.isParallelReadingFromReplicas())
        return give_up("parallel reading from replicas is enabled");
    if (rfmt.isQueryWithFinal())
        return give_up("FINAL is not supported by AuxiliaryIndex");
    if (auto mutations_snapshot = rfmt.getMutationsSnapshot();
        mutations_snapshot && (mutations_snapshot->hasDataMutations() || mutations_snapshot->hasPatchParts()))
        return give_up("source has in-flight mutations");

    auto qp = extractQueryParams(*shape);
    if (!qp)
        return no_layers_updated;
    const bool keep_search_column = expressionOutputsDependOnSearchColumn(*shape->expression_step, *qp);
    if (filterPredicateDependsOnSearchColumn(shape->filter_step, *qp))
        return give_up("query filter depends on the search column");
    if (prewherePredicateDependsOnSearchColumn(rfmt.getPrewhereInfo(), *qp))
        return give_up("query PREWHERE depends on the search column");

    if (!context)
        return no_layers_updated;

    const bool force_mi = context->getSettingsRef()[Setting::force_using_auxiliary_index];
    const auto storage_metadata = rfmt.getStorageMetadata();

    if (!force_mi && sourceHasVectorSimilarityIndex(storage_metadata, qp->search_column))
        return give_up("source has a vector similarity index; set force_using_auxiliary_index=1 to prefer MI");

    std::vector<StoragePtr> auxiliary_index_owners;
    auto auxiliary_index_candidates = findAuxiliaryIndexCandidates(rfmt.getStorageID(), context, qp->search_column, auxiliary_index_owners);
    if (auxiliary_index_candidates.empty())
        return give_up("no AuxiliaryIndex is registered on the source for the search column");

    const auto & settings_ref = context->getSettingsRef();
    const String force_name = settings_ref[Setting::force_auxiliary_index];
    const String disable_name = settings_ref[Setting::disable_auxiliary_index];

    if (!disable_name.empty())
        std::erase_if(auxiliary_index_candidates, [&](auto * cand) { return cand->getStorageID().getTableName() == disable_name; });
    if (auxiliary_index_candidates.empty())
        return give_up("all AuxiliaryIndex candidates were excluded by disable_auxiliary_index");

    QueryFeatures features;
    features.query_vector = qp->reference_vector;
    features.distance_function = qp->distance_function;
    features.k = qp->top_k;

    /// Cost-based winner selection. First match every candidate; collect the
    /// successful matches together with their algorithm-reported cost. Sort
    /// ascending by MI name so cost ties resolve deterministically.
    struct ScoredCandidate
    {
        StorageANN * auxiliary_index;
        MatchDescriptor desc;
        ReadyAuxiliaryIndexPartSnapshot ready_snapshot;
        CoverageSnapshot coverage;
        std::unordered_set<UUID> covered_source_parts;
        size_t cost;
    };
    std::vector<ScoredCandidate> scored;
    scored.reserve(auxiliary_index_candidates.size());

    const auto overfetch_factor = settings_ref[Setting::auxiliary_index_overfetch_factor];
    const auto candidate_limit = computeAuxiliaryIndexCandidateLimit(qp->top_k, overfetch_factor);
    if (!candidate_limit)
        return give_up("auxiliary_index_overfetch_factor is outside the supported range");

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
    auto log = getLogger("optimizeAuxiliaryIndex");

    for (auto * cand : auxiliary_index_candidates)
    {
        auto * algo = cand->getAlgorithm();
        if (!algo)
            continue;
        auto desc = algo->match(features);
        if (!desc.has_value())
            continue;

        auto ready_auxiliary_index_parts_data = cand->getAccessPathPartsVectorForInternalUsage();
        if (ready_auxiliary_index_parts_data.empty())
            continue;

        auto ready_snapshot = buildReadySnapshot(ready_auxiliary_index_parts_data, algo, log);
        ready_snapshot = pruneReadySnapshotForActivePartitions(std::move(ready_snapshot), active_metadata_by_uuid);
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
            computeAuxiliaryIndexTotalCost(cost_estimate, *candidate_limit, coverage)});
    }

    if (scored.empty())
        return give_up("every AuxiliaryIndex candidate rejected the query or had no ready parts covering the source");

    std::sort(scored.begin(), scored.end(),
        [](const auto & a, const auto & b) { return a.auxiliary_index->getStorageID().getTableName() < b.auxiliary_index->getStorageID().getTableName(); });

    std::vector<AuxiliaryIndexCandidateScore> scored_view;
    scored_view.reserve(scored.size());
    for (const auto & sc : scored)
        scored_view.push_back({
            .name = sc.auxiliary_index->getStorageID().getTableName(),
            .algorithm = sc.auxiliary_index->getImpl(),
            .cost = sc.cost});

    const String preferred_algorithm = (*rfmt.getMergeTreeData().getSettings())[MergeTreeSetting::auxiliary_index_preferred_algorithm];

    auto winner_idx = pickAuxiliaryIndexWinner(
        scored_view, force_name, preferred_algorithm, fallback_cost, log);
    if (!winner_idx)
        return give_up("AuxiliaryIndex cost model declined every candidate (source scan was cheaper or force_auxiliary_index missed)");

    if (settings.is_explain)
        return no_layers_updated;

    StorageANN * winner = scored[*winner_idx].auxiliary_index;
    auto winning_desc = std::move(scored[*winner_idx].desc);
    auto ready_snapshot = std::move(scored[*winner_idx].ready_snapshot);
    auto covered_source_parts = std::move(scored[*winner_idx].covered_source_parts);

    InternalSearchResult internal_result = winner->getAlgorithm()->search(winning_desc, ready_snapshot, *candidate_limit, context);
    SourceSearchResult source_result = translateInternalHitsToSourceRows(internal_result);
    AuxiliaryIndexHints hints = buildHintsForCoveredSourceParts(source_result, covered_source_parts);
    if (hints.covered_source_parts.empty())
        return give_up("AuxiliaryIndex search returned no hits for any covered source part");

    const bool full_coverage = fullyCoversActiveSourceParts(analyzed->parts_with_ranges, covered_source_parts);

    if (full_coverage)
    {
        prepareRfmtForDistanceVirtual(rfmt, qp->search_column, keep_search_column);
        setAuxiliaryIndexHintsAndApplyToAnalyzed(rfmt, std::move(hints));
        auto child_output_header = rfmt.getOutputHeader();
        child_output_header = rewriteIntermediateStepsForDistanceVirtual(*shape, child_output_header, *qp, keep_search_column);
        rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, child_output_header, *qp);
        return no_layers_updated;
    }

    /// Partial coverage splits the read into an MI branch and a brute-force
    /// branch over uncovered source parts. The uncovered branch is exactly
    /// the source scan that strict mode is meant to forbid.
    if (require_match)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "auxiliary_index_require_match is set but AuxiliaryIndex only covers a subset of source parts; "
            "the remaining parts would be served by a brute-force scan");

    auto in_hints = [&covered_source_parts](const auto & p) { return covered_source_parts.contains(p.data_part->uuid); };
    auto not_in_hints = [&covered_source_parts](const auto & p) { return !covered_source_parts.contains(p.data_part->uuid); };

    auto covered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, in_hints);
    auto uncovered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, not_in_hints);

    prepareRfmtForDistanceVirtual(*covered_rfmt, qp->search_column, keep_search_column);
    setAuxiliaryIndexHintsAndApplyToAnalyzed(*covered_rfmt, std::move(hints));

    auto uncovered_input_header = uncovered_rfmt->getOutputHeader();
    auto uncovered_expression = buildDistanceExpressionForUncovered(uncovered_input_header, *qp, winning_desc.distance, context, keep_search_column);

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

    auto child_output_header = union_output_header;
    child_output_header = rewriteIntermediateStepsForDistanceVirtual(*shape, child_output_header, *qp, keep_search_column);
    rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, child_output_header, *qp);
    return no_layers_updated;
}

}
}
