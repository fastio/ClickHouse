#include <Processors/QueryPlan/Optimizations/optimizeReflectionReadHint.h>

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
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/Reflection/IReflectionMatcher.h>
#include <Storages/SelectQueryInfo.h>

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
    extern const SettingsBool force_using_ann_index;
    extern const SettingsString force_ann_index;
    extern const SettingsString disable_ann_index;
    extern const SettingsUInt64 ann_index_overfetch_factor;
    extern const SettingsBool ann_index_require_match;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString ann_index_preferred_algorithm;
}

namespace QueryPlanOptimizations
{

void attachANNIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const ANNIndexHints & hints)
{
    if (!hints.covered_source_parts.contains(part_uuid))
        return;

    chassert(!read_hints.ann_index_search_results.has_value());

    auto it = hints.hits_per_part.find(part_uuid);
    if (it != hints.hits_per_part.end())
    {
        read_hints.ann_index_search_results = it->second;
        return;
    }

    NearestNeighbours empty;
    empty.distances = std::vector<float>{};
    read_hints.ann_index_search_results = std::move(empty);
}

void applyANNIndexHints(RangesInDataParts & parts, const ANNIndexHints & hints)
{
    for (auto & part : parts)
        attachANNIndexHintForPart(part.data_part->uuid, part.read_hints, hints);
}


void setANNIndexHintsAndApplyToAnalyzed(ReadFromMergeTree & rfmt, ANNIndexHints hints)
{
    /// The optimizer calls selectRangesToRead for coverage/cost before the
    /// winner is known. If that cached analysis already exists, setting hints
    /// on RFMT alone is too late: apply them to the cached parts as well.
    if (auto analyzed = rfmt.getAnalyzedResult())
        applyANNIndexHints(analyzed->parts_with_ranges, hints);

    rfmt.setANNIndexHints(std::move(hints));
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


/// Enumerate `IReflectionMatcher` candidates that depend on `source_id` and
/// announce a `ReadHint` rewrite. Engine-agnostic: the optimizer never names a
/// concrete storage class.
///
/// `owners_out` parallels the result and keeps each candidate's storage alive
/// for the duration of the optimization pass — Reflection storages cooperating
/// here use `IReflection` to expose the indexed columns and matcher accessor;
/// future engines producing `PlanRewrite`/`CostHint` are not picked up by this
/// function.
std::vector<IReflectionMatcher *> findReadHintMatchers(
    const StorageID & source_id,
    const ContextPtr & context,
    const String & search_column,
    std::vector<StoragePtr> & owners_out)
{
    std::vector<IReflectionMatcher *> result;
    auto & catalog = DatabaseCatalog::instance();
    auto deps = catalog.getReferentialDependents(source_id);
    for (const auto & dep_id : deps)
    {
        auto dep_storage = catalog.tryGetTable(dep_id, context);
        if (!dep_storage)
            continue;
        auto * reflection = dynamic_cast<IReflection *>(dep_storage.get());
        if (!reflection)
            continue;
        auto * matcher = reflection->getReflectionMatcher();
        if (!matcher || matcher->matchKind() != ReflectionMatchKind::ReadHint)
            continue;
        const auto & indexed_columns = reflection->getReflectionIndexedColumns();
        if (indexed_columns.size() != 1 || indexed_columns.front() != search_column)
            continue;
        result.push_back(matcher);
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
    const ReflectionDistanceInfo & distance,
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
            "ANNIndex partial-coverage: search column {} missing from uncovered RFMT output",
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
/// setANNIndexHints, ...).
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


size_t tryUseReflectionReadHint(
    QueryPlan::Node * parent_node,
    QueryPlan::Nodes & nodes,
    const Optimization::ExtraSettings & settings)
{
    constexpr size_t no_layers_updated = 0;

    auto shape = walkPlanShape(parent_node);
    if (!shape)
        return no_layers_updated;

    auto & rfmt = *shape->rfmt_step;

    /// `ann_index_require_match` (strict mode) takes effect once we know we're
    /// looking at an ANN-shaped query — i.e. after `walkPlanShape` matched and
    /// `extractQueryParams` succeeded. Earlier guards (parallel replicas,
    /// unsupported filter / PREWHERE expressions) still throw under strict
    /// mode because the user's contract is "this query MUST go through a
    /// Reflection ReadHint"; silently falling back to a brute-force scan is
    /// exactly what strict mode is meant to prevent.
    auto context = rfmt.getContext();
    const bool require_match = context && context->getSettingsRef()[Setting::ann_index_require_match];
    auto give_up = [&](std::string_view reason) -> size_t
    {
        if (require_match)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ann_index_require_match is set but no Reflection ReadHint rewrite was applied: {}",
                reason);
        return no_layers_updated;
    };

    if (rfmt.isParallelReadingFromReplicas())
        return give_up("parallel reading from replicas is enabled");
    if (rfmt.isQueryWithFinal())
        return give_up("FINAL is not supported by Reflection ReadHint");
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

    const bool force_match = context->getSettingsRef()[Setting::force_using_ann_index];
    const auto storage_metadata = rfmt.getStorageMetadata();

    if (!force_match && sourceHasVectorSimilarityIndex(storage_metadata, qp->search_column))
        return give_up("source has a vector similarity index; set force_using_ann_index=1 to prefer Reflection");

    std::vector<StoragePtr> matcher_owners;
    auto matchers = findReadHintMatchers(rfmt.getStorageID(), context, qp->search_column, matcher_owners);
    if (matchers.empty())
        return give_up("no Reflection ReadHint matcher is registered on the source for the search column");

    const auto & settings_ref = context->getSettingsRef();
    const String force_name = settings_ref[Setting::force_ann_index];
    const String disable_name = settings_ref[Setting::disable_ann_index];

    if (!disable_name.empty())
    {
        for (size_t i = 0; i < matchers.size();)
        {
            if (matcher_owners[i]->getStorageID().getTableName() == disable_name)
            {
                matchers.erase(matchers.begin() + i);
                matcher_owners.erase(matcher_owners.begin() + i);
            }
            else
            {
                ++i;
            }
        }
    }
    if (matchers.empty())
        return give_up("all Reflection ReadHint candidates were excluded by disable_ann_index");

    const auto overfetch_factor = settings_ref[Setting::ann_index_overfetch_factor];
    const auto candidate_limit = computeReflectionReadHintCandidateLimit(qp->top_k, overfetch_factor);
    if (!candidate_limit)
        return give_up("ann_index_overfetch_factor is outside the supported range");

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

    ReflectionPlanShape plan_shape;
    plan_shape.distance_function = qp->distance_function;
    plan_shape.search_column = qp->search_column;
    plan_shape.query_vector = qp->reference_vector;
    plan_shape.top_k = qp->top_k;
    plan_shape.candidate_limit = *candidate_limit;
    plan_shape.active_source_parts = &analyzed->parts_with_ranges;

    auto log = getLogger("optimizeReflectionReadHint");

    /// Per-candidate `matchReadHint`: cheap offers only. Cost assembly stays
    /// framework-side via `computeReflectionReadHintTotalCost`. Sort ascending
    /// by reflection name so cost ties resolve deterministically.
    struct ScoredCandidate
    {
        IReflectionMatcher * matcher;
        ReflectionReadHintOffer offer;
        size_t cost;
    };
    std::vector<ScoredCandidate> scored;
    scored.reserve(matchers.size());

    for (auto * matcher : matchers)
    {
        auto offer = matcher->matchReadHint(plan_shape, context);
        if (!offer)
            continue;
        const auto cost = computeReflectionReadHintTotalCost(ReflectionReadHintCost{
            .engine_search_cost = offer->engine_search_cost,
            .candidate_limit = plan_shape.candidate_limit,
            .uncovered_source_rows = offer->uncovered_source_rows,
            .ready_reflection_parts = offer->ready_reflection_parts,
            .has_source_parts = offer->has_source_parts,
            .full_coverage = offer->full_coverage});
        scored.push_back({matcher, std::move(*offer), cost});
    }

    if (scored.empty())
        return give_up("every Reflection ReadHint candidate declined the query or had no covering parts");

    std::sort(scored.begin(), scored.end(),
        [](const auto & a, const auto & b) { return a.offer.reflection_name < b.offer.reflection_name; });

    std::vector<ReflectionReadHintCandidateScore> scored_view;
    scored_view.reserve(scored.size());
    for (const auto & sc : scored)
        scored_view.push_back({
            .name = sc.offer.reflection_name,
            .engine = sc.offer.engine_name,
            .cost = sc.cost});

    const String preferred_engine = (*rfmt.getMergeTreeData().getSettings())[MergeTreeSetting::ann_index_preferred_algorithm];

    auto winner_idx = pickReflectionReadHintWinner(
        scored_view, force_name, preferred_engine, fallback_cost, log);
    if (!winner_idx)
        return give_up("Reflection cost model declined every candidate (source scan was cheaper or force_ann_index missed)");

    if (settings.is_explain)
        return no_layers_updated;

    auto * winner = scored[*winner_idx].matcher;
    auto realization = winner->realizeReadHint(plan_shape, scored[*winner_idx].offer, context);
    if (realization.hits.covered_source_parts.empty())
        return give_up("Reflection ReadHint search returned no hits for any covered source part");

    const bool full_coverage = fullyCoversActiveSourceParts(analyzed->parts_with_ranges, realization.hits.covered_source_parts);

    if (full_coverage)
    {
        prepareRfmtForDistanceVirtual(rfmt, qp->search_column, keep_search_column);
        setANNIndexHintsAndApplyToAnalyzed(rfmt, std::move(realization.hits));
        auto child_output_header = rfmt.getOutputHeader();
        child_output_header = rewriteIntermediateStepsForDistanceVirtual(*shape, child_output_header, *qp, keep_search_column);
        rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, child_output_header, *qp);
        return no_layers_updated;
    }

    /// Partial coverage splits the read into a Reflection branch and a brute-
    /// force branch over uncovered source parts. The uncovered branch is
    /// exactly the source scan that strict mode is meant to forbid.
    if (require_match)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ann_index_require_match is set but Reflection only covers a subset of source parts; "
            "the remaining parts would be served by a brute-force scan");

    const auto & covered = realization.hits.covered_source_parts;
    auto in_hints = [&covered](const auto & p) { return covered.contains(p.data_part->uuid); };
    auto not_in_hints = [&covered](const auto & p) { return !covered.contains(p.data_part->uuid); };

    auto covered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, in_hints);
    auto uncovered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, not_in_hints);

    prepareRfmtForDistanceVirtual(*covered_rfmt, qp->search_column, keep_search_column);
    setANNIndexHintsAndApplyToAnalyzed(*covered_rfmt, std::move(realization.hits));

    auto uncovered_input_header = uncovered_rfmt->getOutputHeader();
    auto uncovered_expression = buildDistanceExpressionForUncovered(uncovered_input_header, *qp, realization.distance, context, keep_search_column);

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
