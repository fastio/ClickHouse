#include <Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
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
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeIndices.h>

#include <algorithm>


namespace DB
{

namespace Setting
{
    extern const SettingsBool force_using_materialized_index;
}

namespace QueryPlanOptimizations
{

void attachMaterializedIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const MaterializedIndexHints & hints)
{
    auto it = hints.per_part.find(part_uuid);
    if (it == hints.per_part.end())
        return;
    chassert(!read_hints.mi_search_results.has_value());
    read_hints.mi_search_results = it->second;
}

void applyMaterializedIndexHints(RangesInDataParts & parts, const MaterializedIndexHints & hints)
{
    for (auto & part : parts)
        attachMaterializedIndexHintForPart(part.data_part->uuid, part.read_hints, hints);
}


namespace
{

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
        auto * mi = typeid_cast<StorageMaterializedIndex *>(dep_storage.get());
        if (!mi)
            continue;
        const auto & indexed_columns = mi->getIndexedColumns();
        if (indexed_columns.size() != 1 || indexed_columns.front() != search_column)
            continue;
        result.push_back(mi);
        owners_out.push_back(std::move(dep_storage));
    }
    return result;
}


bool fullyCoversActiveSourceParts(const RangesInDataParts & active_parts, const MaterializedIndexHints & hints)
{
    for (const auto & p : active_parts)
    {
        if (!hints.per_part.contains(p.data_part->uuid))
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

    if (rfmt.isParallelReadingFromReplicas())
        return no_layers_updated;
    if (rfmt.getPrewhereInfo())
        return no_layers_updated;

    auto qp = extractQueryParams(*shape);
    if (!qp)
        return no_layers_updated;

    auto context = rfmt.getContext();
    if (!context)
        return no_layers_updated;

    const bool force_mi = context->getSettingsRef()[Setting::force_using_materialized_index];
    const auto storage_metadata = rfmt.getStorageMetadata();

    if (!force_mi && sourceHasVectorSimilarityIndex(storage_metadata, qp->search_column))
        return no_layers_updated;

    std::vector<StoragePtr> mi_owners;
    auto mi_candidates = findMaterializedIndexCandidates(rfmt.getStorageID(), context, qp->search_column, mi_owners);
    if (mi_candidates.empty())
        return no_layers_updated;

    QueryFeatures features;
    features.query_vector = qp->reference_vector;
    features.k = qp->top_k;

    StorageMaterializedIndex * winner = nullptr;
    std::optional<MatchDescriptor> winning_desc;
    for (auto * cand : mi_candidates)
    {
        auto * algo = cand->getAlgorithm();
        if (!algo)
            continue;
        auto desc = algo->match(features);
        if (desc.has_value())
        {
            winner = cand;
            winning_desc = std::move(desc);
            break;
        }
    }

    if (!winner)
        return no_layers_updated;

    auto ready_mi_parts_data = winner->getAccessPathPartsVectorForInternalUsage();
    if (ready_mi_parts_data.empty())
        return no_layers_updated;

    ReadyMaterializedIndexPartSnapshot ready_snapshot;
    ready_snapshot.parts.reserve(ready_mi_parts_data.size());
    for (const auto & p : ready_mi_parts_data)
        ready_snapshot.parts.push_back(p->getDataPartStoragePtr());

    SearchResult search_result = winner->getAlgorithm()->search(*winning_desc, ready_snapshot, qp->top_k, context);
    if (search_result.per_part.empty())
        return no_layers_updated;

    MaterializedIndexHints hints;
    for (auto & set : search_result.per_part)
    {
        NearestNeighbours nn;
        nn.rows = std::move(set.part_offsets);
        nn.distances = std::move(set.distances);
        hints.per_part.emplace(set.source_part_uuid, std::move(nn));
    }

    auto analyzed = rfmt.selectRangesToRead();
    if (!analyzed)
        return no_layers_updated;

    const bool full_coverage = fullyCoversActiveSourceParts(analyzed->parts_with_ranges, hints);

    if (full_coverage)
    {
        rfmt.replaceVectorColumnWithDistanceColumn(qp->search_column);
        rfmt.setMaterializedIndexHints(std::move(hints));
        rewriteExpressionForDistanceVirtual(shape->expression_node, *shape->sorting_step, rfmt.getOutputHeader(), *qp);
        return no_layers_updated;
    }

    auto in_hints = [&hints](const auto & p) { return hints.per_part.contains(p.data_part->uuid); };
    auto not_in_hints = [&hints](const auto & p) { return !hints.per_part.contains(p.data_part->uuid); };

    auto covered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, in_hints);
    auto uncovered_rfmt = cloneRfmtWithFilteredParts(rfmt, *analyzed, not_in_hints);

    covered_rfmt->replaceVectorColumnWithDistanceColumn(qp->search_column);
    covered_rfmt->setMaterializedIndexHints(std::move(hints));

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
