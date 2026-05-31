#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/optimizeReflectionReadHint.h>

#include <optional>
#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

TEST(OptimizationOrder, ReflectionReadHintFirst)
{
    const auto & opts = getOptimizations();

    auto find_index = [&](const char * name) -> std::optional<size_t>
    {
        for (size_t i = 0; i < opts.size(); ++i)
            if (std::string(opts[i].name) == name)
                return i;
        return std::nullopt;
    };

    auto read_hint_pos = find_index("useReflectionReadHint");
    auto vs_pos = find_index("useVectorSearch");

    ASSERT_TRUE(read_hint_pos.has_value());
    ASSERT_TRUE(vs_pos.has_value());
    EXPECT_LT(*read_hint_pos, *vs_pos);
}

TEST(ExpressionSearchColumnDependency, IgnoresSortColumnOutput)
{
    ActionsDAG dag;
    const auto & embedding = dag.addInput("embedding", std::make_shared<DataTypeFloat32>());
    const auto & distance_output = dag.addAlias(embedding, "distance");
    dag.getOutputs() = {&distance_output};

    EXPECT_FALSE(materializedIndexExpressionNeedsSearchColumn(dag, "distance", "embedding"));
}

TEST(ExpressionSearchColumnDependency, DetectsSelectedSearchColumn)
{
    ActionsDAG dag;
    const auto & embedding = dag.addInput("embedding", std::make_shared<DataTypeFloat32>());
    const auto & distance_output = dag.addAlias(embedding, "distance");
    dag.getOutputs() = {&embedding, &distance_output};

    EXPECT_TRUE(materializedIndexExpressionNeedsSearchColumn(dag, "distance", "embedding"));
}

TEST(ExpressionSearchColumnDependency, DetectsAliasAndQualifiedSearchColumn)
{
    ActionsDAG alias_dag;
    const auto & embedding = alias_dag.addInput("embedding", std::make_shared<DataTypeFloat32>());
    const auto & embedding_alias = alias_dag.addAlias(embedding, "selected_embedding");
    alias_dag.getOutputs() = {&embedding_alias};
    EXPECT_TRUE(materializedIndexExpressionNeedsSearchColumn(alias_dag, "distance", "embedding"));

    ActionsDAG qualified_dag;
    const auto & qualified_embedding = qualified_dag.addInput("default.table.embedding", std::make_shared<DataTypeFloat32>());
    qualified_dag.getOutputs() = {&qualified_embedding};
    EXPECT_TRUE(materializedIndexExpressionNeedsSearchColumn(qualified_dag, "distance", "embedding"));
}
