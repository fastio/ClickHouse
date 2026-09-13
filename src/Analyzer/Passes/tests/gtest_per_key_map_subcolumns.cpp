#include <Storages/MergeTree/tests/gtest_per_key_map_context.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>
#include <Analyzer/Passes/FunctionToSubcolumnsPass.h>

namespace
{
using namespace DB;
using PerKeyMapSubcolumns = PerKeyMapStorageTest;

TEST_F(PerKeyMapSubcolumns, ContainsRequestsExistenceSubcolumn)
{
    auto source = std::make_shared<TableNode>(storage, context);
    auto map = std::make_shared<ColumnNode>(metadata->getColumns().getPhysical("m"), source);
    auto function = std::make_shared<FunctionNode>("mapContainsKey");
    function->getArguments().getNodes() = {map, std::make_shared<ConstantNode>(String("a.null"))};
    resolveOrdinaryFunctionNodeByName(*function, "mapContainsKey", context);
    QueryTreeNodePtr node = function;
    FunctionToSubcolumnsPass().run(node, context);
    auto * column = node->as<ColumnNode>();
    ASSERT_NE(column, nullptr) << node->formatConvertedASTForErrorMessage();
    EXPECT_EQ(column->getColumnName(), "m.exists_a.null");
    EXPECT_EQ(column->getResultType()->getName(), "UInt8");
}

TEST_F(PerKeyMapSubcolumns, MapKeysRequestsKeysSubcolumn)
{
    auto source = std::make_shared<TableNode>(storage, context);
    auto map = std::make_shared<ColumnNode>(metadata->getColumns().getPhysical("m"), source);
    auto function = std::make_shared<FunctionNode>("mapKeys");
    function->getArguments().getNodes() = {map};
    resolveOrdinaryFunctionNodeByName(*function, "mapKeys", context);
    QueryTreeNodePtr node = function;
    FunctionToSubcolumnsPass().run(node, context);
    auto * column = node->as<ColumnNode>();
    ASSERT_NE(column, nullptr) << node->formatConvertedASTForErrorMessage();
    EXPECT_EQ(column->getColumnName(), "m.keys");
}
}
