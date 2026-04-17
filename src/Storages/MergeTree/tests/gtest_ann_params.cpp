#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexDiskANN.h>
#include <Storages/IndicesDescription.h>

#include <Common/Exception.h>
#include <Core/Field.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

using namespace DB;

namespace
{

/// Build an `equals` AST node for a single `key = value` pair, matching what
/// the parser produces inside INDEX TYPE `diskann(...)` argument lists.
template <typename T>
ASTPtr makeKeyValueArgument(const String & key, const T & value)
{
    auto key_ast = make_intrusive<ASTIdentifier>(key);
    auto value_ast = make_intrusive<ASTLiteral>(Field(value));
    return makeASTFunction("equals", std::move(key_ast), std::move(value_ast));
}

/// Assemble a minimal `IndexDescription` populated with just enough fields for
/// `ANNIndexParams::fromDescription` and `diskANNIndexValidator` to exercise
/// their argument-parsing paths. Does not require a `Context`.
IndexDescription makeIndexDescription(std::vector<ASTPtr> args)
{
    IndexDescription description;
    description.type = "diskann";
    description.name = "idx_vec";

    auto list = make_intrusive<ASTExpressionList>();
    list->children.reserve(args.size());
    for (auto & arg : args)
        list->children.push_back(std::move(arg));
    description.arguments = list;
    return description;
}

}

TEST(ANNIndexParams, DefaultsApplied)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
    });

    auto params = ANNIndexParams::fromDescription(index);
    EXPECT_EQ(params.metric, "l2");
    EXPECT_EQ(params.dim, 128u);
    EXPECT_EQ(params.graph_degree, 64u);
    EXPECT_EQ(params.search_list_size, 100u);
    EXPECT_DOUBLE_EQ(params.alpha, 1.2);
    EXPECT_EQ(params.pq_bytes, 0u);
}

TEST(ANNIndexParams, AllFieldsParsed)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("cosine")),
        makeKeyValueArgument("dim", static_cast<UInt64>(768)),
        makeKeyValueArgument("R", static_cast<UInt64>(96)),
        makeKeyValueArgument("L", static_cast<UInt64>(200)),
        makeKeyValueArgument("alpha", 1.5),
        makeKeyValueArgument("pq_bytes", static_cast<UInt64>(32)),
    });

    auto params = ANNIndexParams::fromDescription(index);
    EXPECT_EQ(params.metric, "cosine");
    EXPECT_EQ(params.dim, 768u);
    EXPECT_EQ(params.graph_degree, 96u);
    EXPECT_EQ(params.search_list_size, 200u);
    EXPECT_DOUBLE_EQ(params.alpha, 1.5);
    EXPECT_EQ(params.pq_bytes, 32u);
}

TEST(ANNIndexParams, MipsAcceptedAtDDLLayer)
{
    /// metric='mips' is accepted at the DDL layer; downstream build-time code
    /// is responsible for raising NOT_IMPLEMENTED until MIPS lands.
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("mips")),
        makeKeyValueArgument("dim", static_cast<UInt64>(256)),
    });

    auto params = ANNIndexParams::fromDescription(index);
    EXPECT_EQ(params.metric, "mips");
    EXPECT_EQ(params.dim, 256u);
}

TEST(ANNIndexParams, MissingRequiredMetric)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, MissingRequiredDim)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, InvalidMetricRejected)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("xyz")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, ZeroDimRejected)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(0)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, GraphDegreeOutOfRange)
{
    auto index_low = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("R", static_cast<UInt64>(8)),
    });
    EXPECT_THROW(ANNIndexParams::fromDescription(index_low), DB::Exception);

    auto index_high = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("R", static_cast<UInt64>(200)),
    });
    EXPECT_THROW(ANNIndexParams::fromDescription(index_high), DB::Exception);
}

TEST(ANNIndexParams, SearchListBelowGraphDegree)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("R", static_cast<UInt64>(64)),
        makeKeyValueArgument("L", static_cast<UInt64>(10)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, AlphaOutOfRange)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("alpha", 0.5),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, PqBytesNotInAllowedSet)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("pq_bytes", static_cast<UInt64>(7)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, UnknownKeyRejected)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("foo", static_cast<UInt64>(1)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}

TEST(ANNIndexParams, DuplicateKeyRejected)
{
    auto index = makeIndexDescription({
        makeKeyValueArgument("metric", String("l2")),
        makeKeyValueArgument("dim", static_cast<UInt64>(128)),
        makeKeyValueArgument("dim", static_cast<UInt64>(256)),
    });

    EXPECT_THROW(ANNIndexParams::fromDescription(index), DB::Exception);
}
