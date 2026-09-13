#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromMemory.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

std::string encodeRowSources(const std::vector<RowSourcePart> & rows)
{
    return std::string(reinterpret_cast<const char *>(rows.data()), rows.size() * sizeof(RowSourcePart));
}

ColumnPtr gatherUntilFinished(ColumnGathererStream & stream, std::vector<ssize_t> * required_sources = nullptr)
{
    MutableColumnPtr result;
    size_t steps = 0;
    while (steps < 32)
    {
        ++steps;
        auto status = stream.merge();
        if (status.required_source >= 0)
        {
            if (required_sources)
                required_sources->push_back(status.required_source);
            else
                ADD_FAILURE() << "unexpected need_data from source " << status.required_source;
            break;
        }

        if (status.chunk && status.chunk.getNumRows())
        {
            const auto & column = status.chunk.getColumns().at(0);
            if (!result)
                result = column->cloneEmpty();
            result->insertRangeFrom(*column, 0, column->size());
        }

        if (status.is_finished)
            break;
    }

    EXPECT_LT(steps, 32u);
    return result;
}

IMergingAlgorithm::Inputs makeSingleColumnInputs(size_t num_inputs, size_t present_source, ColumnPtr present_column)
{
    IMergingAlgorithm::Inputs inputs(num_inputs);
    inputs[present_source].set(Chunk(Columns{present_column}, present_column->size()));
    return inputs;
}

}

TEST(ColumnGathererVirtualDefaults, EmptyBitmapMatchesInsertRangeFrom)
{
    const std::vector<RowSourcePart> rows{RowSourcePart(0), RowSourcePart(1), RowSourcePart(0)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());

    ColumnGathererStream stream(2, buf, 16, 1024 * 1024, std::nullopt, false);
    auto left = ColumnUInt64::create();
    left->insert(10);
    left->insert(20);
    auto right = ColumnUInt64::create();
    right->insert(30);

    IMergingAlgorithm::Inputs inputs(2);
    inputs[0].set(Chunk(Columns{std::move(left)}, 2));
    inputs[1].set(Chunk(Columns{std::move(right)}, 1));
    stream.initialize(std::move(inputs));

    auto result = gatherUntilFinished(stream);
    ASSERT_TRUE(result);
    ASSERT_EQ(result->size(), 3u);
    EXPECT_EQ(result->operator[](0).safeGet<UInt64>(), 10u);
    EXPECT_EQ(result->operator[](1).safeGet<UInt64>(), 30u);
    EXPECT_EQ(result->operator[](2).safeGet<UInt64>(), 20u);
}

TEST(ColumnGathererVirtualDefaults, CrossSourceDefaultsDoNotPull)
{
    const std::vector<RowSourcePart> rows{
        RowSourcePart(0),
        RowSourcePart(1),
        RowSourcePart(2),
        RowSourcePart(1),
        RowSourcePart(0)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());

    ColumnGathererStream stream(3, buf, 16, 1024 * 1024, std::nullopt, false, {0, 1, 1});
    auto present = ColumnUInt64::create();
    present->insert(10);
    present->insert(20);
    stream.initialize(makeSingleColumnInputs(3, 0, std::move(present)));

    std::vector<ssize_t> required;
    auto result = gatherUntilFinished(stream, &required);
    EXPECT_TRUE(required.empty());
    ASSERT_TRUE(result);
    ASSERT_EQ(result->size(), 5u);
    EXPECT_EQ(result->operator[](0).safeGet<UInt64>(), 10u);
    EXPECT_EQ(result->operator[](1).safeGet<UInt64>(), 0u);
    EXPECT_EQ(result->operator[](2).safeGet<UInt64>(), 0u);
    EXPECT_EQ(result->operator[](3).safeGet<UInt64>(), 0u);
    EXPECT_EQ(result->operator[](4).safeGet<UInt64>(), 20u);
    EXPECT_EQ(stream.takeVirtualDefaultRows(), 0u);
}

TEST(ColumnGathererVirtualDefaults, SkipDoesNotInsert)
{
    const std::vector<RowSourcePart> rows{
        RowSourcePart(0),
        RowSourcePart(1, true),
        RowSourcePart(2, true),
        RowSourcePart(1),
        RowSourcePart(0)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());

    ColumnGathererStream stream(3, buf, 16, 1024 * 1024, std::nullopt, false, {0, 1, 1});
    auto present = ColumnUInt64::create();
    present->insert(4);
    present->insert(5);
    stream.initialize(makeSingleColumnInputs(3, 0, std::move(present)));

    auto result = gatherUntilFinished(stream);
    ASSERT_TRUE(result);
    ASSERT_EQ(result->size(), 3u);
    EXPECT_EQ(result->operator[](0).safeGet<UInt64>(), 4u);
    EXPECT_EQ(result->operator[](1).safeGet<UInt64>(), 0u);
    EXPECT_EQ(result->operator[](2).safeGet<UInt64>(), 5u);
}

TEST(ColumnGathererVirtualDefaults, BitmapSizeMismatchThrows)
{
    const std::vector<RowSourcePart> rows{RowSourcePart(0)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());
    EXPECT_THROW(
        ColumnGathererStream(2, buf, 16, 1024 * 1024, std::nullopt, false, {1}),
        Exception);
}

TEST(ColumnGathererVirtualDefaults, PreferredBlockSplitsDefaultRun)
{
    const std::vector<RowSourcePart> rows{RowSourcePart(0), RowSourcePart(1), RowSourcePart(2), RowSourcePart(1)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());

    ColumnGathererStream stream(3, buf, 2, 1024 * 1024, std::nullopt, false, {0, 1, 1});
    auto present = ColumnUInt64::create();
    present->insert(7);
    stream.initialize(makeSingleColumnInputs(3, 0, std::move(present)));

    auto first = stream.merge();
    ASSERT_GE(first.chunk.getNumRows(), 1u);
    EXPECT_EQ(first.required_source, -1);

    auto rest = gatherUntilFinished(stream);
    ASSERT_TRUE(rest);
    EXPECT_EQ(first.chunk.getNumRows() + rest->size(), 4u);
}

TEST(ColumnGathererVirtualDefaults, LowCardinalityNullableDefaultIsNull)
{
    auto type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    auto present = type->createColumn();
    present->insert(Field(String("x")));

    const std::vector<RowSourcePart> rows{RowSourcePart(0), RowSourcePart(1), RowSourcePart(2)};
    const auto encoded = encodeRowSources(rows);
    ReadBufferFromMemory buf(encoded.data(), encoded.size());
    ColumnGathererStream stream(3, buf, 16, 1024 * 1024, std::nullopt, false, {0, 1, 1});
    stream.initialize(makeSingleColumnInputs(3, 0, std::move(present)));

    auto result = gatherUntilFinished(stream);
    ASSERT_TRUE(result);
    ASSERT_EQ(result->size(), 3u);
    EXPECT_EQ(result->operator[](0).safeGet<String>(), "x");
    EXPECT_TRUE(result->isNullAt(1));
    EXPECT_TRUE(result->isNullAt(2));
}
