#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>

#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <gtest/gtest.h>

using namespace DB;

template <typename T>
void testLowCardinalityNumberInsert(const DataTypePtr & data_type)
{
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();

    column->insert(static_cast<T>(15));
    column->insert(static_cast<T>(20));
    column->insert(static_cast<T>(25));

    Field value;
    column->get(0, value);
    ASSERT_EQ(value.safeGet<T>(), 15);

    column->get(1, value);
    ASSERT_EQ(value.safeGet<T>(), 20);

    column->get(2, value);
    ASSERT_EQ(value.safeGet<T>(), 25);
}

TEST(ColumnLowCardinality, Insert)
{
    testLowCardinalityNumberInsert<UInt8>(std::make_shared<DataTypeUInt8>());
    testLowCardinalityNumberInsert<UInt16>(std::make_shared<DataTypeUInt16>());
    testLowCardinalityNumberInsert<UInt32>(std::make_shared<DataTypeUInt32>());
    testLowCardinalityNumberInsert<UInt64>(std::make_shared<DataTypeUInt64>());
    testLowCardinalityNumberInsert<UInt128>(std::make_shared<DataTypeUInt128>());
    testLowCardinalityNumberInsert<UInt256>(std::make_shared<DataTypeUInt256>());

    testLowCardinalityNumberInsert<Int8>(std::make_shared<DataTypeInt8>());
    testLowCardinalityNumberInsert<Int16>(std::make_shared<DataTypeInt16>());
    testLowCardinalityNumberInsert<Int32>(std::make_shared<DataTypeInt32>());
    testLowCardinalityNumberInsert<Int64>(std::make_shared<DataTypeInt64>());
    testLowCardinalityNumberInsert<Int128>(std::make_shared<DataTypeInt128>());
    testLowCardinalityNumberInsert<Int256>(std::make_shared<DataTypeInt256>());

    testLowCardinalityNumberInsert<BFloat16>(std::make_shared<DataTypeBFloat16>());
    testLowCardinalityNumberInsert<Float32>(std::make_shared<DataTypeFloat32>());
    testLowCardinalityNumberInsert<Float64>(std::make_shared<DataTypeFloat64>());
}

TEST(ColumnLowCardinality, Clone)
{
    auto data_type = std::make_shared<DataTypeInt32>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();
    ASSERT_FALSE(assert_cast<const ColumnLowCardinality &>(*column).nestedIsNullable());

    auto nullable_column = assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();

    ASSERT_TRUE(assert_cast<const ColumnLowCardinality &>(*nullable_column).nestedIsNullable());
    ASSERT_FALSE(assert_cast<const ColumnLowCardinality &>(*column).nestedIsNullable());
}

TEST(ColumnLowCardinality, CloneNullableKeepsZeroValue)
{
    auto data_type = std::make_shared<DataTypeUInt64>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();

    column->insert(static_cast<UInt64>(0));
    column->insert(static_cast<UInt64>(1));
    column->insert(static_cast<UInt64>(2));

    auto nullable_column = assert_cast<const ColumnLowCardinality &>(*column).cloneNullable();
    const auto & nullable_lc = assert_cast<const ColumnLowCardinality &>(*nullable_column);

    ASSERT_TRUE(nullable_lc.nestedIsNullable());
    ASSERT_FALSE(nullable_lc.isNullAt(0));
    ASSERT_FALSE(nullable_lc.isNullAt(1));
    ASSERT_FALSE(nullable_lc.isNullAt(2));

    Field value;
    nullable_column->get(0, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 0);
    nullable_column->get(1, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 1);
    nullable_column->get(2, value);
    ASSERT_EQ(value.safeGet<UInt64>(), 2);
}

TEST(ColumnLowCardinality, EmptyDictionaryEmptyIndexes)
{
    /// Test edge case: empty dictionary (size=0) with empty indexes (num_rows=0)
    /// This should not throw an error, as empty indexes are always valid
    /// Regression test for bug where check was: if (max_position >= limit)
    /// When num_rows=0, max_position stays 0, and with limit=0, this incorrectly threw
    
    auto data_type = std::make_shared<DataTypeUInt32>();
    auto low_cardinality_type = std::make_shared<DataTypeLowCardinality>(data_type);
    auto column = low_cardinality_type->createColumn();
    auto & lc_column = assert_cast<ColumnLowCardinality &>(*column);
    
    // Create empty keys and indexes columns
    auto empty_keys = ColumnUInt32::create();
    auto empty_indexes = ColumnUInt8::create();
    
    // This should NOT throw an exception
    ASSERT_NO_THROW(lc_column.insertRangeFromDictionaryEncodedColumn(*empty_keys, *empty_indexes));
    
    ASSERT_EQ(column->size(), 0);
}

namespace
{
void expectColumnsEqual(const IColumn & lhs, const IColumn & rhs)
{
    ASSERT_EQ(lhs.size(), rhs.size());
    for (size_t i = 0; i < lhs.size(); ++i)
    {
        Field left;
        Field right;
        lhs.get(i, left);
        rhs.get(i, right);
        ASSERT_EQ(left, right);
    }
}

void expectInsertManyDefaultsMatchesLoop(MutableColumnPtr bulk, MutableColumnPtr one_by_one, size_t length)
{
    const size_t prefix = bulk->size();
    ASSERT_EQ(prefix, one_by_one->size());
    bulk->insertManyDefaults(length);
    for (size_t i = 0; i < length; ++i)
        one_by_one->insertDefault();
    expectColumnsEqual(*bulk, *one_by_one);
}
}

TEST(ColumnLowCardinality, InsertManyDefaultsMatchesInsertDefault)
{
    auto numeric_type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeUInt64>());
    auto numeric_bulk = numeric_type->createColumn();
    auto numeric_loop = numeric_type->createColumn();
    numeric_bulk->insert(Field{UInt64{7}});
    numeric_loop->insert(Field{UInt64{7}});
    expectInsertManyDefaultsMatchesLoop(std::move(numeric_bulk), std::move(numeric_loop), 0);
    numeric_bulk = numeric_type->createColumn();
    numeric_loop = numeric_type->createColumn();
    numeric_bulk->insert(Field{UInt64{7}});
    numeric_loop->insert(Field{UInt64{7}});
    expectInsertManyDefaultsMatchesLoop(std::move(numeric_bulk), std::move(numeric_loop), 1);
    numeric_bulk = numeric_type->createColumn();
    numeric_loop = numeric_type->createColumn();
    numeric_bulk->insert(Field{UInt64{7}});
    numeric_loop->insert(Field{UInt64{7}});
    expectInsertManyDefaultsMatchesLoop(std::move(numeric_bulk), std::move(numeric_loop), 8);

    auto nullable_type = std::make_shared<DataTypeLowCardinality>(
        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    auto nullable_bulk = nullable_type->createColumn();
    auto nullable_loop = nullable_type->createColumn();
    nullable_bulk->insert(Field{String{"x"}});
    nullable_loop->insert(Field{String{"x"}});
    expectInsertManyDefaultsMatchesLoop(std::move(nullable_bulk), std::move(nullable_loop), 5);
}

TEST(ColumnLowCardinality, InsertManyDefaultsKeepsSharedDictionary)
{
    auto dictionary_keys = ColumnUInt64::create();
    for (UInt64 value : {0, 10})
        dictionary_keys->insertValue(value);

    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(dictionary_keys));

    auto source_indexes = ColumnUInt8::create();
    source_indexes->insertValue(1);
    MutableColumnPtr column = IColumn::mutate(ColumnLowCardinality::create(dictionary, std::move(source_indexes), /* is_shared = */ true));
    auto & low_cardinality = assert_cast<ColumnLowCardinality &>(*column);
    const IColumnUnique * dictionary_before = &low_cardinality.getDictionary();

    column->insertManyDefaults(4);

    ASSERT_TRUE(low_cardinality.isSharedDictionary());
    ASSERT_EQ(&low_cardinality.getDictionary(), dictionary_before);
    ASSERT_EQ(column->size(), 5);
    ASSERT_EQ(low_cardinality.getIndexAt(0), 1);
    for (size_t i = 1; i < column->size(); ++i)
        ASSERT_EQ(low_cardinality.getIndexAt(i), low_cardinality.getDictionary().getDefaultValueIndex());
}

TEST(ColumnLowCardinality, InsertManyDefaultsPreservesIndexWidth)
{
    auto dictionary_keys = ColumnUInt64::create();
    for (UInt64 value : {0, 10})
        dictionary_keys->insertValue(value);

    ColumnPtr dictionary = DataTypeLowCardinality::createColumnUnique(DataTypeUInt64(), std::move(dictionary_keys));
    auto wide_indexes = ColumnUInt16::create();
    wide_indexes->insertValue(1);
    MutableColumnPtr column = IColumn::mutate(ColumnLowCardinality::create(dictionary, std::move(wide_indexes), /* is_shared = */ false));
    const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*column);
    ASSERT_EQ(low_cardinality.getSizeOfIndexType(), sizeof(UInt16));

    MutableColumnPtr expected = column->cloneResized(column->size());
    column->insertManyDefaults(3);
    for (size_t i = 0; i < 3; ++i)
        expected->insertDefault();

    ASSERT_EQ(assert_cast<const ColumnLowCardinality &>(*column).getSizeOfIndexType(), sizeof(UInt16));
    expectColumnsEqual(*column, *expected);
}
