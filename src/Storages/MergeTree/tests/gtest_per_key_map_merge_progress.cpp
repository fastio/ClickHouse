#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/MergeTree/ColumnSizeEstimator.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(PerKeyMapMergeProgress, DistributesMapBytesAcrossKeys)
{
    std::map<String, UInt64> column_to_size{{"id", 100}, {"m", 1000}};
    addMapKeyColumnsGatheringSizes(column_to_size, {"m.key_a", "m.key_b", "m.key_c"}, 1000);

    EXPECT_EQ(column_to_size["m.key_a"] + column_to_size["m.key_b"] + column_to_size["m.key_c"], 1000u);
    EXPECT_GE(column_to_size["m.key_a"], 333u);
    EXPECT_LE(column_to_size["m.key_a"], 334u);

    NamesAndTypesList key_columns{{"id", std::make_shared<DataTypeUInt64>()}};
    NamesAndTypesList gathering{
        {"m.key_a", std::make_shared<DataTypeUInt64>()},
        {"m.key_b", std::make_shared<DataTypeUInt64>()},
        {"m.key_c", std::make_shared<DataTypeUInt64>()},
    };

    ColumnSizeEstimator estimator(std::move(column_to_size), key_columns, gathering);
    const Float64 key_weight = estimator.keyColumnsWeight();
    const Float64 map_weight
        = estimator.columnWeight("m.key_a") + estimator.columnWeight("m.key_b") + estimator.columnWeight("m.key_c");

    EXPECT_GT(estimator.columnWeight("m.key_a"), 0.0);
    EXPECT_GT(estimator.columnWeight("m.key_b"), 0.0);
    EXPECT_GT(estimator.columnWeight("m.key_c"), 0.0);
    EXPECT_NEAR(key_weight + map_weight, 1.0, 1e-12);
    EXPECT_NEAR(key_weight, 100.0 / 1100.0, 1e-12);
}

TEST(PerKeyMapMergeProgress, ZeroPayloadKeysStillHaveWeight)
{
    std::map<String, UInt64> column_to_size{{"id", 50}};
    addMapKeyColumnsGatheringSizes(column_to_size, {"m.key_a", "m.key_b"}, 0);

    EXPECT_EQ(column_to_size["m.key_a"], 1u);
    EXPECT_EQ(column_to_size["m.key_b"], 1u);

    NamesAndTypesList key_columns{{"id", std::make_shared<DataTypeUInt64>()}};
    NamesAndTypesList gathering{
        {"m.key_a", std::make_shared<DataTypeUInt64>()},
        {"m.key_b", std::make_shared<DataTypeUInt64>()},
    };

    ColumnSizeEstimator estimator(std::move(column_to_size), key_columns, gathering);
    EXPECT_GT(estimator.columnWeight("m.key_a"), 0.0);
    EXPECT_GT(estimator.columnWeight("m.key_b"), 0.0);
    EXPECT_LT(estimator.keyColumnsWeight(), 1.0);
}

TEST(PerKeyMapMergeProgress, EmptyKeyListIsNoOp)
{
    std::map<String, UInt64> column_to_size{{"id", 10}};
    addMapKeyColumnsGatheringSizes(column_to_size, {}, 999);
    EXPECT_EQ(column_to_size.size(), 1u);
    EXPECT_EQ(column_to_size["id"], 10u);
}

TEST(PerKeyMapMergeProgress, UnmappedKeysWouldBeZero)
{
    std::map<String, UInt64> column_to_size{{"id", 100}, {"m", 1000}};
    NamesAndTypesList key_columns{{"id", std::make_shared<DataTypeUInt64>()}};
    NamesAndTypesList gathering{
        {"m.key_a", std::make_shared<DataTypeUInt64>()},
        {"m.key_b", std::make_shared<DataTypeUInt64>()},
    };

    ColumnSizeEstimator estimator(std::move(column_to_size), key_columns, gathering);
    EXPECT_EQ(estimator.columnWeight("m.key_a"), 0.0);
    EXPECT_EQ(estimator.columnWeight("m.key_b"), 0.0);
    EXPECT_EQ(estimator.keyColumnsWeight(), 1.0);
}
