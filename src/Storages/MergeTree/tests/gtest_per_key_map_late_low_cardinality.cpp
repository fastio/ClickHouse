#include <Columns/ColumnMap.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapKeyColumns.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>

namespace DB::ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
using namespace DB;

struct Scenario
{
    String name;
    size_t depth;
    bool late_key;
    bool empty_array;
};

class PerKeyMapLateLowCardinality : public testing::TestWithParam<Scenario>
{
protected:
    std::filesystem::path directory;

    void SetUp() override
    {
        MainThreadStatus::getInstance();
        getContext();
        directory = std::filesystem::current_path() / "tmp"
            / ("per_key_late_lc_" + std::to_string(getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(directory);
    }

    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
};

TEST_P(PerKeyMapLateLowCardinality, RejectLateKeysBeforeCopyingTemplate)
{
    const auto & scenario = GetParam();
    auto disk = std::make_shared<DiskLocal>("test_disk", directory.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
    DataTypePtr value_type = std::make_shared<DataTypeLowCardinality>(makeNullable(std::make_shared<DataTypeString>()));
    Field value = String("value");
    for (size_t level = 0; level < scenario.depth; ++level)
    {
        value_type = std::make_shared<DataTypeArray>(value_type);
        value = Array{value};
    }
    if (scenario.empty_array)
        value = Array{};
    ASSERT_TRUE(canBeMapKeyColumnsValueType(value_type));
    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), value_type);
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(ColumnsDescription({{"m", map_type}}));
    auto storage_settings = std::make_shared<MergeTreeSettings>();
    storage_settings->set("index_granularity", UInt64(4));
    storage_settings->set("index_granularity_bytes", UInt64(0));
    MergeTreeIndexGranularityInfo info(*storage_settings, MarkType(true, false, false, MergeTreeDataPartType::Wide));
    auto granularity = std::make_shared<MergeTreeIndexGranularityConstant>(4);
    MergeTreeWriterSettings settings;
    settings.can_use_adaptive_granularity = true;
    settings.rewrite_primary_key = true;
    settings.save_marks_in_cache = true;
    settings.min_compress_block_size = 16;
    settings.max_compress_block_size = 32;
    settings.marks_compression_codec = "LZ4";
    settings.marks_compress_block_size = 32;
    SerializationInfoSettings serialization_settings;
    serialization_settings.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    SerializationByName serializations{{"m", map_type->getSerialization(serialization_settings)}};
    MergeTreeDataPartWriterWide writer(
        "all_0_0_0", "PerKeyMapLateLowCardinality", serializations, storage, info, storage_settings,
        metadata->getColumns().getAllPhysical(), metadata, {}, info.mark_type.getFileExtension(),
        CompressionCodecFactory::instance().get("LZ4", {}), settings, granularity, nullptr);

    auto make_block = [&](const String & key)
    {
        auto column = map_type->createColumn();
        for (size_t row = 0; row < 8; ++row)
            column->insert(Map{Tuple{key, value}});
        return Block{{std::move(column), map_type, "m"}};
    };

    ASSERT_NO_THROW(writer.write(make_block("a"), nullptr));
    if (scenario.late_key)
    {
        try
        {
            writer.write(make_block("b"), nullptr);
            FAIL() << "Expected the second block with a new key to throw";
        }
        catch (const Exception & exception)
        {
            EXPECT_EQ(exception.code(), ErrorCodes::NOT_IMPLEMENTED);
            EXPECT_NE(exception.message().find("after the first written block is not supported"), String::npos);
        }
    }
    else
    {
        ASSERT_NO_THROW(writer.write(make_block("a"), nullptr));
        writer.finalizeIndexGranularity();
        MergeTreeDataPartChecksums checksums;
        NameSet removed;
        ASSERT_NO_THROW(writer.fillChecksums(checksums, removed));
        ASSERT_NO_THROW(writer.finish(false));
    }
}

INSTANTIATE_TEST_SUITE_P(CrossBlock, PerKeyMapLateLowCardinality, testing::Values(
    Scenario{"TopLevelLateKey", 0, true, false},
    Scenario{"ArrayLateKey", 1, true, false},
    Scenario{"NestedArrayLateKey", 2, true, false},
    Scenario{"EmptyArrayLateKey", 1, true, true},
    Scenario{"TopLevelExistingKey", 0, false, false},
    Scenario{"ArrayExistingKey", 1, false, false},
    Scenario{"NestedArrayExistingKey", 2, false, false}),
    [](const testing::TestParamInfo<Scenario> & param)
    {
        return param.param.name;
    });
}
