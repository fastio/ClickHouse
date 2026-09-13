#include <Columns/ColumnMap.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadHelpers.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>

namespace ProfileEvents
{
extern const Event FileSync;
}

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace
{
using namespace DB;

class PerKeyMapWriter : public testing::Test
{
protected:
    std::filesystem::path directory;
    DataTypePtr map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeUInt64>());

    void SetUp() override
    {
        MainThreadStatus::getInstance();
        getContext();
        directory = std::filesystem::current_path() / "tmp"
            / ("per_key_writer_" + std::to_string(getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(directory);
    }

    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }

    std::unique_ptr<MergeTreeDataPartWriterWide> makeWriter(UInt64 max_keys = 1024)
    {
        auto disk = std::make_shared<DiskLocal>("test_disk", directory.string() + "/");
        auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
        auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
        auto metadata = std::make_shared<StorageInMemoryMetadata>();
        metadata->setColumns(ColumnsDescription({{"m", map_type}}));
        auto storage_settings = std::make_shared<MergeTreeSettings>();
        storage_settings->set("max_keys_in_map", max_keys);
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
        auto writer = std::make_unique<MergeTreeDataPartWriterWide>(
            "all_0_0_0", "PerKeyMapMarks", serializations, storage, info, storage_settings,
            metadata->getColumns().getAllPhysical(), metadata, std::vector<MergeTreeIndexPtr>{}, info.mark_type.getFileExtension(),
            CompressionCodecFactory::instance().get("LZ4", {}), settings, granularity, nullptr);
        return writer;

    }

    Block block(std::initializer_list<String> keys)
    {
        auto column = map_type->createColumn();
        Map values;
        for (const auto & key : keys)
            values.emplace_back(Tuple{key, UInt64(1)});
        for (size_t row = 0; row < 6; ++row)
            column->insert(values);
        return Block{{std::move(column), map_type, "m"}};
    }
};

TEST_F(PerKeyMapWriter, NewKeysShareTemplateSync)
{
    auto writer = makeWriter();
    writer->write(block({"a"}), nullptr, nullptr);
    auto before = CurrentThread::getProfileEvents()[ProfileEvents::FileSync];
    writer->write(block({"a", "b"}), nullptr, nullptr);
    auto single_key_syncs = CurrentThread::getProfileEvents()[ProfileEvents::FileSync] - before;
    ASSERT_GT(single_key_syncs, 0);
    before = CurrentThread::getProfileEvents()[ProfileEvents::FileSync];
    writer->write(block({"a", "b", "c", "d", "e"}), nullptr, nullptr);
    EXPECT_EQ(CurrentThread::getProfileEvents()[ProfileEvents::FileSync] - before, single_key_syncs);
    writer->finalizeIndexGranularity();
    MergeTreeDataPartChecksums checksums;
    NameSet removed;
    writer->fillChecksums(checksums, removed);
    writer->finish(false);
}

TEST_F(PerKeyMapWriter, KeyLimitIncludesActualCount)
{
    auto writer = makeWriter(2);
    writer->write(block({"a", "b"}), nullptr, nullptr);
    try
    {
        writer->write(block({"a", "c", "d"}), nullptr, nullptr);
        FAIL() << "Expected the key limit to reject the block";
    }
    catch (const Exception & exception)
    {
        EXPECT_EQ(exception.code(), ErrorCodes::LIMIT_EXCEEDED);
        EXPECT_NE(exception.message().find("m"), String::npos);
        EXPECT_NE(exception.message().find("4"), String::npos);
        EXPECT_NE(exception.message().find("max_keys_in_map (2)"), String::npos);
    }
    writer->cancel();
}
}
