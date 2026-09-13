#include <Columns/ColumnMap.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapKeyColumns.h>
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

#include <gtest/gtest.h>
#include <filesystem>
#include <unistd.h>

namespace
{
using namespace DB;

struct Scenario
{
    String name;
    std::vector<size_t> block_rows;
    std::vector<size_t> first_rows;
};

class PerKeyMapMarks : public testing::TestWithParam<std::tuple<Scenario, bool, bool>>
{
protected:
    std::filesystem::path directory;

    void SetUp() override
    {
        MainThreadStatus::getInstance();
        getContext();
        directory = std::filesystem::current_path() / "tmp"
            / ("per_key_marks_" + std::to_string(getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(directory);
    }

    void TearDown() override
    {
        std::filesystem::remove_all(directory);
    }
};

TEST_P(PerKeyMapMarks, EveryGranuleCanBeReadFromItsMark)
{
    const auto & [scenario, compressed_marks, nullable_array] = GetParam();
    auto disk = std::make_shared<DiskLocal>("test_disk", directory.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
    auto value_type = DataTypeFactory::instance().get(nullable_array ? "Array(Nullable(Int32))" : "UInt64");
    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), value_type);
    auto value = [&](size_t key, size_t row) -> Field
    {
        if (!nullable_array)
            return UInt64(1000 * (key + 1) + row);
        if (row % 3 == 0)
            return Array{};
        if (row % 3 == 1)
            return Array{Field{}};
        return Array{Int64(1000 * (key + 1) + row), Field{}, -Int64(row)};
    };
    auto metadata = std::make_shared<StorageInMemoryMetadata>();
    metadata->setColumns(ColumnsDescription({{"m", map_type}}));
    auto storage_settings = std::make_shared<MergeTreeSettings>();
    storage_settings->set("index_granularity", UInt64(4));
    storage_settings->set("index_granularity_bytes", UInt64(0));
    MergeTreeIndexGranularityInfo info(*storage_settings, MarkType(true, compressed_marks, false, MergeTreeDataPartType::Wide));
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
        "all_0_0_0", "PerKeyMapMarks", serializations, storage, info, storage_settings,
        metadata->getColumns().getAllPhysical(), metadata, {}, info.mark_type.getFileExtension(),
        CompressionCodecFactory::instance().get("LZ4", {}), settings, granularity, nullptr);

    size_t total_rows = 0;
    for (size_t block_rows : scenario.block_rows)
    {
        auto column = map_type->createColumn();
        for (size_t row = total_rows; row < total_rows + block_rows; ++row)
        {
            Map values;
            for (size_t key = 0; key < scenario.first_rows.size(); ++key)
                if (row >= scenario.first_rows[key])
                    values.emplace_back(Tuple{String(1, static_cast<char>('a' + key)), value(key, row)});
            column->insert(values);
        }
        /// Each call supplies exactly one block to the same writer, including incomplete granules.
        writer.write(Block{{std::move(column), map_type, "m"}}, nullptr, nullptr);
        total_rows += block_rows;
    }
    writer.finalizeIndexGranularity();
    MergeTreeDataPartChecksums checksums;
    NameSet removed;
    writer.fillChecksums(checksums, removed);
    writer.finish(false);
    const auto cached_marks = writer.releaseCachedMarks();
    for (const auto & [name, unused_marks] : cached_marks)
        EXPECT_EQ(name.find("per_key_template"), String::npos);

    const size_t data_marks = (total_rows + 3) / 4;
    ASSERT_EQ(granularity->getMarksCount(), data_marks + 1);
    auto physical_type = getValueTypeForMapKeyColumn(value_type);
    auto physical_serialization = physical_type->getDefaultSerialization();
    for (size_t key = 0; key < scenario.first_rows.size(); ++key)
    {
        SCOPED_TRACE(testing::Message() << "key=" << key);
        std::unordered_map<String, std::vector<MarkInCompressedFile>> marks;
        std::unordered_map<String, std::unique_ptr<CompressedReadBufferFromFile>> streams;
        ISerialization::EnumerateStreamsSettings enumerate_settings;
        ISerialization::SubstreamPath prefix;
        prefix.push_back(ISerialization::Substream::MapKey);
        prefix.back().name_of_substream = "key_" + String(1, static_cast<char>('a' + key));
        enumerate_settings.path = prefix;
        physical_serialization->enumerateStreams(enumerate_settings, [&](const auto & path)
        {
            const auto name = ISerialization::getFileNameForStream(
                {"m", map_type}, path, ISerialization::StreamFileNameSettings(*storage_settings));
            auto file = storage->readFile(name + info.mark_type.getFileExtension(), {}, std::nullopt);
            std::unique_ptr<CompressedReadBuffer> decompressor;
            ReadBuffer * input = file.get();
            if (compressed_marks)
            {
                decompressor = std::make_unique<CompressedReadBuffer>(*file);
                input = decompressor.get();
            }
            auto & stream_marks = marks[name];
            while (!input->eof())
            {
                MarkInCompressedFile mark;
                UInt64 rows;
                readBinaryLittleEndian(mark.offset_in_compressed_file, *input);
                readBinaryLittleEndian(mark.offset_in_decompressed_block, *input);
                readBinaryLittleEndian(rows, *input);
                const auto index = stream_marks.size();
                ASSERT_LT(index, granularity->getMarksCount()) << name;
                EXPECT_EQ(rows, granularity->getMarkRows(index)) << name << " mark=" << index;
                stream_marks.push_back(mark);
            }
            EXPECT_EQ(stream_marks.size(), granularity->getMarksCount()) << name;
            const auto & cached = *cached_marks.at(name);
            ASSERT_EQ(cached.size(), stream_marks.size()) << name;
            for (size_t index = 0; index < cached.size(); ++index)
            {
                EXPECT_EQ(cached[index].offset_in_compressed_file, stream_marks[index].offset_in_compressed_file) << name;
                EXPECT_EQ(cached[index].offset_in_decompressed_block, stream_marks[index].offset_in_decompressed_block) << name;
            }
            streams[name] = std::make_unique<CompressedReadBufferFromFile>(storage->readFile(name + ".bin", {}, std::nullopt));
        }, ISerialization::SubstreamData(physical_serialization).withType(physical_type));

        for (const auto & [name, stream_marks] : marks)
            ASSERT_EQ(stream_marks.size(), granularity->getMarksCount()) << name;

        /// Seek backwards to ensure decoding never depends on a previous sequential read.
        for (size_t index = data_marks; index-- > 0;)
        {
            SCOPED_TRACE(testing::Message() << "mark=" << index);
            for (auto & [name, stream] : streams)
            {
                const auto & mark = marks.at(name)[index];
                stream->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
            }
            ISerialization::DeserializeBinaryBulkSettings read_settings;
            read_settings.path = prefix;
            read_settings.getter = [&](const auto & path) -> ReadBuffer *
            {
                return streams.at(ISerialization::getFileNameForStream(
                    {"m", map_type}, path, ISerialization::StreamFileNameSettings(*storage_settings))).get();
            };
            ISerialization::DeserializeBinaryBulkStatePtr state;
            auto result = physical_type->createColumn();
            const size_t rows = granularity->getMarkRows(index);
            physical_serialization->deserializeBinaryBulkWithMultipleStreams(*result, rows, read_settings, state, nullptr);
            ASSERT_EQ(result->size(), rows);
            for (size_t offset = 0; offset < rows; ++offset)
            {
                const size_t row = 4 * index + offset;
                const Field expected = row < scenario.first_rows[key] ? Field{} : value(key, row);
                EXPECT_EQ((*result)[offset], expected) << "row=" << row;
            }
        }
    }
}

INSTANTIATE_TEST_SUITE_P(Boundaries, PerKeyMapMarks, testing::Combine(testing::Values(
    Scenario{"ExistingKeys", {6, 6}, {0, 0}},
    Scenario{"CompletedHistory", {8, 8}, {0, 8}},
    Scenario{"PendingOnly", {2, 6}, {0, 2}},
    Scenario{"HistoryAndPending", {6, 6}, {0, 6}},
    Scenario{"MultiplePendingKeys", {2, 1, 1, 4}, {0, 2, 3}},
    Scenario{"FinalizePending", {6, 1}, {0, 6}},
    Scenario{"EmptyFirstBlock", {6, 6}, {6}}), testing::Bool(), testing::Bool()),
    [](const testing::TestParamInfo<PerKeyMapMarks::ParamType> & param)
    {
        return std::get<0>(param.param).name + (std::get<1>(param.param) ? "CompressedMarks" : "PlainMarks")
            + (std::get<2>(param.param) ? "NullableArray" : "Scalar");
    });
}
