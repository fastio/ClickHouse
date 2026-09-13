#include <Storages/MergeTree/tests/gtest_per_key_map_context.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeReaderWide.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Compression/CompressionFactory.h>

namespace
{
using namespace DB;
using PerKeyMapDiskPresence = PerKeyMapStorageTest;

TEST_F(PerKeyMapDiskPresence, ReadTogetherAndSeek)
{
    const auto map_column = metadata->getColumns().getPhysical("m");
    const NamesAndTypesList columns{map_column};
    auto settings = std::make_shared<MergeTreeSettings>(*storage->getSettings());
    settings->set("index_granularity", UInt64(2));
    settings->set("index_granularity_bytes", UInt64(0));
    auto disk = std::make_shared<DiskLocal>("test_disk", std::filesystem::absolute(directory).string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto part_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
    auto part = std::make_shared<MergeTreeDataPartWide>(
        *storage, *settings, "all_0_0_0", MergeTreePartInfo("all", 0, 0, 0), part_storage, nullptr, PartDirIntent::CreateFresh);
    SerializationInfoSettings info;
    info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    part->setColumns(columns, SerializationInfoByName(columns, info), 0);
    auto granularity = std::make_shared<MergeTreeIndexGranularityConstant>(2);
    part->index_granularity = granularity;
    MergeTreeWriterSettings writer_settings;
    writer_settings.can_use_adaptive_granularity = true;
    writer_settings.rewrite_primary_key = true;
    writer_settings.min_compress_block_size = 16;
    writer_settings.max_compress_block_size = 32;
    writer_settings.marks_compression_codec = "LZ4";
    writer_settings.marks_compress_block_size = 32;
    SerializationByName serializations{{"m", map_column.type->getSerialization(info)}};
    MergeTreeDataPartWriterWide writer(
        part->name, "PerKeyMapDiskPresence", serializations, part_storage, part->index_granularity_info,
        settings, columns, metadata, {}, part->getMarksFileExtension(), CompressionCodecFactory::instance().get("LZ4", {}),
        writer_settings, granularity, nullptr);
    auto values = map_column.type->createColumn();
    values->insert(Map{Tuple{String("a"), String("")}, Tuple{String("a.null"), String("dot")}, Tuple{String("b"), String("other")}});
    values->insert(Map{});
    values->insert(Map{Tuple{String("b"), String("other")}});
    values->insert(Map{Tuple{String("a"), String("last")}});
    writer.write(Block{{std::move(values), map_column.type, "m"}}, nullptr, nullptr);
    writer.finalizeIndexGranularity();
    NameSet removed;
    writer.fillChecksums(part->checksums, removed);
    writer.finish(false);
    part->rows_count = 4;
    part->setColumnsSubstreams(writer.getColumnsSubstreams());

    NamesAndTypesList requested;
    for (const String name : {"keys", "exists_a", "exists_missing", "key_a", "key_a.null"})
        requested.emplace_back("m", name, map_column.type, map_column.type->getSubcolumnType(name));
    auto read_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>());
    auto reader_settings = MergeTreeReaderSettings::createFromSettings();
    reader_settings.load_marks_asynchronously = false;
    MergeTreeReaderWide reader(
        read_info, requested, {}, storage->getStorageSnapshotWithoutData(metadata, context), settings,
        nullptr, nullptr, nullptr, MarkRanges{MarkRange(0, granularity->getMarksCount())}, reader_settings);
    MutableColumns result(requested.size());
    ASSERT_EQ(reader.readRows(0, false, 2, result), 2);
    ASSERT_EQ(reader.readRows(0, true, 2, result), 2);
    ASSERT_EQ(result.size(), 5);
    EXPECT_EQ((*result[0])[0], Field(Array{String("a"), String("a.null"), String("b")}));
    EXPECT_EQ((*result[0])[1], Field(Array{}));
    EXPECT_EQ((*result[0])[2], Field(Array{String("b")}));
    EXPECT_EQ((*result[0])[3], Field(Array{String("a")}));
    for (size_t row = 0; row < 4; ++row)
    {
        EXPECT_EQ(result[1]->getUInt(row), row == 0 || row == 3);
        EXPECT_EQ(result[2]->getUInt(row), 0);
    }
    EXPECT_EQ((*result[4])[0], Field(String("dot")));
    EXPECT_TRUE(result[4]->isNullAt(3));
    EXPECT_EQ((*result[3])[0], Field(String("")));
    EXPECT_TRUE(result[3]->isNullAt(1));
    EXPECT_TRUE(result[3]->isNullAt(2));
    EXPECT_EQ((*result[3])[3], Field(String("last")));
    MutableColumns tail(requested.size());
    ASSERT_EQ(reader.readRows(1, false, 2, tail), 2);
    for (size_t col = 0; col < result.size(); ++col)
    {
        EXPECT_EQ((*tail[col])[0], (*result[col])[2]);
        EXPECT_EQ((*tail[col])[1], (*result[col])[3]);
    }
}

class PerKeyMapDictionaryMarks : public PerKeyMapStorageTest, public testing::WithParamInterface<std::tuple<String, UInt64>>
{
};

TEST_P(PerKeyMapDictionaryMarks, PrefixBeforeFirstMarkAndRoundTrip)
{
    const auto & [nested_type, dictionary_size] = GetParam();
    auto map_type = DataTypeFactory::instance().get("Map(String, LowCardinality(Nullable(" + nested_type + ")))");
    auto part_metadata = std::make_shared<StorageInMemoryMetadata>(*metadata);
    const NamesAndTypesList columns{{"m", map_type}};
    part_metadata->setColumns(ColumnsDescription(columns));
    auto settings = std::make_shared<MergeTreeSettings>(*storage->getSettings());
    settings->set("index_granularity", UInt64(2));
    settings->set("index_granularity_bytes", UInt64(0));
    auto disk = std::make_shared<DiskLocal>("test_disk", std::filesystem::absolute(directory).string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
    auto part_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");
    auto part = std::make_shared<MergeTreeDataPartWide>(
        *storage, *settings, "all_0_0_0", MergeTreePartInfo("all", 0, 0, 0), part_storage, nullptr, PartDirIntent::CreateFresh);
    SerializationInfoSettings info;
    info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    part->setColumns(columns, SerializationInfoByName(columns, info), 0);
    auto granularity = std::make_shared<MergeTreeIndexGranularityConstant>(2);
    part->index_granularity = granularity;
    MergeTreeWriterSettings writer_settings;
    writer_settings.can_use_adaptive_granularity = true;
    writer_settings.rewrite_primary_key = true;
    writer_settings.save_marks_in_cache = true;
    writer_settings.min_compress_block_size = 65536;
    writer_settings.max_compress_block_size = 65536;
    writer_settings.low_cardinality_max_dictionary_size = dictionary_size;
    writer_settings.low_cardinality_use_single_dictionary_for_part = true;
    writer_settings.marks_compression_codec = "LZ4";
    writer_settings.marks_compress_block_size = 32;
    SerializationByName serializations{{"m", map_type->getSerialization(info)}};
    MergeTreeDataPartWriterWide writer(
        part->name, "PerKeyMapDictionaryMarks", serializations, part_storage, part->index_granularity_info,
        settings, columns, part_metadata, {}, part->getMarksFileExtension(), CompressionCodecFactory::instance().get("LZ4", {}),
        writer_settings, granularity, nullptr);
    auto value = [&](Int64 number) -> Field
    {
        return nested_type == "String" ? Field(number ? std::to_string(number) : String{}) : Field(number);
    };
    const std::vector<Field> a{value(1), Field{}, value(2), Field{}, value(0), value(3)};
    const std::vector<Field> b{value(0), Field{}, Field{}, value(3), Field{}, value(3)};
    auto expected = map_type->createColumn();
    for (size_t row = 0; row < a.size(); ++row)
    {
        Map map;
        if (!a[row].isNull())
            map.emplace_back(Tuple{String("a"), a[row]});
        if (!b[row].isNull())
            map.emplace_back(Tuple{String("b"), b[row]});
        expected->insert(map);
    }
    /// Split inside a granule to check that subsequent blocks do not repeat the prefix.
    for (size_t offset : {0, 3})
    {
        auto block = expected->cloneEmpty();
        block->insertRangeFrom(*expected, offset, 3);
        writer.write(Block{{std::move(block), map_type, "m"}}, nullptr, nullptr);
    }
    writer.finalizeIndexGranularity();
    NameSet removed;
    writer.fillChecksums(part->checksums, removed);
    writer.finish(false);
    part->rows_count = expected->size();
    part->setColumnsSubstreams(writer.getColumnsSubstreams());
    const auto marks = writer.releaseCachedMarks();
    for (const String key : {"a", "b"})
    {
        const auto & first = marks.at("m.key_" + key + ".dict")->front();
        EXPECT_EQ(first.offset_in_compressed_file, 0);
        EXPECT_EQ(first.offset_in_decompressed_block, sizeof(UInt64)) << key;
    }

    for (bool full_map : {true, false})
    {
        SCOPED_TRACE(full_map ? "full Map" : "key values and existence");
        NamesAndTypesList requested;
        if (full_map)
            requested.emplace_back("m", map_type);
        else
            for (const String name : {"key_a", "key_b", "exists_a"})
                requested.emplace_back("m", name, map_type, map_type->getSubcolumnType(name));
        auto read_info = std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part, std::make_shared<AlterConversions>());
        auto reader_settings = MergeTreeReaderSettings::createFromSettings();
        reader_settings.load_marks_asynchronously = false;
        MergeTreeReaderWide reader(
            read_info, requested, {}, storage->getStorageSnapshotWithoutData(part_metadata, context), settings,
            nullptr, nullptr, nullptr, MarkRanges{MarkRange(0, granularity->getMarksCount())}, reader_settings);
        auto check = [&](const MutableColumns & result, size_t offset, size_t count)
        {
            for (size_t row = 0; row < count; ++row)
            {
                if (full_map)
                    EXPECT_EQ((*result[0])[row], (*expected)[offset + row]);
                else
                {
                    EXPECT_EQ((*result[0])[row], a[offset + row]);
                    EXPECT_EQ((*result[1])[row], b[offset + row]);
                    EXPECT_EQ(result[2]->getUInt(row), !a[offset + row].isNull());
                }
            }
        };
        MutableColumns result(requested.size());
        ASSERT_EQ(reader.readRows(0, false, 2, result), 2);
        ASSERT_EQ(reader.readRows(0, true, 4, result), 4);
        check(result, 0, 6);
        MutableColumns tail(requested.size());
        ASSERT_EQ(reader.readRows(1, false, 2, tail), 2);
        check(tail, 2, 2);
    }
}

INSTANTIATE_TEST_SUITE_P(
    Dictionaries, PerKeyMapDictionaryMarks,
    testing::Combine(testing::Values(String("String"), String("Int64")), testing::Values(UInt64(8192), UInt64(2))));

}
