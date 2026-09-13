#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/assert_cast.h>

#include <gtest/gtest.h>
#include <map>
#include <set>
#include <vector>

namespace DB
{
namespace
{

TEST(MapNullableArrayStreams, DistinctNullMaps)
{
    for (const auto marker : {ISerialization::Substream::MapKey, ISerialization::Substream::MapKeyTemplate})
    {
        for (const auto & [value_name, suffix] : std::vector<std::pair<String, String>>{
                 {"Array(Nullable(Int32))", "null1"}, {"Array(Array(Nullable(String)))", "null2"}})
        {
            SCOPED_TRACE(value_name);
            auto physical = DataTypeNullable::createForInternalUse(DataTypeFactory::instance().get(value_name));
            auto serialization = physical->getDefaultSerialization();
            ISerialization::EnumerateStreamsSettings settings;
            settings.path.emplace_back(marker);
            settings.path.back().name_of_substream = "key_k";
            std::set<String> files;
            std::set<String> cache_names;
            serialization->enumerateStreams(settings, [&](const auto & path)
            {
                EXPECT_TRUE(files.insert(ISerialization::getFileNameForStream("m", path, {})).second);
                EXPECT_TRUE(cache_names.insert(ISerialization::getSubcolumnNameForStream(path, true)).second);
            }, ISerialization::SubstreamData(serialization).withType(physical));
            String prefix = marker == ISerialization::Substream::MapKey ? "m.key_k" : "m.per_key_template";
            EXPECT_TRUE(files.contains(prefix + ".null"));
            EXPECT_TRUE(files.contains(prefix + "." + suffix));
        }
    }
}

TEST(MapNullableArrayStreams, PreserveOrdinaryArrayNames)
{
    auto type = DataTypeFactory::instance().get("Array(Nullable(Int32))");
    auto serialization = type->getDefaultSerialization();
    ISerialization::EnumerateStreamsSettings settings;
    std::set<String> files;
    serialization->enumerateStreams(settings, [&](const auto & path)
    {
        files.insert(ISerialization::getFileNameForStream("a", path, {}));
    }, ISerialization::SubstreamData(serialization).withType(type));
    EXPECT_EQ(files, (std::set<String>{"a", "a.null", "a.size0"}));
}

TEST(MapNullableArrayStreams, PresenceAndValueShareCacheInEitherOrder)
{
    auto type = DataTypeFactory::instance().get("Map(String, Array(Nullable(Int32)))");
    SerializationInfoSettings info;
    info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    auto serialization = type->getSerialization(info);
    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*serialization);
    auto column = type->createColumn();
    column->insert(Map{});
    column->insert(Map{Tuple{String("k"), Array{}}});
    column->insert(Map{Tuple{String("k"), Array{Field{}}}});
    column->insert(Map{Tuple{String("k"), Array{Int64(1), Field{}, Int64(-2), Int64(3)}}});

    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> outputs;
    ISerialization::SerializeBinaryBulkSettings write_settings;
    write_settings.getter = [&](const auto & path) -> WriteBuffer *
    {
        auto & out = outputs[ISerialization::getFileNameForStream("m", path, {})];
        if (!out)
            out = std::make_unique<WriteBufferFromOwnString>();
        return out.get();
    };
    ISerialization::SerializeBinaryBulkStatePtr write_state;
    serialization->serializeBinaryBulkStatePrefix(*column, write_settings, write_state);
    per_key.addKeys(write_state, per_key.collectNewKeys(*column, *write_state));
    per_key.initializeKeyPrefixes(write_settings, write_state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, 4, write_settings, write_state);
    serialization->serializeBinaryBulkStateSuffix(write_settings, write_state);
    for (auto & [name, out] : outputs)
        out->finalize();
    EXPECT_EQ(outputs.at("m.key_k.null")->str(), String("\1\0\0\0", 4));
    EXPECT_EQ(outputs.at("m.key_k.null1")->str(), String("\1\0\1\0\0", 5));

    for (bool presence_first : {false, true})
    {
        ISerialization::SubstreamsCache cache;
        std::map<String, std::unique_ptr<ReadBufferFromString>> inputs;
        for (bool read_presence : {presence_first, !presence_first})
        {
            auto reader = read_presence ? type->getSubcolumnSerialization("exists_k", serialization) : serialization;
            auto result_type = read_presence ? DataTypeFactory::instance().get("UInt8") : type;
            auto result = result_type->createColumn();
            ISerialization::DeserializeBinaryBulkSettings read_settings;
            read_settings.getter = [&](const auto & path) -> ReadBuffer *
            {
                String name = ISerialization::getFileNameForStream("m", path, {});
                if (read_presence)
                    EXPECT_TRUE(name == "m.keys_info" || name == "m.key_k.null") << name;
                auto & in = inputs[name];
                if (!in || name == "m.keys_info")
                    in = std::make_unique<ReadBufferFromString>(outputs.at(name)->str());
                return in.get();
            };
            ISerialization::DeserializeBinaryBulkStatePtr state;
            reader->deserializeBinaryBulkStatePrefix(read_settings, state, nullptr);
            reader->deserializeBinaryBulkWithMultipleStreams(*result, 4, read_settings, state, &cache);
            ASSERT_EQ(result->size(), 4);
            for (size_t i = 0; i < 4; ++i)
            {
                if (read_presence)
                    EXPECT_EQ(result->getUInt(i), i != 0);
                else
                    EXPECT_EQ((*result)[i], (*column)[i]);
            }
        }
    }
}

}
}
