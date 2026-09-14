#include <Columns/ColumnMap.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/assert_cast.h>

#include <gtest/gtest.h>
#include <map>
#include <set>

namespace
{
using namespace DB;

class MapPerKeyReading : public testing::Test
{
protected:
    DataTypePtr type = DataTypeFactory::instance().get("Map(String, LowCardinality(Nullable(String)))");
    SerializationPtr serialization;
    std::map<String, String> streams;

    static String streamName(const ISerialization::SubstreamPath & path)
    {
        return ISerialization::getFileNameForStream("m", path, {});
    }

    void SetUp() override
    {
        SerializationInfoSettings info;
        info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
        serialization = type->getSerialization(info);
        auto column = type->createColumn();
        column->insert(Map{Tuple{String("a"), String("first")}, Tuple{String("b"), String("other")}});
        column->insert(Map{Tuple{String("b"), String("other")}});

        std::map<String, std::unique_ptr<WriteBufferFromOwnString>> buffers;
        ISerialization::SerializeBinaryBulkSettings settings;
        settings.low_cardinality_max_dictionary_size = 8192;
        settings.low_cardinality_use_single_dictionary_for_part = true;
        settings.getter = [&](const auto & path) -> WriteBuffer *
        {
            auto & buffer = buffers[streamName(path)];
            if (!buffer)
                buffer = std::make_unique<WriteBufferFromOwnString>();
            return buffer.get();
        };
        ISerialization::SerializeBinaryBulkStatePtr state;
        serialization->serializeBinaryBulkStatePrefix(*column, settings, state);
        const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*serialization);
        per_key.addKeys(state, per_key.collectNewKeys(*column, *state));
        per_key.initializeKeyPrefixes(settings, state);
        serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, column->size(), settings, state);
        serialization->serializeBinaryBulkStateSuffix(settings, state);
        for (auto & [name, buffer] : buffers)
        {
            buffer->finalize();
            streams.emplace(name, buffer->str());
        }
    }

    void checkSingleKey(const String & key, bool missing)
    {
        auto subserialization = type->getSubcolumnSerialization("key_" + key, serialization);
        auto result_type = assert_cast<const SerializationMapKeyColumns &>(*serialization).getPhysicalValueType();
        std::map<String, std::unique_ptr<ReadBufferFromString>> buffers;
        std::set<String> accessed_keys;
        ISerialization::DeserializeBinaryBulkSettings settings;
        settings.getter = [&](const auto & path) -> ReadBuffer *
        {
            for (const auto & component : path)
                if (component.type == ISerialization::Substream::MapKey)
                    accessed_keys.insert(component.name_of_substream);
            const auto name = streamName(path);
            auto & buffer = buffers[name];
            if (!buffer)
                buffer = std::make_unique<ReadBufferFromString>(streams.at(name));
            return buffer.get();
        };
        ISerialization::DeserializeBinaryBulkStatePtr state;
        subserialization->deserializeBinaryBulkStatePrefix(settings, state, nullptr);
        ColumnPtr result = result_type->createColumn();
        subserialization->deserializeBinaryBulkWithMultipleStreams(result, 0, 2, settings, state, nullptr);
        ASSERT_EQ(result->size(), 2);
        EXPECT_EQ((*result)[0], missing ? Field{} : Field(String("first")));
        EXPECT_TRUE(result->isNullAt(1));
        EXPECT_EQ(accessed_keys, missing ? std::set<String>{} : std::set<String>{"key_a"});
    }
};

TEST_F(MapPerKeyReading, FixedKeyDoesNotReadOtherDictionaries)
{
    checkSingleKey("a", false);
}

TEST_F(MapPerKeyReading, MissingKeyDoesNotReadAnyDictionary)
{
    checkSingleKey("missing", true);
}

class MapPerKeyPresence : public testing::TestWithParam<std::tuple<String, UInt64>>
{
};

void checkPresenceReading(const String & value_name, UInt64 dictionary_limit, const Strings & subcolumns)
{
    auto type = DataTypeFactory::instance().get("Map(String, " + value_name + ")");
    SerializationInfoSettings info;
    info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
    auto serialization = type->getSerialization(info);
    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*serialization);
    auto column = type->createColumn();
    auto value = [&](UInt64 n) -> Field
    {
        if (value_name.starts_with("LowCardinality"))
            return String(n ? "value_" + std::to_string(n) : "");
        if (value_name.starts_with("Array"))
            return n ? Field(Array{n}) : Field(Array{});
        return n;
    };
    column->insert(Map{Tuple{String("a"), value(0)}, Tuple{String("a.null"), value(1)}});
    column->insert(Map{});
    column->insert(Map{Tuple{String("b"), value(2)}});
    column->insert(Map{Tuple{String("a"), value(3)}});

    std::map<String, std::unique_ptr<WriteBufferFromOwnString>> outputs;
    auto stream_name = [](const auto & path) { return ISerialization::getFileNameForStream("m", path, {}); };
    ISerialization::SerializeBinaryBulkSettings write_settings;
    write_settings.low_cardinality_max_dictionary_size = dictionary_limit;
    write_settings.low_cardinality_use_single_dictionary_for_part = true;
    write_settings.getter = [&](const auto & path) -> WriteBuffer *
    {
        auto & out = outputs[stream_name(path)];
        if (!out)
            out = std::make_unique<WriteBufferFromOwnString>();
        return out.get();
    };
    ISerialization::SerializeBinaryBulkStatePtr write_state;
    serialization->serializeBinaryBulkStatePrefix(*column, write_settings, write_state);
    per_key.addKeys(write_state, per_key.collectNewKeys(*column, *write_state));
    per_key.initializeKeyPrefixes(write_settings, write_state);
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 0, 2, write_settings, write_state);
    std::map<String, size_t> second_block_offsets;
    for (const auto & [name, out] : outputs)
        second_block_offsets.emplace(name, out->count());
    serialization->serializeBinaryBulkWithMultipleStreams(*column, 2, 2, write_settings, write_state);
    serialization->serializeBinaryBulkStateSuffix(write_settings, write_state);
    for (auto & [name, out] : outputs)
        out->finalize();

    for (const String & subcolumn : subcolumns)
    {
        SCOPED_TRACE(subcolumn);
        auto reader = type->getSubcolumnSerialization(subcolumn, serialization);
        auto result_type = DataTypeFactory::instance().get(subcolumn == "keys" ? "Array(String)" : "UInt8");
        ColumnPtr result = result_type->createColumn();
        std::map<String, std::unique_ptr<ReadBufferFromString>> inputs;
        std::set<String> dictionary_streams;
        std::set<String> accessed_keys;
        ISerialization::DeserializeBinaryBulkSettings read_settings;
        read_settings.getter = [&](const auto & path) -> ReadBuffer *
        {
            const auto kind = path.back().type;
            EXPECT_TRUE(kind == ISerialization::Substream::MapKeysInfo
                || kind == ISerialization::Substream::NullMap
                || kind == ISerialization::Substream::DictionaryKeys
                || kind == ISerialization::Substream::DictionaryIndexes);
            for (const auto & component : path)
                if (component.type == ISerialization::Substream::MapKey)
                    accessed_keys.insert(component.name_of_substream);
            const auto name = stream_name(path);
            if (kind == ISerialization::Substream::DictionaryKeys)
                dictionary_streams.insert(name);
            auto & in = inputs[name];
            if (!in)
                in = std::make_unique<ReadBufferFromString>(outputs.at(name)->str());
            return in.get();
        };
        ISerialization::DeserializeBinaryBulkStatePtr read_state;
        reader->deserializeBinaryBulkStatePrefix(read_settings, read_state, nullptr);
        ISerialization::EnumerateStreamsSettings enumerate_settings;
        reader->enumerateStreams(enumerate_settings, [&](const auto & path)
        {
            EXPECT_NE(path.back().type, ISerialization::Substream::DictionaryKeys);
            EXPECT_NE(path.back().type, ISerialization::Substream::DictionaryKeysPrefix);
        }, ISerialization::SubstreamData(reader).withType(result_type).withDeserializeState(read_state));
        /// Split reads both within and across serialized blocks.
        for (size_t limit : {1, 2, 1})
        {
            reader->deserializeBinaryBulkWithMultipleStreams(result, 0, limit, read_settings, read_state, nullptr);
            read_settings.continuous_reading = true;
        }
        ASSERT_EQ(result->size(), 4);
        if (subcolumn == "keys")
        {
            EXPECT_EQ((*result)[0], Field(Array{String("a"), String("a.null")}));
            EXPECT_EQ((*result)[1], Field(Array{}));
            EXPECT_EQ((*result)[2], Field(Array{String("b")}));
            EXPECT_EQ((*result)[3], Field(Array{String("a")}));
        }
        else
        {
            EXPECT_EQ(result->getUInt(0), subcolumn != "exists_missing");
            EXPECT_EQ(result->getUInt(1), 0);
            EXPECT_EQ(result->getUInt(2), 0);
            EXPECT_EQ(result->getUInt(3), subcolumn == "exists_a");
            EXPECT_EQ(accessed_keys, subcolumn == "exists_missing" ? std::set<String>{}
                : std::set<String>{subcolumn == "exists_a" ? "key_a" : "key_a.null"});
        }
        for (const auto & name : dictionary_streams)
            EXPECT_EQ(inputs.at(name)->count(), sizeof(UInt64)) << name;

        /// A new mark must discard pending rows, including in a cloned prefix state.
        auto seek_state = read_state->clone();
        for (auto & [name, in] : inputs)
        {
            if (dictionary_streams.contains(name) || !second_block_offsets.contains(name))
                continue;
            in = std::make_unique<ReadBufferFromString>(outputs.at(name)->str());
            in->ignore(second_block_offsets.at(name));
        }
        read_settings.continuous_reading = false;
        ColumnPtr tail = result_type->createColumn();
        reader->deserializeBinaryBulkWithMultipleStreams(tail, 0, 2, read_settings, seek_state, nullptr);
        ASSERT_EQ(tail->size(), 2);
        EXPECT_EQ((*tail)[0], (*result)[2]);
        EXPECT_EQ((*tail)[1], (*result)[3]);
    }
}

TEST_P(MapPerKeyPresence, ReadExistenceWithoutValues)
{
    const auto & [value_name, dictionary_limit] = GetParam();
    checkPresenceReading(value_name, dictionary_limit, {"exists_a", "exists_a.null", "exists_missing"});
}

TEST_P(MapPerKeyPresence, ReadKeysWithoutValues)
{
    const auto & [value_name, dictionary_limit] = GetParam();
    checkPresenceReading(value_name, dictionary_limit, {"keys"});
}

INSTANTIATE_TEST_SUITE_P(PhysicalTypes, MapPerKeyPresence, testing::Values(
    std::tuple<String, UInt64>{"UInt64", 8192},
    std::tuple<String, UInt64>{"Array(UInt64)", 8192},
    std::tuple<String, UInt64>{"LowCardinality(Nullable(String))", 8192},
    std::tuple<String, UInt64>{"LowCardinality(Nullable(String))", 0},
    std::tuple<String, UInt64>{"LowCardinality(Nullable(String))", 2}));

void expectSameAsSingleKeyExtract(
    const IColumn & nested_column,
    const DataTypePtr & key_type,
    const DataTypePtr & physical_type,
    const std::vector<Field> & keys,
    size_t start,
    size_t end)
{
    std::vector<MutableColumnPtr> pivoted;
    std::vector<IColumn *> pivoted_ptrs;
    pivoted.reserve(keys.size());
    pivoted_ptrs.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
    {
        pivoted.emplace_back(physical_type->createColumn());
        pivoted_ptrs.push_back(pivoted.back().get());
    }

    extractRegisteredKeyValuesFromMap(nested_column, keys, pivoted_ptrs, start, end);

    for (size_t i = 0; i < keys.size(); ++i)
    {
        auto key_column = key_type->createColumn();
        key_column->insert(keys[i]);
        auto expected = physical_type->createColumn();
        extractKeyValueFromMap(nested_column, *key_column, *expected, start, end);
        ASSERT_EQ(pivoted[i]->size(), expected->size());
        for (size_t row = 0; row < expected->size(); ++row)
        {
            Field left;
            Field right;
            pivoted[i]->get(row, left);
            expected->get(row, right);
            ASSERT_EQ(left, right) << "key index " << i << " row " << row;
        }
    }
}

TEST(MapKeyExtract, RegisteredKeysMatchSingleKeyExtract)
{
    auto key_type = std::make_shared<DataTypeString>();
    auto value_type = std::make_shared<DataTypeUInt64>();
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto physical_type = std::make_shared<DataTypeNullable>(value_type);
    auto column = map_type->createColumn();
    column->insert(Map{Tuple{String("a"), UInt64(1)}, Tuple{String("b"), UInt64(2)}, Tuple{String("a"), UInt64(99)}});
    column->insert(Map{Tuple{String("b"), UInt64(0)}});
    column->insert(Map{});
    column->insert(Map{Tuple{String(""), UInt64(5)}, Tuple{String("c"), UInt64(6)}});

    const auto & map = assert_cast<const ColumnMap &>(*column);
    const std::vector<Field> keys{String("a"), String("b"), String("c"), String(""), String("missing")};
    expectSameAsSingleKeyExtract(*map.getNestedColumnPtr(), key_type, physical_type, keys, 0, column->size());
    expectSameAsSingleKeyExtract(*map.getNestedColumnPtr(), key_type, physical_type, keys, 1, 3);
    expectSameAsSingleKeyExtract(*map.getNestedColumnPtr(), key_type, physical_type, {String("b")}, 0, column->size());
    expectSameAsSingleKeyExtract(*map.getNestedColumnPtr(), key_type, physical_type, {}, 0, column->size());
}

TEST(MapKeyExtract, NumericKeysMatchSingleKeyExtract)
{
    auto key_type = std::make_shared<DataTypeUInt64>();
    auto value_type = std::make_shared<DataTypeInt32>();
    auto map_type = std::make_shared<DataTypeMap>(key_type, value_type);
    auto physical_type = std::make_shared<DataTypeNullable>(value_type);
    auto column = map_type->createColumn();
    column->insert(Map{Tuple{UInt64(0), Int32(-1)}, Tuple{UInt64(2), Int32(3)}});
    column->insert(Map{Tuple{UInt64(2), Int32(4)}, Tuple{UInt64(2), Int32(8)}});

    const auto & map = assert_cast<const ColumnMap &>(*column);
    const std::vector<Field> keys{UInt64(0), UInt64(2), UInt64(7)};
    expectSameAsSingleKeyExtract(*map.getNestedColumnPtr(), key_type, physical_type, keys, 0, column->size());
}

}
