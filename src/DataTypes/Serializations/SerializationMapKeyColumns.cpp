#include <DataTypes/Serializations/SerializationMapKeyColumns.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationLowCardinality.h>
#include <DataTypes/Serializations/SerializationNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeMapKeyColumns.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Upper bound for the pre-allocation of the key column when reading a manifest.
/// `key_count` comes straight from the on-disk stream and can be arbitrary on a
/// corrupt or truncated part, so we never reserve more than this up front. The
/// read loop still consumes exactly `key_count` keys and grows the column
/// naturally for legitimately large manifests; if the data is short the per-key
/// `deserializeBinary` hits end of stream and throws before over-allocating.
constexpr UInt64 MANIFEST_KEY_COUNT_RESERVE_LIMIT = 1ULL << 20;

String serializeKeyText(const ISerialization & key_serialization, const IColumn & key_column, size_t row)
{
    WriteBufferFromOwnString buf;
    key_serialization.serializeText(key_column, row, buf, FormatSettings{});
    return buf.str();
}

String serializeKeyText(const ISerialization & key_serialization, const DataTypePtr & key_type, const Field & key)
{
    auto column = key_type->createColumn();
    column->insert(key);
    return serializeKeyText(key_serialization, *column, 0);
}

void collectFirstSeenKeys(
    const ColumnMap & map,
    const ISerialization & key_serialization,
    std::vector<Field> & keys,
    std::map<Field, String> & key_names)
{
    const auto & nested_array = map.getNestedColumn();
    const auto & tuple = map.getNestedData();
    const auto & keys_column = tuple.getColumn(0);
    const auto & offsets = nested_array.getOffsets();

    for (size_t row = 0; row < map.size(); ++row)
    {
        size_t start = offsets[ssize_t(row) - 1];
        size_t end = offsets[row];
        for (size_t i = start; i < end; ++i)
        {
            Field key;
            keys_column.get(i, key);
            if (key_names.contains(key))
                continue;
            String name = DataTypeMap::KEY_SUBCOLUMN_PREFIX.data() + serializeKeyText(key_serialization, keys_column, i);
            key_names.emplace(key, name);
            keys.push_back(std::move(key));
        }
    }
}

}

struct SerializationMapKeyColumns::SerializeState : public ISerialization::SerializeBinaryBulkState
{
    std::vector<Field> keys;
    std::map<Field, String> key_names;
    /// Reverse index of subcolumn name -> the key that owns it. Used to reject two
    /// distinct keys whose text encoding maps to the same subcolumn name, which
    /// would otherwise silently write both keys into the same physical streams.
    std::map<String, Field> name_owner;
    std::map<Field, SerializeBinaryBulkStatePtr> key_states;
    std::set<Field> copied_from_template;
    SerializeBinaryBulkStatePtr template_state;
    bool suffix_written = false;
};

struct SerializationMapKeyColumns::DeserializeState : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<Field> keys;
    std::map<Field, String> key_names;
    std::map<Field, DeserializeBinaryBulkStatePtr> key_states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<DeserializeState>(*this);
        for (auto & [key, key_state] : copy->key_states)
            key_state = key_state ? key_state->clone() : nullptr;
        return copy;
    }
};

struct DeserializeBinaryBulkStateMapKeyColumn : public ISerialization::DeserializeBinaryBulkState
{
    bool missing = false;
    ISerialization::DeserializeBinaryBulkStatePtr physical_state;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<DeserializeBinaryBulkStateMapKeyColumn>(*this);
        copy->physical_state = physical_state ? physical_state->clone() : nullptr;
        return copy;
    }
};

UInt128 SerializationMapKeyColumns::getHash(
    const SerializationPtr & text_serialization_,
    const SerializationPtr & physical_serialization_)
{
    SipHash hash;
    hash.update("MapPerKey");
    hash.update(text_serialization_->getHash());
    hash.update(physical_serialization_->getHash());
    return hash.get128();
}

SerializationMapKeyColumns::SerializationMapKeyColumns(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const DataTypePtr & physical_value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & text_serialization_,
    const SerializationPtr & physical_serialization_)
    : SerializationWrapper(text_serialization_)
    , key_type(key_type_)
    , value_type(value_type_)
    , physical_value_type(physical_value_type_)
    , key_serialization(key_serialization_)
    , physical_serialization(physical_serialization_)
{
}

SerializationPtr SerializationMapKeyColumns::create(
    const DataTypePtr & key_type_,
    const DataTypePtr & value_type_,
    const DataTypePtr & physical_value_type_,
    const SerializationPtr & key_serialization_,
    const SerializationPtr & text_serialization_,
    const SerializationPtr & physical_serialization_)
{
    return ISerialization::pooled(
        getHash(text_serialization_, physical_serialization_),
        [&]
        {
            return new SerializationMapKeyColumns(
                key_type_,
                value_type_,
                physical_value_type_,
                key_serialization_,
                text_serialization_,
                physical_serialization_);
        });
}

bool SerializationMapKeyColumns::physicalTypeContainsLowCardinality() const
{
    bool contains_low_cardinality = physical_value_type->lowCardinality();
    physical_value_type->forEachChild([&](const IDataType & child)
    {
        contains_low_cardinality |= child.lowCardinality();
    });
    return contains_low_cardinality;
}

String SerializationMapKeyColumns::getKeySubcolumnName(const Field & key) const
{
    return String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + serializeKeyText(*key_serialization, key_type, key);
}

String SerializationMapKeyColumns::getKeySubcolumnName(const IColumn & key_column, size_t row) const
{
    return String(DataTypeMap::KEY_SUBCOLUMN_PREFIX) + serializeKeyText(*key_serialization, key_column, row);
}

std::vector<Field> SerializationMapKeyColumns::collectNewKeys(const IColumn & column, const SerializeBinaryBulkState & state) const
{
    const auto & map_state = typeid_cast<const SerializeState &>(state);
    const auto & map = assert_cast<const ColumnMap &>(column);

    std::vector<Field> collected;
    std::map<Field, String> names;
    collectFirstSeenKeys(map, *key_serialization, collected, names);

    std::vector<Field> new_keys;
    new_keys.reserve(collected.size());
    for (auto & key : collected)
    {
        if (!map_state.key_names.contains(key))
            new_keys.push_back(std::move(key));
    }
    return new_keys;
}

void SerializationMapKeyColumns::addKeys(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const
{
    auto * map_state = typeid_cast<SerializeState *>(state.get());
    if (!map_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid serialize state for SerializationMapKeyColumns");

    for (const auto & key : keys)
    {
        if (map_state->key_names.contains(key))
            continue;
        String name = getKeySubcolumnName(key);
        auto [owner_it, inserted] = map_state->name_owner.try_emplace(name, key);
        if (!inserted)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Two distinct keys of a with_key_columns Map map to the same subcolumn name {}: their {} text "
                "serialization is not injective. Such key values cannot be stored per key.",
                name,
                key_type->getName());
        }
        map_state->key_names.emplace(key, name);
        map_state->keys.push_back(key);
        map_state->key_states.emplace(key, SerializeBinaryBulkStatePtr{});
    }
}

void SerializationMapKeyColumns::markKeysCopiedFromTemplate(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const
{
    auto * map_state = typeid_cast<SerializeState *>(state.get());
    if (!map_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid serialize state for SerializationMapKeyColumns");

    for (const auto & key : keys)
        map_state->copied_from_template.insert(key);
}

const std::vector<Field> & SerializationMapKeyColumns::getRegisteredKeys(const SerializeBinaryBulkState & state) const
{
    return typeid_cast<const SerializeState &>(state).keys;
}

size_t SerializationMapKeyColumns::getRegisteredKeyCount(const SerializeBinaryBulkState & state) const
{
    return typeid_cast<const SerializeState &>(state).keys.size();
}

void SerializationMapKeyColumns::enumeratePhysicalStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    Substream::Type prefix_type,
    const String & key_name,
    const DeserializeBinaryBulkStatePtr & physical_state) const
{
    settings.path.push_back(prefix_type);
    if (prefix_type == Substream::MapKey)
        settings.path.back().name_of_substream = key_name;

    auto next_data = SubstreamData(physical_serialization)
        .withType(physical_value_type)
        .withColumn(data.column ? physical_value_type->createColumn() : nullptr)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(physical_state);
    physical_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationMapKeyColumns::enumerateKeyStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data,
    const Field & key) const
{
    enumeratePhysicalStreams(settings, callback, data, Substream::MapKey, getKeySubcolumnName(key), nullptr);
}

void SerializationMapKeyColumns::enumerateTemplateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    enumeratePhysicalStreams(settings, callback, data, Substream::MapKeyTemplate, {}, nullptr);
}

void SerializationMapKeyColumns::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();

    const auto * deserialize_state = data.deserialize_state ? typeid_cast<const DeserializeState *>(data.deserialize_state.get()) : nullptr;
    if (deserialize_state)
    {
        for (const auto & key : deserialize_state->keys)
        {
            auto it = deserialize_state->key_states.find(key);
            enumeratePhysicalStreams(
                settings,
                callback,
                data,
                Substream::MapKey,
                deserialize_state->key_names.at(key),
                it != deserialize_state->key_states.end() ? it->second : nullptr);
        }
        return;
    }

    if (data.column)
    {
        const auto & map = assert_cast<const ColumnMap &>(*data.column);
        std::vector<Field> keys;
        std::map<Field, String> names;
        collectFirstSeenKeys(map, *key_serialization, keys, names);
        for (const auto & key : keys)
            enumeratePhysicalStreams(settings, callback, data, Substream::MapKey, names.at(key), nullptr);
    }
}

void SerializationMapKeyColumns::serializeBinaryBulkStatePrefix(
    const IColumn & /*column*/,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto map_state = std::make_shared<SerializeState>();

    settings.path.push_back(Substream::MapKeyTemplate);
    auto empty_nulls = physical_value_type->createColumn();
    physical_serialization->serializeBinaryBulkStatePrefix(*empty_nulls, settings, map_state->template_state);
    settings.path.pop_back();

    state = std::move(map_state);
}

void SerializationMapKeyColumns::writeManifest(WriteBuffer & out, const std::vector<Field> & keys) const
{
    std::map<Field, size_t> ordered;
    for (size_t i = 0; i < keys.size(); ++i)
        ordered.emplace(keys[i], i);

    std::vector<Field> ordered_keys;
    ordered_keys.reserve(ordered.size());
    for (const auto & [key, _] : ordered)
        ordered_keys.push_back(key);

    writeBinary(MANIFEST_VERSION, out);
    writeVarUInt(ordered_keys.size(), out);
    auto key_column = key_type->createColumn();
    for (const auto & key : ordered_keys)
        key_column->insert(key);
    for (size_t i = 0; i < key_column->size(); ++i)
        key_serialization->serializeBinary(*key_column, i, out, FormatSettings{});
}

std::vector<Field> SerializationMapKeyColumns::readManifest(ReadBuffer & in) const
{
    UInt8 version = 0;
    readBinary(version, in);
    if (version != MANIFEST_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown with_key_columns Map keys info version {}", static_cast<UInt32>(version));

    UInt64 key_count = 0;
    readVarUInt(key_count, in);
    auto key_column = key_type->createColumn();
    key_column->reserve(std::min(key_count, MANIFEST_KEY_COUNT_RESERVE_LIMIT));
    for (UInt64 i = 0; i < key_count; ++i)
        key_serialization->deserializeBinary(*key_column, in, FormatSettings{});

    std::vector<Field> keys;
    keys.reserve(key_column->size());
    for (size_t i = 0; i < key_column->size(); ++i)
    {
        Field key;
        key_column->get(i, key);
        keys.push_back(std::move(key));
    }
    return keys;
}

void SerializationMapKeyColumns::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);

    for (auto & [key, key_state] : map_state->key_states)
    {
        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = map_state->key_names.at(key);
        physical_serialization->serializeBinaryBulkStateSuffix(settings, key_state);
        settings.path.pop_back();
    }

    settings.path.push_back(Substream::MapKeyTemplate);
    physical_serialization->serializeBinaryBulkStateSuffix(settings, map_state->template_state);
    settings.path.pop_back();

    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys info");

    std::vector<Field> ordered_keys;
    ordered_keys.reserve(map_state->key_names.size());
    for (const auto & [key, name] : map_state->key_names)
        ordered_keys.push_back(key);

    writeManifest(*stream, ordered_keys);
    map_state->suffix_written = true;
}

void SerializationMapKeyColumns::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto map_state = std::make_shared<DeserializeState>();

    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
    {
        /// The column is absent from this part (e.g. added by ALTER after the part was
        /// written), so there is no keys-info manifest. Read it as an empty key set: the
        /// whole Map then materialises as empty Maps for every row, matching how other
        /// serializations treat columns missing from old parts. A null manifest stream
        /// means the column is not in this part's checksums; a manifest that is present
        /// but references a missing key data file is still rejected as corruption in
        /// MergeTreeReaderWide::addStreams.
        state = std::move(map_state);
        return;
    }

    UInt8 version = 0;
    readBinary(version, *stream);
    if (version != MANIFEST_VERSION)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Unknown with_key_columns Map keys info version {}", static_cast<UInt32>(version));

    UInt64 key_count = 0;
    readVarUInt(key_count, *stream);
    auto key_column = key_type->createColumn();
    key_column->reserve(std::min(key_count, MANIFEST_KEY_COUNT_RESERVE_LIMIT));
    for (UInt64 i = 0; i < key_count; ++i)
        key_serialization->deserializeBinary(*key_column, *stream, FormatSettings{});

    for (size_t i = 0; i < key_column->size(); ++i)
    {
        Field key;
        key_column->get(i, key);
        String name = getKeySubcolumnName(*key_column, i);
        map_state->key_names.emplace(key, name);
        map_state->keys.push_back(key);

        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = name;
        DeserializeBinaryBulkStatePtr key_state;
        physical_serialization->deserializeBinaryBulkStatePrefix(settings, key_state, cache);
        settings.path.pop_back();
        map_state->key_states.emplace(std::move(key), std::move(key_state));
    }

    state = std::move(map_state);
}

void SerializationMapKeyColumns::initializeKeyPrefixes(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);
    for (auto & [key, key_state] : map_state->key_states)
    {
        if (!key_state && !map_state->copied_from_template.contains(key))
        {
            settings.path.push_back(Substream::MapKey);
            settings.path.back().name_of_substream = map_state->key_names.at(key);
            auto empty_column = physical_value_type->createColumn();
            physical_serialization->serializeBinaryBulkStatePrefix(*empty_column, settings, key_state);
            settings.path.pop_back();
        }
    }
}

void SerializationMapKeyColumns::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);
    const auto & map = assert_cast<const ColumnMap &>(column);
    /// `limit == 0` follows the generic ISerialization contract ("write until the end of
    /// the column"). The MergeTree wide writer always drives the real per-granule writes
    /// with an explicit non-zero row count that matches the all-NULL template stream
    /// (writeTemplateNulls), while the substreams-enumeration path calls this with
    /// offset == size and limit == 0, i.e. zero rows. Both stay row-aligned.
    size_t end = limit && offset + limit < map.size() ? offset + limit : map.size();
    size_t rows = end - offset;

    std::vector<Field> keys;
    std::vector<MutableColumnPtr> value_columns;
    std::vector<IColumn *> value_ptrs;
    keys.reserve(map_state->key_states.size());
    value_columns.reserve(map_state->key_states.size());
    value_ptrs.reserve(map_state->key_states.size());
    for (const auto & [key, _] : map_state->key_states)
    {
        keys.push_back(key);
        value_columns.emplace_back(physical_value_type->createColumn());
        value_ptrs.push_back(value_columns.back().get());
    }

    extractRegisteredKeyValuesFromMap(map.getNestedColumn(), keys, value_ptrs, offset, end);

    size_t key_index = 0;
    for (auto & [key, key_state] : map_state->key_states)
    {
        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = map_state->key_names.at(key);
        physical_serialization->serializeBinaryBulkWithMultipleStreams(*value_columns[key_index], 0, rows, settings, key_state);
        settings.path.pop_back();
        ++key_index;
    }
}

void SerializationMapKeyColumns::writeTemplateNulls(
    size_t rows,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    auto * map_state = checkAndGetState<SerializeState>(state);
    auto nulls = physical_value_type->createColumn();
    nulls->insertManyDefaults(rows);

    settings.path.push_back(Substream::MapKeyTemplate);
    physical_serialization->serializeBinaryBulkWithMultipleStreams(*nulls, 0, rows, settings, map_state->template_state);
    settings.path.pop_back();
}

void SerializationMapKeyColumns::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * map_state = checkAndGetState<DeserializeState>(state);
    auto mutable_column = column->assumeMutable();
    auto & map = assert_cast<ColumnMap &>(*mutable_column);

    std::vector<ColumnPtr> value_columns;
    value_columns.reserve(map_state->keys.size());
    size_t rows = 0;

    for (const auto & key : map_state->keys)
    {
        ColumnPtr values = physical_value_type->createColumn();
        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = map_state->key_names.at(key);
        physical_serialization->deserializeBinaryBulkWithMultipleStreams(
            values, rows_offset, limit, settings, map_state->key_states.at(key), cache);
        settings.path.pop_back();
        rows = values->size();
        value_columns.emplace_back(std::move(values));
    }

    if (map_state->keys.empty())
    {
        mutable_column->insertManyDefaults(limit);
        column = std::move(mutable_column);
        return;
    }

    auto & nested_array = map.getNestedColumn();
    auto & tuple = map.getNestedData();
    auto & keys_column = tuple.getColumn(0);
    auto & values_column = tuple.getColumn(1);
    auto & offsets = nested_array.getOffsets();

    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t i = 0; i < map_state->keys.size(); ++i)
        {
            if (value_columns[i]->isNullAt(row))
                continue;
            keys_column.insert(map_state->keys[i]);
            if (const auto * nullable = typeid_cast<const ColumnNullable *>(value_columns[i].get()))
                values_column.insertFrom(nullable->getNestedColumn(), row);
            else
                values_column.insertFrom(*value_columns[i], row);
        }
        offsets.push_back(keys_column.size());
    }
    column = std::move(mutable_column);
}

SerializationMapKeyColumn::SerializationMapKeyColumn(
    const SerializationPtr & physical_serialization_,
    const SerializationPtr & map_serialization_,
    ColumnPtr key_,
    String key_subcolumn_name_,
    std::vector<Field> manifest_keys_,
    bool enumerate_keys_info_)
    : SerializationWrapper(physical_serialization_)
    , map_serialization(map_serialization_)
    , key(std::move(key_))
    , key_subcolumn_name(std::move(key_subcolumn_name_))
    , manifest_keys(std::move(manifest_keys_))
    , enumerate_keys_info(enumerate_keys_info_)
{
}

SerializationPtr SerializationMapKeyColumn::create(
    const SerializationPtr & physical_serialization_,
    const SerializationPtr & map_serialization_,
    ColumnPtr key_,
    const String & key_subcolumn_name_,
    std::vector<Field> manifest_keys_,
    bool enumerate_keys_info_)
{
    return std::shared_ptr<ISerialization>(new SerializationMapKeyColumn(
        physical_serialization_,
        map_serialization_,
        std::move(key_),
        key_subcolumn_name_,
        std::move(manifest_keys_),
        enumerate_keys_info_));
}

void SerializationMapKeyColumn::enumerateStreams(
    EnumerateStreamsSettings & settings,
    const StreamCallback & callback,
    const SubstreamData & data) const
{
    if (enumerate_keys_info)
    {
        settings.path.push_back(Substream::MapKeysInfo);
        callback(settings.path);
        settings.path.pop_back();
    }

    /// After prefix, a missing key has no data files. Do not list them: prefetch
    /// would try to open streams that were never written.
    const auto * value_state = data.deserialize_state
        ? typeid_cast<const DeserializeBinaryBulkStateMapKeyColumn *>(data.deserialize_state.get())
        : nullptr;
    if (value_state && value_state->missing)
        return;

    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*map_serialization);
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    auto next_data = SubstreamData(nested_serialization)
        .withType(per_key.getPhysicalValueType())
        .withColumn(data.column)
        .withSerializationInfo(data.serialization_info)
        .withDeserializeState(value_state ? value_state->physical_state : data.deserialize_state);
    nested_serialization->enumerateStreams(settings, callback, next_data);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsDeserializeStatesCache * cache) const
{
    auto value_state = std::make_shared<DeserializeBinaryBulkStateMapKeyColumn>();
    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*map_serialization);
    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
    {
        /// Column absent from this part (added by ALTER after the part was written):
        /// no manifest, so the key does not exist here. Read it as all-NULL, the same
        /// as a key that is not present in an existing manifest.
        value_state->missing = true;
        state = std::move(value_state);
        return;
    }
    const auto keys = per_key.readManifest(*stream);

    Field requested;
    key->get(0, requested);
    if (std::ranges::find(keys, requested) == keys.end())
    {
        value_state->missing = true;
        state = std::move(value_state);
        return;
    }

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    nested_serialization->deserializeBinaryBulkStatePrefix(settings, value_state->physical_state, cache);
    settings.path.pop_back();
    state = std::move(value_state);
}

void SerializationMapKeyColumn::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * value_state = checkAndGetState<DeserializeBinaryBulkStateMapKeyColumn>(state);
    if (value_state->missing)
    {
        auto mutable_column = column->assumeMutable();
        mutable_column->insertManyDefaults(limit);
        column = std::move(mutable_column);
        return;
    }

    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*map_serialization);
    const auto & physical_type = per_key.getPhysicalValueType();
    if ((physical_type->isNullable() && !column->isNullable())
        || (physical_type->lowCardinality() && !column->lowCardinality()))
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Per-key Map key subcolumn must be deserialized into {}, got {}",
            physical_type->getName(),
            column->getName());
    }

    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    nested_serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_offset, limit, settings, value_state->physical_state, cache);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkStatePrefix(
    const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    nested_serialization->serializeBinaryBulkStatePrefix(column, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkWithMultipleStreams(
    const IColumn & column,
    size_t offset,
    size_t limit,
    SerializeBinaryBulkSettings & settings,
    SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    nested_serialization->serializeBinaryBulkWithMultipleStreams(column, offset, limit, settings, state);
    settings.path.pop_back();
}

void SerializationMapKeyColumn::serializeBinaryBulkStateSuffix(
    SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    settings.path.push_back(Substream::MapKey);
    settings.path.back().name_of_substream = key_subcolumn_name;
    nested_serialization->serializeBinaryBulkStateSuffix(settings, state);
    settings.path.pop_back();

    if (manifest_keys.empty())
        return;

    const auto & per_key = assert_cast<const SerializationMapKeyColumns &>(*map_serialization);
    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing stream for Map keys info");
    per_key.writeManifest(*stream, manifest_keys);
}

namespace
{
const SerializationMapKeyColumns & requireMapKeyColumns(const SerializationPtr & serialization)
{
    const auto * map = typeid_cast<const SerializationMapKeyColumns *>(serialization.get());
    if (!map)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Existence subcolumns require with_key_columns Map serialization");
    return *map;
}

struct MapPresenceState : public ISerialization::DeserializeBinaryBulkState
{
    std::vector<Field> keys;
    std::vector<ISerialization::DeserializeBinaryBulkStatePtr> states;

    ISerialization::DeserializeBinaryBulkStatePtr clone() const override
    {
        auto copy = std::make_shared<MapPresenceState>(*this);
        for (auto & state : copy->states)
            state = state ? state->clone() : nullptr;
        return copy;
    }
};
}

SerializationMapKeyPresence::SerializationMapKeyPresence(SerializationPtr map_serialization_, std::optional<Field> key_)
    : SerializationWrapper(map_serialization_)
    , map_serialization(std::move(map_serialization_))
    , requested_key(std::move(key_))
{
}

void SerializationMapKeyPresence::enumerateStreams(
    EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    settings.path.push_back(Substream::MapKeysInfo);
    callback(settings.path);
    settings.path.pop_back();
    const auto & map = requireMapKeyColumns(map_serialization);
    const auto * state = data.deserialize_state ? typeid_cast<const MapPresenceState *>(data.deserialize_state.get()) : nullptr;
    std::vector<Field> keys;
    if (state)
        keys = state->keys;
    else if (requested_key)
        keys.push_back(*requested_key);
    for (const auto & key : keys)
    {
        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = map.getKeySubcolumnName(key);
        if (map.getPhysicalValueType()->lowCardinality())
        {
            if (!state)
            {
                settings.path.push_back(settings.use_specialized_prefixes_and_suffixes_substreams ? Substream::DictionaryKeysPrefix : Substream::DictionaryKeys);
                callback(settings.path);
                settings.path.pop_back();
            }
            settings.path.push_back(Substream::DictionaryIndexes);
            callback(settings.path);
            settings.path.pop_back();
        }
        else
        {
            settings.path.push_back(Substream::NullMap);
            callback(settings.path);
            settings.path.pop_back();
        }
        settings.path.pop_back();
    }
}

void SerializationMapKeyPresence::deserializeBinaryBulkStatePrefix(
    DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsDeserializeStatesCache *) const
{
    const auto & map = requireMapKeyColumns(map_serialization);
    settings.path.push_back(Substream::MapKeysInfo);
    auto * stream = settings.getter(settings.path);
    settings.path.pop_back();
    if (!stream)
    {
        /// Column absent from this part (added by ALTER after the part was written):
        /// no manifest, so no key is present. Read presence as empty, i.e. the key is
        /// absent for every row.
        state = std::make_shared<MapPresenceState>();
        return;
    }
    auto keys = map.readManifest(*stream);
    auto result = std::make_shared<MapPresenceState>();
    for (auto & key : keys)
    {
        if (requested_key && *requested_key != key)
            continue;
        result->keys.push_back(key);
        result->states.emplace_back();
        if (map.getPhysicalValueType()->lowCardinality())
        {
            settings.path.push_back(Substream::MapKey);
            settings.path.back().name_of_substream = map.getKeySubcolumnName(key);
            assert_cast<const SerializationLowCardinality &>(*map.getPhysicalSerialization())
                .deserializeNullMapStatePrefix(settings, result->states.back());
            settings.path.pop_back();
        }
    }
    state = std::move(result);
}

void SerializationMapKeyPresence::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr & state,
    SubstreamsCache * cache) const
{
    auto * presence = checkAndGetState<MapPresenceState>(state);
    const auto & map = requireMapKeyColumns(map_serialization);
    auto mutable_column = column->assumeMutable();
    if (presence->keys.empty())
    {
        mutable_column->insertManyDefaults(limit);
        column = std::move(mutable_column);
        return;
    }
    std::vector<ColumnPtr> nulls;
    size_t rows = 0;
    for (size_t i = 0; i < presence->keys.size(); ++i)
    {
        ColumnPtr null_map_ptr = ColumnUInt8::create();
        settings.path.push_back(Substream::MapKey);
        settings.path.back().name_of_substream = map.getKeySubcolumnName(presence->keys[i]);
        settings.path.push_back(Substream::NullMap);
        /// Copy only this range from the shared `NullMap` cache. Adopting the
        /// whole cached column would replay earlier granules into `exists_*`.
        bool cached = false;
        if (auto cached_column_with_num_read_rows = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
        {
            const auto & [cached_column, count] = *cached_column_with_num_read_rows;
            if (cached_column->size() < count)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cached null map is smaller than the number of rows read for this range");
            auto null_map = ColumnUInt8::create();
            null_map->insertRangeFrom(*cached_column, cached_column->size() - count, count);
            null_map_ptr = std::move(null_map);
            cached = true;
        }
        settings.path.pop_back();
        if (!cached)
        {
            auto null_map = IColumn::mutate(std::move(null_map_ptr));
            if (map.getPhysicalValueType()->lowCardinality())
            {
                if (auto values = getColumnWithNumReadRowsFromSubstreamsCache(cache, settings.path))
                {
                    const auto & [cached_column, count] = *values;
                    auto & null_data = assert_cast<ColumnUInt8 &>(*null_map).getData();
                    for (size_t row = cached_column->size() - count; row < cached_column->size(); ++row)
                        null_data.push_back(cached_column->isNullAt(row));
                }
                else
                {
                    const auto & lc = assert_cast<const SerializationLowCardinality &>(*map.getPhysicalSerialization());
                    if (rows_offset)
                    {
                        auto skipped = ColumnUInt8::create();
                        lc.deserializeNullMap(*skipped, rows_offset, settings, presence->states[i]);
                    }
                    lc.deserializeNullMap(*null_map, limit, settings, presence->states[i]);
                }
            }
            else
            {
                settings.path.push_back(Substream::NullMap);
                auto * stream = settings.getter(settings.path);
                settings.path.pop_back();
                if (!stream)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Missing null map for with_key_columns Map key {}", presence->keys[i]);
                SerializationNumber<UInt8>::create()->deserializeBinaryBulk(*null_map, *stream, rows_offset, limit, 0);
            }
            settings.path.push_back(Substream::NullMap);
            addColumnWithNumReadRowsToSubstreamsCache(cache, settings.path, null_map->getPtr(), null_map->size());
            settings.path.pop_back();
            null_map_ptr = std::move(null_map);
        }
        settings.path.pop_back();
        if (i && rows != null_map_ptr->size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Inconsistent row counts in with_key_columns Map null maps");
        rows = null_map_ptr->size();
        nulls.push_back(std::move(null_map_ptr));
    }
    if (requested_key)
    {
        auto & result = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        for (UInt8 is_null : assert_cast<const ColumnUInt8 &>(*nulls.front()).getData())
            result.push_back(!is_null);
        column = std::move(mutable_column);
        return;
    }
    auto & array = assert_cast<ColumnArray &>(*mutable_column);
    auto & keys = array.getData();
    for (size_t row = 0; row < rows; ++row)
    {
        for (size_t i = 0; i < presence->keys.size(); ++i)
            if (!assert_cast<const ColumnUInt8 &>(*nulls[i]).getData()[row])
                keys.insert(presence->keys[i]);
        array.getOffsets().push_back(keys.size());
    }
    column = std::move(mutable_column);
}

}
