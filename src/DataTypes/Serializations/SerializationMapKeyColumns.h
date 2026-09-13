#pragma once

#include <DataTypes/Serializations/SerializationWrapper.h>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <map>
#include <vector>

namespace DB
{

class SerializationMapKeyColumn;

/// Disk serialization for `map_serialization_version = 'with_key_columns'`.
/// Each distinct key is a row-aligned `Nullable(V)` (or `LowCardinality(Nullable(T))`)
/// subcolumn. A per-column manifest lists the keys present in the part.
class SerializationMapKeyColumns final : public SerializationWrapper
{
public:
    static constexpr UInt8 MANIFEST_VERSION = 1;

    static UInt128 getHash(
        const SerializationPtr & text_serialization_,
        const SerializationPtr & physical_serialization_);

    static SerializationPtr create(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const DataTypePtr & physical_value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & text_serialization_,
        const SerializationPtr & physical_serialization_);

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    const DataTypePtr & getPhysicalValueType() const { return physical_value_type; }
    const SerializationPtr & getKeySerialization() const { return key_serialization; }
    const SerializationPtr & getPhysicalSerialization() const { return physical_serialization; }
    bool physicalTypeContainsLowCardinality() const;

    String getKeySubcolumnName(const Field & key) const;
    String getKeySubcolumnName(const IColumn & key_column, size_t row) const;

    /// Keys from `column` that are not yet registered in `state`, first-seen order.
    std::vector<Field> collectNewKeys(const IColumn & column, const SerializeBinaryBulkState & state) const;
    void addKeys(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const;
    void markKeysCopiedFromTemplate(SerializeBinaryBulkStatePtr & state, const std::vector<Field> & keys) const;
    const std::vector<Field> & getRegisteredKeys(const SerializeBinaryBulkState & state) const;
    size_t getRegisteredKeyCount(const SerializeBinaryBulkState & state) const;

    /// Initialize newly registered key streams before the writer records their first marks.
    void initializeKeyPrefixes(SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const;

    void enumerateKeyStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        const Field & key) const;

    void enumerateTemplateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const;

    void writeTemplateNulls(
        size_t rows,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const;

    std::vector<Field> readManifest(ReadBuffer & in) const;
    void writeManifest(WriteBuffer & out, const std::vector<Field> & keys) const;

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    struct SerializeState;
    struct DeserializeState;

private:
    SerializationMapKeyColumns(
        const DataTypePtr & key_type_,
        const DataTypePtr & value_type_,
        const DataTypePtr & physical_value_type_,
        const SerializationPtr & key_serialization_,
        const SerializationPtr & text_serialization_,
        const SerializationPtr & physical_serialization_);

    void enumeratePhysicalStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data,
        Substream::Type prefix_type,
        const String & key_name,
        const DeserializeBinaryBulkStatePtr & physical_state) const;

    DataTypePtr key_type;
    DataTypePtr value_type;
    DataTypePtr physical_value_type;
    SerializationPtr key_serialization;
    SerializationPtr physical_serialization;
};

/// Reads one physical key subcolumn. Missing keys produce `NULL`s and do not open data files.
class SerializationMapKeyColumn final : public SerializationWrapper
{
public:
    static SerializationPtr create(
        const SerializationPtr & physical_serialization_,
        const SerializationPtr & map_serialization_,
        ColumnPtr key_,
        const String & key_subcolumn_name_,
        std::vector<Field> manifest_keys_ = {},
        bool enumerate_keys_info_ = true);

    void enumerateStreams(
        EnumerateStreamsSettings & settings,
        const StreamCallback & callback,
        const SubstreamData & data) const override;

    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsDeserializeStatesCache * cache) const override;

    void deserializeBinaryBulkWithMultipleStreams(
        IColumn & column,
        size_t limit,
        DeserializeBinaryBulkSettings & settings,
        DeserializeBinaryBulkStatePtr & state,
        SubstreamsCache * cache) const override;

    void serializeBinaryBulkStatePrefix(
        const IColumn & column,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkWithMultipleStreams(
        const IColumn & column,
        size_t offset,
        size_t limit,
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

    void serializeBinaryBulkStateSuffix(
        SerializeBinaryBulkSettings & settings,
        SerializeBinaryBulkStatePtr & state) const override;

private:
    SerializationMapKeyColumn(
        const SerializationPtr & physical_serialization_,
        const SerializationPtr & map_serialization_,
        ColumnPtr key_,
        String key_subcolumn_name_,
        std::vector<Field> manifest_keys_,
        bool enumerate_keys_info_);

    SerializationPtr map_serialization;
    ColumnPtr key;
    String key_subcolumn_name;
    std::vector<Field> manifest_keys;
    bool enumerate_keys_info = true;
};

/// Reads a key's existence (`UInt8`) or all present keys (`Array(K)`) without reconstructing values.
class SerializationMapKeyPresence final : public SerializationWrapper
{
public:
    SerializationMapKeyPresence(SerializationPtr map_serialization_, std::optional<Field> key_);

    void enumerateStreams(EnumerateStreamsSettings &, const StreamCallback &, const SubstreamData &) const override;
    void deserializeBinaryBulkStatePrefix(
        DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsDeserializeStatesCache *) const override;
    void deserializeBinaryBulkWithMultipleStreams(
        IColumn &, size_t, DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsCache *) const override;

private:
    SerializationPtr map_serialization;
    std::optional<Field> requested_key;
};

}
