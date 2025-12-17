#pragma once
#include <DataTypes/Serializations/SimpleTextSerialization.h>
#include <AggregateFunctions/IAggregateFunction.h>

namespace DB
{

class SerializationPostingList final : public SimpleTextSerialization
{
    AggregateFunctionPtr function;
public:
    SerializationPostingList(const AggregateFunctionPtr & function_);

    void serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const override;
    void deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const override;

    void serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override;
    void deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const override;
 
    void serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const override;
    void deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const override;

    void serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const override;
    void deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, size_t, double) const override;

    void enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const override;

    void serializeBinaryBulkStatePrefix(const IColumn & column, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;
    void serializeBinaryBulkWithMultipleStreams(const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const override;

    void deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings &, DeserializeBinaryBulkStatePtr &, SubstreamsDeserializeStatesCache *) const override;
    void deserializeBinaryBulkWithMultipleStreams(ColumnPtr & column, size_t rows_offset, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsCache * cache) const override;
};

}
