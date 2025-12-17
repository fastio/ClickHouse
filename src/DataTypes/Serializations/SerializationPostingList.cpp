#include <DataTypes/Serializations/SerializationPostingList.h>
#include <AggregateFunctions/AggregateFunctionPostingListData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}


void SerializationPostingList::enumerateStreams(EnumerateStreamsSettings & settings, const StreamCallback & callback, const SubstreamData & data) const
{
    (void) settings;
    (void) callback;
    (void) data;
}

struct SerializePostingListState : public ISerialization::SerializeBinaryBulkState
{
};

struct DeserializePostingListState : public ISerialization::DeserializeBinaryBulkState
{
};

void SerializationPostingList::serializeBinaryBulkStatePrefix(const IColumn & /* column */, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    (void) settings;
    state = std::make_shared<SerializePostingListState>();
}

void SerializationPostingList::serializeBinaryBulkWithMultipleStreams(const IColumn & column, size_t offset, size_t limit, SerializeBinaryBulkSettings & settings, SerializeBinaryBulkStatePtr & state) const
{
    (void) column;
    (void) offset;
    (void) limit;
    (void) settings;
    (void) state;
}

void SerializationPostingList::deserializeBinaryBulkStatePrefix(DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state,  SubstreamsDeserializeStatesCache *) const
{
    (void) settings;
    (void) state;
    state = std::make_shared<DeserializePostingListState>();
}

void SerializationPostingList::deserializeBinaryBulkWithMultipleStreams(ColumnPtr & column, size_t offset, size_t limit, DeserializeBinaryBulkSettings & settings, DeserializeBinaryBulkStatePtr & state, SubstreamsCache * cache) const
{
    (void) column;
    (void) offset;
    (void) limit;
    (void) settings;
    (void) state;
    (void) cache;
}

SerializationPostingList::SerializationPostingList(const AggregateFunctionPtr & function_)
    : function(function_)
{
}

[[noreturn]] static void throwNoSerialization()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Serialization is not implemented for type PostingList");
}

void SerializationPostingList::serializeBinary(const Field &, WriteBuffer &, const FormatSettings &) const { throwNoSerialization(); }
void SerializationPostingList::deserializeBinary(Field &, ReadBuffer &, const FormatSettings &) const { throwNoSerialization(); }
void SerializationPostingList::serializeBinary(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const { throwNoSerialization(); }
void SerializationPostingList::deserializeBinary(IColumn &, ReadBuffer &, const FormatSettings &) const { throwNoSerialization(); }
void SerializationPostingList::serializeText(const IColumn &, size_t, WriteBuffer &, const FormatSettings &) const { throwNoSerialization(); }
void SerializationPostingList::deserializeText(IColumn &, ReadBuffer &, const FormatSettings &, bool) const { throwNoSerialization(); }
void SerializationPostingList::serializeBinaryBulk(const IColumn &, WriteBuffer &, size_t, size_t) const { throwNoSerialization(); }
void SerializationPostingList::deserializeBinaryBulk(IColumn &, ReadBuffer &, size_t, size_t, double) const { throwNoSerialization(); }

}
