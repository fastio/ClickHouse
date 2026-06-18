#include <Storages/Reflection/ANNIndex/ANNIndexLocator.h>

#include <Common/Exception.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace ANNIndexLocator
{
namespace
{

constexpr UInt64 STABLE_ID_RECORD_SIZE = sizeof(UInt64) * 2;
constexpr UInt64 OFFSET_RECORD_SIZE = sizeof(UInt32) + sizeof(UInt64);

String toFileName(std::string_view file_name)
{
    return String{file_name.data(), file_name.size()};
}

void checkBinaryFileSize(const String & file_name, UInt64 bytes, UInt64 record_size)
{
    if (bytes % record_size != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN locator file {} has size {}, which is not a multiple of record size {}",
            file_name, bytes, record_size);
}

UInt64 checkedOffset(UInt32 internal_id, UInt64 record_size)
{
    return static_cast<UInt64>(internal_id) * record_size;
}

}

void writeStableIdsToBuffer(const std::vector<StableId> & ids, WriteBuffer & out)
{
    for (const auto & id : ids)
    {
        writeBinary(id.block_number, out);
        writeBinary(id.block_offset, out);
    }
}

std::vector<StableId> readStableIdsFromBuffer(ReadBuffer & in, UInt64 bytes)
{
    checkBinaryFileSize(toFileName(STABLE_ID_FILE_NAME), bytes, STABLE_ID_RECORD_SIZE);

    std::vector<StableId> ids;
    ids.reserve(bytes / STABLE_ID_RECORD_SIZE);
    for (UInt64 i = 0; i < bytes / STABLE_ID_RECORD_SIZE; ++i)
    {
        StableId id;
        readBinary(id.block_number, in);
        readBinary(id.block_offset, in);
        ids.push_back(id);
    }
    return ids;
}

void writeStableIds(
    IDataPartStorage & storage,
    const std::vector<StableId> & ids,
    const WriteSettings & write_settings)
{
    auto out = storage.writeFile(toFileName(STABLE_ID_FILE_NAME), 4096, write_settings);
    writeStableIdsToBuffer(ids, *out);
    out->finalize();
}

std::vector<StableId> readStableIds(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(STABLE_ID_FILE_NAME);
    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    return readStableIdsFromBuffer(*in, storage.getFileSize(file_name));
}

StableId readStableIdAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(STABLE_ID_FILE_NAME);
    const UInt64 file_size = storage.getFileSize(file_name);
    const UInt64 offset = checkedOffset(internal_id, STABLE_ID_RECORD_SIZE);
    if (offset + STABLE_ID_RECORD_SIZE > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN locator stable id {} is out of range for {} bytes",
            internal_id, file_size);

    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    in->seek(offset, SEEK_SET);

    StableId id;
    readBinary(id.block_number, *in);
    readBinary(id.block_offset, *in);
    return id;
}

void writeOffsetsToBuffer(const std::vector<OffsetEntry> & offsets, WriteBuffer & out)
{
    for (const auto & offset : offsets)
    {
        writeBinary(offset.part_id, out);
        writeBinary(offset.part_offset, out);
    }
}

std::vector<OffsetEntry> readOffsetsFromBuffer(ReadBuffer & in, UInt64 bytes)
{
    checkBinaryFileSize(toFileName(OFFSET_FILE_NAME), bytes, OFFSET_RECORD_SIZE);

    std::vector<OffsetEntry> offsets;
    offsets.reserve(bytes / OFFSET_RECORD_SIZE);
    for (UInt64 i = 0; i < bytes / OFFSET_RECORD_SIZE; ++i)
    {
        OffsetEntry offset;
        readBinary(offset.part_id, in);
        readBinary(offset.part_offset, in);
        offsets.push_back(offset);
    }
    return offsets;
}

void writeOffsets(
    IDataPartStorage & storage,
    const std::vector<OffsetEntry> & offsets,
    const WriteSettings & write_settings)
{
    auto out = storage.writeFile(toFileName(OFFSET_FILE_NAME), 4096, write_settings);
    writeOffsetsToBuffer(offsets, *out);
    out->finalize();
}

std::vector<OffsetEntry> readOffsets(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(OFFSET_FILE_NAME);
    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    return readOffsetsFromBuffer(*in, storage.getFileSize(file_name));
}

OffsetEntry readOffsetAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(OFFSET_FILE_NAME);
    const UInt64 file_size = storage.getFileSize(file_name);
    const UInt64 file_offset = checkedOffset(internal_id, OFFSET_RECORD_SIZE);
    if (file_offset + OFFSET_RECORD_SIZE > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN locator offset {} is out of range for {} bytes",
            internal_id, file_size);

    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    in->seek(file_offset, SEEK_SET);

    OffsetEntry offset;
    readBinary(offset.part_id, *in);
    readBinary(offset.part_offset, *in);
    return offset;
}

}

}
