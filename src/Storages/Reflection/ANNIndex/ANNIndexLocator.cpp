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
#include <IO/MMappedFile.h>
#include <IO/MMappedFileCache.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <cstring>
#include <filesystem>


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

OffsetEntry OffsetReader::at(UInt64 internal_id) const
{
    const char * base = mapping ? mapping->getData() : buffer.data();
    /// 12-byte on-disk stride, not sizeof(OffsetEntry) (== 16 with padding).
    const char * record = base + internal_id * OFFSET_RECORD_SIZE;
    OffsetEntry entry;
    memcpy(&entry.part_id, record, sizeof(UInt32));
    memcpy(&entry.part_offset, record + sizeof(UInt32), sizeof(UInt64));
    return entry;
}

OffsetReader openOffsets(
    const IDataPartStorage & storage,
    MMappedFileCache * mmap_cache,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(OFFSET_FILE_NAME);
    const UInt64 file_size = storage.getFileSize(file_name);
    checkBinaryFileSize(file_name, file_size, OFFSET_RECORD_SIZE);

    OffsetReader reader;
    reader.count = file_size / OFFSET_RECORD_SIZE;

    /// Local disk: share one mmap across queries via the cache and let the OS page
    /// in only the touched records. Remote disk has no usable local path to mmap, so
    /// read the whole sidecar once into a byte buffer (remote ANN parts are out of
    /// scope for this optimization).
    if (mmap_cache && file_size > 0 && !storage.isStoredOnRemoteDisk())
    {
        const String path = std::filesystem::path(storage.getFullPath()) / file_name;
        reader.mapping = mmap_cache->getOrSet(MMappedFileCache::hash(path, 0, -1), [&]
        {
            return std::make_shared<MMappedFile>(path, 0);
        });
        return reader;
    }

    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    readStringUntilEOF(reader.buffer, *in);
    return reader;
}

}

}
