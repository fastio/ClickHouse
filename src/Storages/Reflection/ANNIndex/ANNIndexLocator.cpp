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

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <algorithm>
#include <iterator>
#include <sstream>


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
constexpr UInt64 OFFSET_RECORD_SIZE = sizeof(UInt64);

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

void writeStringFile(
    IDataPartStorage & storage,
    std::string_view file_name,
    const String & body,
    const WriteSettings & write_settings)
{
    auto out = storage.writeFile(toFileName(file_name), 4096, write_settings);
    out->write(body.data(), body.size());
    out->finalize();
}

String readStringFile(
    const IDataPartStorage & storage,
    std::string_view file_name,
    const ReadSettings & read_settings)
{
    auto in = storage.readFile(toFileName(file_name), read_settings, std::nullopt);
    String body;
    readStringUntilEOF(body, *in);
    return body;
}

}

GraphRangeMap::GraphRangeMap(const std::vector<LocatorRangeSegment> & segments_)
    : segments(segments_)
{
    validateRangeSegments(segments);
    starts.reserve(segments.size());
    for (const auto & segment : segments)
        starts.push_back(segment.start_internal_id);
}

const LocatorRangeSegment & GraphRangeMap::lookup(UInt32 internal_id) const
{
    if (segments.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot lookup ANN locator range in an empty map");
    if (internal_id >= segments.back().end_internal_id)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN locator internal id {} is out of range [0, {})",
            internal_id, segments.back().end_internal_id);

    auto it = std::upper_bound(starts.begin(), starts.end(), internal_id);
    const size_t idx = static_cast<size_t>(std::distance(starts.begin(), it) - 1);
    return segments[idx];
}

void validateRangeSegments(const std::vector<LocatorRangeSegment> & segments)
{
    UInt32 expected_start = 0;
    for (const auto & segment : segments)
    {
        if (segment.start_internal_id != expected_start)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ANN locator range starts at {}, expected {}",
                segment.start_internal_id, expected_start);
        if (segment.end_internal_id <= segment.start_internal_id)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "ANN locator range [{}, {}) is empty or reversed",
                segment.start_internal_id, segment.end_internal_id);

        expected_start = segment.end_internal_id;
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

void writeOffsetsToBuffer(const std::vector<UInt64> & offsets, WriteBuffer & out)
{
    for (UInt64 offset : offsets)
        writeBinary(offset, out);
}

std::vector<UInt64> readOffsetsFromBuffer(ReadBuffer & in, UInt64 bytes)
{
    checkBinaryFileSize(toFileName(OFFSET_FILE_NAME), bytes, OFFSET_RECORD_SIZE);

    std::vector<UInt64> offsets;
    offsets.reserve(bytes / OFFSET_RECORD_SIZE);
    for (UInt64 i = 0; i < bytes / OFFSET_RECORD_SIZE; ++i)
    {
        UInt64 offset = 0;
        readBinary(offset, in);
        offsets.push_back(offset);
    }
    return offsets;
}

void writeOffsets(
    IDataPartStorage & storage,
    const std::vector<UInt64> & offsets,
    const WriteSettings & write_settings)
{
    auto out = storage.writeFile(toFileName(OFFSET_FILE_NAME), 4096, write_settings);
    writeOffsetsToBuffer(offsets, *out);
    out->finalize();
}

std::vector<UInt64> readOffsets(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(OFFSET_FILE_NAME);
    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    return readOffsetsFromBuffer(*in, storage.getFileSize(file_name));
}

UInt64 readOffsetAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings)
{
    const String file_name = toFileName(OFFSET_FILE_NAME);
    const UInt64 file_size = storage.getFileSize(file_name);
    const UInt64 offset = checkedOffset(internal_id, OFFSET_RECORD_SIZE);
    if (offset + OFFSET_RECORD_SIZE > file_size)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "ANN locator offset {} is out of range for {} bytes",
            internal_id, file_size);

    auto in = storage.readFile(file_name, read_settings, std::nullopt);
    in->seek(offset, SEEK_SET);

    UInt64 part_offset = 0;
    readBinary(part_offset, *in);
    return part_offset;
}

String serializeRangeSegments(const std::vector<LocatorRangeSegment> & segments)
{
    validateRangeSegments(segments);

    Poco::JSON::Object root;
    root.set("locator_format_version", static_cast<Int64>(LOCATOR_FORMAT_VERSION));

    Poco::JSON::Array ranges;
    for (const auto & segment : segments)
    {
        Poco::JSON::Object item;
        item.set("start_internal_id", static_cast<Int64>(segment.start_internal_id));
        item.set("end_internal_id", static_cast<Int64>(segment.end_internal_id));
        item.set("target_part_uuid", toString(segment.target_part_uuid));
        ranges.add(item);
    }
    root.set("ranges", ranges);

    std::ostringstream oss;
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

std::vector<LocatorRangeSegment> parseRangeSegments(const String & body)
{
    Poco::JSON::Parser parser;
    auto parsed = parser.parse(body);
    auto root = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!root)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ANN locator ranges JSON root is not an object");

    const auto version = root->getValue<UInt64>("locator_format_version");
    if (version != LOCATOR_FORMAT_VERSION)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Unsupported ANN locator format version {}, expected {}",
            version, static_cast<UInt64>(LOCATOR_FORMAT_VERSION));

    auto ranges = root->getArray("ranges");
    if (!ranges)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ANN locator ranges JSON misses ranges array");

    std::vector<LocatorRangeSegment> segments;
    segments.reserve(ranges->size());
    for (size_t i = 0; i < ranges->size(); ++i)
    {
        auto item = ranges->getObject(static_cast<unsigned int>(i));
        if (!item)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ANN locator range {} is not an object", i);

        LocatorRangeSegment segment;
        segment.start_internal_id = item->getValue<UInt32>("start_internal_id");
        segment.end_internal_id = item->getValue<UInt32>("end_internal_id");
        if (!tryParse(segment.target_part_uuid, item->getValue<std::string>("target_part_uuid")))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ANN locator range {} has invalid target_part_uuid", i);
        segments.push_back(segment);
    }

    validateRangeSegments(segments);
    return segments;
}

void writeRangeSegments(
    IDataPartStorage & storage,
    const std::vector<LocatorRangeSegment> & segments,
    const WriteSettings & write_settings)
{
    writeStringFile(storage, RANGES_FILE_NAME, serializeRangeSegments(segments), write_settings);
}

std::vector<LocatorRangeSegment> readRangeSegments(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings)
{
    return parseRangeSegments(readStringFile(storage, RANGES_FILE_NAME, read_settings));
}

}

}
