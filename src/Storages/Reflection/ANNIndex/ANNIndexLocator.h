#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <limits>
#include <string_view>
#include <vector>


namespace DB
{

class IDataPartStorage;
class ReadBuffer;
class WriteBuffer;
struct ReadSettings;
struct WriteSettings;

namespace ANNIndexLocator
{

inline constexpr std::string_view STABLE_ID_FILE_NAME = "stable_id.bin";
inline constexpr std::string_view OFFSET_FILE_NAME = "offset.bin";
inline constexpr std::string_view RANGES_FILE_NAME = "locator_ranges.json";
inline constexpr UInt8 LOCATOR_FORMAT_VERSION = 2;
inline constexpr UInt64 OFFSET_TOMBSTONE = std::numeric_limits<UInt64>::max();

struct StableId
{
    UInt64 block_number = 0;
    UInt64 block_offset = 0;

    bool operator==(const StableId &) const = default;
};

struct LocatorRangeSegment
{
    UInt32 start_internal_id = 0;
    UInt32 end_internal_id = 0;
    UUID target_part_uuid = UUIDHelpers::Nil;

    bool operator==(const LocatorRangeSegment &) const = default;
};

class GraphRangeMap
{
public:
    GraphRangeMap() = default;
    explicit GraphRangeMap(const std::vector<LocatorRangeSegment> & segments_);

    const LocatorRangeSegment & lookup(UInt32 internal_id) const;
    bool empty() const { return segments.empty(); }
    size_t size() const { return segments.size(); }
    UInt32 rows() const { return segments.empty() ? 0 : segments.back().end_internal_id; }
    const std::vector<LocatorRangeSegment> & getSegments() const { return segments; }

private:
    std::vector<UInt32> starts;
    std::vector<LocatorRangeSegment> segments;
};

void validateRangeSegments(const std::vector<LocatorRangeSegment> & segments);

void writeStableIdsToBuffer(const std::vector<StableId> & ids, WriteBuffer & out);
std::vector<StableId> readStableIdsFromBuffer(ReadBuffer & in, UInt64 bytes);
void writeStableIds(
    IDataPartStorage & storage,
    const std::vector<StableId> & ids,
    const WriteSettings & write_settings);
std::vector<StableId> readStableIds(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings);
StableId readStableIdAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings);

void writeOffsetsToBuffer(const std::vector<UInt64> & offsets, WriteBuffer & out);
std::vector<UInt64> readOffsetsFromBuffer(ReadBuffer & in, UInt64 bytes);
void writeOffsets(
    IDataPartStorage & storage,
    const std::vector<UInt64> & offsets,
    const WriteSettings & write_settings);
std::vector<UInt64> readOffsets(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings);
UInt64 readOffsetAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings);

String serializeRangeSegments(const std::vector<LocatorRangeSegment> & segments);
std::vector<LocatorRangeSegment> parseRangeSegments(const String & body);
void writeRangeSegments(
    IDataPartStorage & storage,
    const std::vector<LocatorRangeSegment> & segments,
    const WriteSettings & write_settings);
std::vector<LocatorRangeSegment> readRangeSegments(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings);

}

}
