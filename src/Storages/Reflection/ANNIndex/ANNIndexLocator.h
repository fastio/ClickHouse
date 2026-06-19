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
inline constexpr UInt8 LOCATOR_FORMAT_VERSION = 3;
inline constexpr UInt64 OFFSET_TOMBSTONE = std::numeric_limits<UInt64>::max();

/// On-disk record sizes (also the inline-graph payload record size). The offset
/// record is byte-identical to one graph payload entry: UInt32 part_id followed
/// by UInt64 part_offset.
inline constexpr UInt64 STABLE_ID_RECORD_SIZE = sizeof(UInt64) * 2;
inline constexpr UInt64 OFFSET_RECORD_SIZE = sizeof(UInt32) + sizeof(UInt64);

struct StableId
{
    UInt64 block_number = 0;
    UInt64 block_offset = 0;

    bool operator==(const StableId &) const = default;
};

struct OffsetEntry
{
    UInt32 part_id = 0;
    UInt64 part_offset = 0;

    bool operator==(const OffsetEntry &) const = default;
};

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

void writeOffsetsToBuffer(const std::vector<OffsetEntry> & offsets, WriteBuffer & out);
std::vector<OffsetEntry> readOffsetsFromBuffer(ReadBuffer & in, UInt64 bytes);
void writeOffsets(
    IDataPartStorage & storage,
    const std::vector<OffsetEntry> & offsets,
    const WriteSettings & write_settings);
std::vector<OffsetEntry> readOffsets(
    const IDataPartStorage & storage,
    const ReadSettings & read_settings);
OffsetEntry readOffsetAt(
    const IDataPartStorage & storage,
    UInt32 internal_id,
    const ReadSettings & read_settings);

}

}
