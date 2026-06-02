#include <Storages/Reflection/ANNIndex/ANNIndexPayloadCodec.h>

#include <cstring>


namespace DB::ANNIndexPayloadCodec
{

Version chooseVersion(UInt64 max_part_rows) noexcept
{
    if (max_part_rows <= static_cast<UInt64>(std::numeric_limits<UInt32>::max()))
        return Version::V1_OFFSET32;
    return Version::V2_OFFSET64;
}

void pack(uint8_t * dest, Version v, UInt32 part_id, UInt64 part_offset) noexcept
{
    /// Layout (little-endian):
    ///   V1 = [part_id:4B][offset_lo32:4B]
    ///   V2 = [part_id:4B][offset:8B]
    std::memcpy(dest, &part_id, sizeof(UInt32));
    if (v == Version::V1_OFFSET32)
    {
        const UInt32 narrow = static_cast<UInt32>(part_offset);
        std::memcpy(dest + sizeof(UInt32), &narrow, sizeof(UInt32));
    }
    else
    {
        std::memcpy(dest + sizeof(UInt32), &part_offset, sizeof(UInt64));
    }
}

void unpack(const uint8_t * src, Version v, UInt32 & part_id, UInt64 & part_offset) noexcept
{
    std::memcpy(&part_id, src, sizeof(UInt32));
    if (v == Version::V1_OFFSET32)
    {
        UInt32 narrow = 0;
        std::memcpy(&narrow, src + sizeof(UInt32), sizeof(UInt32));
        part_offset = narrow;
    }
    else
    {
        std::memcpy(&part_offset, src + sizeof(UInt32), sizeof(UInt64));
    }
}

}
