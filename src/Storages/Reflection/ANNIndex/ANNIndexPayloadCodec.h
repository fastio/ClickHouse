#pragma once

#include <Core/Types.h>

#include <cstddef>
#include <cstdint>
#include <limits>


namespace DB::ANNIndexPayloadCodec
{

/// On-disk / in-memory record layout for ANNIndex inline payload. Replaces
/// the legacy 24 B `[UUID:16B][offset:UInt64]` layout. The (UUID → UInt32
/// part_id) mapping is persisted per-materialized-index-part in
/// `coverage.json` (`payload_part_id` field), so the payload itself only
/// carries the dense local id.
///
/// Two record widths are supported. The build-time chooser
/// (`chooseVersion`) picks the smallest one that fits the largest
/// `part_offset` actually present in the source parts. Both engines
/// (DiskANN sidecar, SPTAG `MemMetadataSet`) use the exact same record
/// bytes, so the matcher is engine-agnostic.
enum class Version : UInt8
{
    /// 8 B / record. Used when every covered source part has
    /// `rows_count <= UINT32_MAX` (the common case in production).
    V1_OFFSET32 = 1,

    /// 12 B / record. Used when at least one covered part exceeds
    /// `UINT32_MAX` rows. Defensive fallback; rare in practice.
    V2_OFFSET64 = 2,
};

/// Sentinel `part_id` used to mark a remapped-away (deleted) row. Reserved
/// across both versions, so a part_id of `0xFFFF_FFFF` never refers to a
/// real source part. The matcher treats records carrying this value as
/// tombstones and skips them.
inline constexpr UInt32 PART_ID_TOMBSTONE = std::numeric_limits<UInt32>::max();

/// Pick the smallest `Version` whose `part_offset` field can hold
/// `max_part_rows - 1` (the largest valid offset). Inputs:
///   `max_part_rows == 0`             → V1 (degenerate empty build).
///   `max_part_rows <= UINT32_MAX`    → V1 (8 B / record).
///   `max_part_rows >  UINT32_MAX`    → V2 (12 B / record).
Version chooseVersion(UInt64 max_part_rows) noexcept;

/// Bytes per record for the given version.
inline constexpr size_t recordSize(Version v) noexcept
{
    return v == Version::V1_OFFSET32 ? 8 : 12;
}

/// Encode one record at `dest`. Caller must ensure `dest` has at least
/// `recordSize(v)` writable bytes. `part_offset` is silently truncated
/// to 32 bits when `v == V1_OFFSET32` — callers must have validated the
/// fit via `chooseVersion` before reaching here.
void pack(uint8_t * dest, Version v, UInt32 part_id, UInt64 part_offset) noexcept;

/// Decode one record at `src`. Caller must ensure `src` has at least
/// `recordSize(v)` readable bytes.
void unpack(const uint8_t * src, Version v, UInt32 & part_id, UInt64 & part_offset) noexcept;

/// True iff the decoded record is a tombstone (remapped-away row).
/// Currently driven exclusively by `part_id`; the offset bits carry no
/// meaning when `part_id == PART_ID_TOMBSTONE`.
inline constexpr bool isTombstone(UInt32 part_id) noexcept
{
    return part_id == PART_ID_TOMBSTONE;
}

}
