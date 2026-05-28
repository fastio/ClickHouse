#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <limits>
#include <unordered_map>
#include <vector>


namespace Poco::JSON
{
    class Object;
}

namespace DB
{

class ReadBuffer;
class IDataPartStorage;
class WriteBuffer;

/// Translate a DiskANN-style fbin row index (a.k.a. `internal_id`) back into
/// source-table coordinates `(source_part_uuid, _part_offset)`.
///
/// Wraps the on-disk layout of one materialized-index-part:
///   * `header.json::segment_boundaries` carves the global id space into
///     `locator_<seg>.bin` segments
///   * each segment file holds 12-byte records `(part_uuid_id, _part_offset)`
///   * `header.json::part_uuid_table` resolves ids to UUIDs
///
class ANNIndexPartReverseLookup
{
public:
    static constexpr UInt64 LOCATOR_ENTRY_SIZE = sizeof(UInt32) + sizeof(UInt64);
    static constexpr UInt64 LOCATOR_PAGE_BYTES = 64 * 1024;
    static constexpr UInt64 LOCATOR_PAGE_ROWS = LOCATOR_PAGE_BYTES / LOCATOR_ENTRY_SIZE;

    /// Reserved UUID table id used by Remap when the source row is no longer
    /// live. Build never assigns this id to a real source part.
    static constexpr UInt32 TOMBSTONE_PART_UUID_ID = std::numeric_limits<UInt32>::max();

    struct LocatorEntry
    {
        UInt32 part_uuid_id = 0;
        UInt64 part_offset = 0;

        bool isTombstone() const
        {
            return part_uuid_id == TOMBSTONE_PART_UUID_ID;
        }
    };

    static LocatorEntry liveLocatorEntry(UInt32 part_uuid_id, UInt64 part_offset);
    static LocatorEntry tombstoneLocatorEntry();
    static LocatorEntry readLocatorEntry(ReadBuffer & in);
    static void writeLocatorEntry(const LocatorEntry & entry, WriteBuffer & out);
    static void addLocatorHeaderFields(Poco::JSON::Object & header);
    static void validateLocatorHeader(const Poco::JSON::Object & header);

    explicit ANNIndexPartReverseLookup(const IDataPartStorage & storage_);

    struct SourceRow
    {
        UUID part_uuid;
        UInt64 part_offset = 0;
        bool is_tombstone = false;
    };

    /// Throws LOGICAL_ERROR if `hit_id >= total_rows` or the on-disk files
    /// disagree with the header's segment boundaries.
    SourceRow lookup(UInt64 hit_id);

    UInt64 totalRows() const { return total_rows; }

private:
    const IDataPartStorage & storage;
    std::vector<UUID> uuid_table;
    std::vector<UInt64> segment_boundaries;
    UInt64 total_rows = 0;

    struct LocatorPage
    {
        UInt64 page_start_row = 0;
        std::vector<LocatorEntry> rows;
    };

    /// Per-query page cache. One touched segment keeps one aligned locator
    /// page, which keeps memory bounded while preserving locality for nearby hits.
    std::unordered_map<size_t, LocatorPage> segment_pages;

    const LocatorEntry & getLocatorEntry(size_t segment_index, UInt64 offset_in_segment);
    void loadLocatorPage(size_t segment_index, UInt64 page_start_row, LocatorPage & page);
    size_t segmentIndexFor(UInt64 hit_id) const;
};

}
