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
///     `mutable_mapping_<seg>.bin` segments
///   * each segment file holds 12-byte records `(part_uuid_dict_id, _part_offset)`
///   * `part_uuid_dict.bin` resolves dict ids to UUIDs (16 bytes/entry)
///
class MaterializedIndexPartReverseLookup
{
public:
    static constexpr UInt64 LOCATOR_FORMAT_VERSION = 1;

    /// Reserved dictionary id used by Remap when the source row is no longer
    /// live. Build never assigns this id to a real source part.
    static constexpr UInt32 TOMBSTONE_DICT_ID = std::numeric_limits<UInt32>::max();

    struct LocatorEntry
    {
        UInt32 dict_id = 0;
        UInt64 part_offset = 0;

        bool isTombstone() const
        {
            return dict_id == TOMBSTONE_DICT_ID;
        }
    };

    static LocatorEntry liveLocatorEntry(UInt32 dict_id, UInt64 part_offset);
    static LocatorEntry tombstoneLocatorEntry();
    static LocatorEntry readLocatorEntry(ReadBuffer & in);
    static void writeLocatorEntry(const LocatorEntry & entry, WriteBuffer & out);
    static void addLocatorHeaderFields(Poco::JSON::Object & header);
    static void validateLocatorHeader(const Poco::JSON::Object & header);

    explicit MaterializedIndexPartReverseLookup(const IDataPartStorage & storage_);

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
    std::vector<UUID> uuid_dict;
    std::vector<UInt64> segment_boundaries;
    UInt64 total_rows = 0;

    /// Lazily-loaded mutable_mapping segments. Each entry is the entire
    /// `mutable_mapping_<seg>.bin` parsed into locator entries. 12 bytes/row
    /// is small enough that loading the whole segment on first hit avoids a
    /// second IO per lookup at the cost of one allocation per touched segment.
    std::unordered_map<size_t, std::vector<LocatorEntry>> segment_cache;

    void loadSegment(size_t segment_index);
    size_t segmentIndexFor(UInt64 hit_id) const;
};

}
