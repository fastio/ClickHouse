#pragma once

#include <Core/Types.h>
#include <Core/UUID.h>

#include <unordered_map>
#include <vector>


namespace DB
{

class IDataPartStorage;

/// Translate a DiskANN-style fbin row index (a.k.a. `internal_id`) back into
/// source-table coordinates `(source_part_uuid, _part_offset)`.
///
/// Wraps the on-disk layout of one mi-part:
///   * `header.json::segment_boundaries` carves the global id space into
///     `mutable_offset/<seg>.bin` segments
///   * each segment file holds 12-byte records `(part_uuid_dict_id, _part_offset)`
///   * `part_uuid_dict.bin` resolves dict ids to UUIDs (16 bytes/entry)
///
/// A dict id of 0 with `_part_offset == 0` is the tombstone pattern produced
/// by Remap when the source part has gone away.
class MiPartReverseLookup
{
public:
    explicit MiPartReverseLookup(const IDataPartStorage & storage_);

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

    /// Lazily-loaded mutable_offset segments. Each entry is the entire
    /// `mutable_offset/<seg>.bin` parsed into `(dict_id, _part_offset)`
    /// pairs. 12 bytes/row is small enough that loading the whole segment
    /// on first hit avoids a second IO per lookup at the cost of one
    /// allocation per touched segment.
    std::unordered_map<size_t, std::vector<std::pair<UInt32, UInt64>>> segment_cache;

    void loadSegment(size_t segment_index);
    size_t segmentIndexFor(UInt64 hit_id) const;
};

}
