#include <Storages/MaterializedIndex/MiPartReverseLookup.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Storages/MergeTree/IDataPartStorage.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <fmt/format.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MiPartReverseLookup::MiPartReverseLookup(const IDataPartStorage & storage_)
    : storage(storage_)
{
    if (storage.existsFile("part_uuid_dict.bin"))
    {
        auto reader = storage.readFile("part_uuid_dict.bin", ReadSettings{}, std::nullopt);
        while (!reader->eof())
        {
            UInt64 hi = 0;
            UInt64 lo = 0;
            readBinaryLittleEndian(hi, *reader);
            readBinaryLittleEndian(lo, *reader);
            UUID uuid;
            UUIDHelpers::getHighBytes(uuid) = hi;
            UUIDHelpers::getLowBytes(uuid) = lo;
            uuid_dict.push_back(uuid);
        }
    }

    if (!storage.existsFile("header.json"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: header.json missing in mi-part {}", storage.getRelativePath());

    auto header_reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *header_reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: header.json is not a JSON object");

    auto boundaries_var = obj->get("segment_boundaries");
    if (boundaries_var.isEmpty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: header.json missing segment_boundaries");
    const auto & boundaries_arr = boundaries_var.extract<Poco::JSON::Array::Ptr>();
    if (!boundaries_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: segment_boundaries is not an array");

    const auto count = boundaries_arr->size();
    segment_boundaries.reserve(count);
    for (size_t i = 0; i < count; ++i)
        segment_boundaries.push_back(boundaries_arr->get(static_cast<unsigned int>(i)).convert<UInt64>());

    if (segment_boundaries.size() < 2 || segment_boundaries.front() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: segment_boundaries must start at 0 with at least one segment");

    total_rows = segment_boundaries.back();
}


size_t MiPartReverseLookup::segmentIndexFor(UInt64 hit_id) const
{
    /// segment_boundaries == [0, s1, s2, ..., total_rows], so the half-open
    /// segment k covers ids in [boundaries[k], boundaries[k+1]). upper_bound
    /// then -1 finds k.
    auto it = std::upper_bound(segment_boundaries.begin(), segment_boundaries.end(), hit_id);
    chassert(it != segment_boundaries.begin());
    return static_cast<size_t>(std::distance(segment_boundaries.begin(), it) - 1);
}


void MiPartReverseLookup::loadSegment(size_t segment_index)
{
    if (segment_cache.contains(segment_index))
        return;

    const String segment_path = fmt::format("mutable_offset/{}.bin", segment_index);
    if (!storage.existsFile(segment_path))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: {} missing", segment_path);

    const UInt64 expected_rows = segment_boundaries[segment_index + 1] - segment_boundaries[segment_index];
    auto reader = storage.readFile(segment_path, ReadSettings{}, std::nullopt);

    std::vector<std::pair<UInt32, UInt64>> entries;
    entries.reserve(expected_rows);
    for (UInt64 i = 0; i < expected_rows; ++i)
    {
        if (reader->eof())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MiPartReverseLookup: {} truncated at row {} (expected {})",
                segment_path, i, expected_rows);
        UInt32 dict_id = 0;
        UInt64 part_offset = 0;
        readBinaryLittleEndian(dict_id, *reader);
        readBinaryLittleEndian(part_offset, *reader);
        entries.emplace_back(dict_id, part_offset);
    }

    segment_cache.emplace(segment_index, std::move(entries));
}


MiPartReverseLookup::SourceRow MiPartReverseLookup::lookup(UInt64 hit_id)
{
    if (hit_id >= total_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: hit_id {} >= total_rows {}", hit_id, total_rows);

    const size_t segment_index = segmentIndexFor(hit_id);
    loadSegment(segment_index);

    const auto & entries = segment_cache.at(segment_index);
    const UInt64 offset_in_segment = hit_id - segment_boundaries[segment_index];
    const auto [dict_id, part_offset] = entries[offset_in_segment];

    /// Tombstone convention from RewriteMutableSegmentsStage: outgoing rows
    /// get written as (dict_id=0, _part_offset=0). We surface this as a
    /// distinguished "is_tombstone" flag so callers can drop the hit
    /// without misinterpreting it as part 0 of the dictionary.
    if (dict_id == 0 && part_offset == 0 && !uuid_dict.empty())
    {
        SourceRow row;
        row.is_tombstone = true;
        return row;
    }

    if (dict_id >= uuid_dict.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MiPartReverseLookup: dict_id {} >= uuid_dict.size {}", dict_id, uuid_dict.size());

    SourceRow row;
    row.part_uuid = uuid_dict[dict_id];
    row.part_offset = part_offset;
    return row;
}

}
