#include <Storages/MaterializedIndex/MaterializedIndexPartReverseLookup.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
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


MaterializedIndexPartReverseLookup::LocatorEntry MaterializedIndexPartReverseLookup::liveLocatorEntry(UInt32 dict_id, UInt64 part_offset)
{
    return {dict_id, part_offset};
}


MaterializedIndexPartReverseLookup::LocatorEntry MaterializedIndexPartReverseLookup::tombstoneLocatorEntry()
{
    return {TOMBSTONE_DICT_ID, 0};
}


MaterializedIndexPartReverseLookup::LocatorEntry MaterializedIndexPartReverseLookup::readLocatorEntry(ReadBuffer & in)
{
    LocatorEntry entry;
    readBinaryLittleEndian(entry.dict_id, in);
    readBinaryLittleEndian(entry.part_offset, in);
    return entry;
}


void MaterializedIndexPartReverseLookup::writeLocatorEntry(const LocatorEntry & entry, WriteBuffer & out)
{
    writeBinaryLittleEndian(entry.dict_id, out);
    writeBinaryLittleEndian(entry.part_offset, out);
}


void MaterializedIndexPartReverseLookup::addLocatorHeaderFields(Poco::JSON::Object & header)
{
    header.set("locator_format_version", LOCATOR_FORMAT_VERSION);
    header.set("locator_tombstone_dict_id", static_cast<UInt64>(TOMBSTONE_DICT_ID));
}


void MaterializedIndexPartReverseLookup::validateLocatorHeader(const Poco::JSON::Object & header)
{
    if (!header.has("locator_format_version"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: header.json missing locator_format_version");
    const UInt64 locator_format_version = header.getValue<UInt64>("locator_format_version");
    if (locator_format_version != LOCATOR_FORMAT_VERSION)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: unsupported locator_format_version {}, expected {}",
            locator_format_version, LOCATOR_FORMAT_VERSION);

    if (!header.has("locator_tombstone_dict_id"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: header.json missing locator_tombstone_dict_id");
    const UInt64 tombstone_dict_id = header.getValue<UInt64>("locator_tombstone_dict_id");
    if (tombstone_dict_id != TOMBSTONE_DICT_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: unsupported locator_tombstone_dict_id {}, expected {}",
            tombstone_dict_id, TOMBSTONE_DICT_ID);
}


MaterializedIndexPartReverseLookup::MaterializedIndexPartReverseLookup(const IDataPartStorage & storage_)
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
            "MaterializedIndexPartReverseLookup: header.json missing in materialized-index-part {}", storage.getRelativePath());

    auto header_reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *header_reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: header.json is not a JSON object");

    validateLocatorHeader(*obj);

    auto boundaries_var = obj->get("segment_boundaries");
    if (boundaries_var.isEmpty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: header.json missing segment_boundaries");
    const auto & boundaries_arr = boundaries_var.extract<Poco::JSON::Array::Ptr>();
    if (!boundaries_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: segment_boundaries is not an array");

    const auto count = boundaries_arr->size();
    segment_boundaries.reserve(count);
    for (size_t i = 0; i < count; ++i)
        segment_boundaries.push_back(boundaries_arr->get(static_cast<unsigned int>(i)).convert<UInt64>());

    if (segment_boundaries.empty() || segment_boundaries.front() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: segment_boundaries must start at 0");

    total_rows = segment_boundaries.back();
}


size_t MaterializedIndexPartReverseLookup::segmentIndexFor(UInt64 hit_id) const
{
    /// segment_boundaries == [0, s1, s2, ..., total_rows], so the half-open
    /// segment k covers ids in [boundaries[k], boundaries[k+1]). upper_bound
    /// then -1 finds k.
    auto it = std::upper_bound(segment_boundaries.begin(), segment_boundaries.end(), hit_id);
    chassert(it != segment_boundaries.begin());
    return static_cast<size_t>(std::distance(segment_boundaries.begin(), it) - 1);
}


void MaterializedIndexPartReverseLookup::loadSegment(size_t segment_index)
{
    if (segment_cache.contains(segment_index))
        return;

    const String segment_path = fmt::format("mutable_mapping_{}.bin", segment_index);
    if (!storage.existsFile(segment_path))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: {} missing", segment_path);

    const UInt64 expected_rows = segment_boundaries[segment_index + 1] - segment_boundaries[segment_index];
    auto reader = storage.readFile(segment_path, ReadSettings{}, std::nullopt);

    std::vector<LocatorEntry> entries;
    entries.reserve(expected_rows);
    for (UInt64 i = 0; i < expected_rows; ++i)
    {
        if (reader->eof())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "MaterializedIndexPartReverseLookup: {} truncated at row {} (expected {})",
                segment_path, i, expected_rows);
        entries.push_back(readLocatorEntry(*reader));
    }

    segment_cache.emplace(segment_index, std::move(entries));
}


MaterializedIndexPartReverseLookup::SourceRow MaterializedIndexPartReverseLookup::lookup(UInt64 hit_id)
{
    if (hit_id >= total_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: hit_id {} >= total_rows {}", hit_id, total_rows);

    const size_t segment_index = segmentIndexFor(hit_id);
    loadSegment(segment_index);

    const auto & entries = segment_cache.at(segment_index);
    const UInt64 offset_in_segment = hit_id - segment_boundaries[segment_index];
    const auto & entry = entries[offset_in_segment];

    /// Tombstone convention from RewriteMutableSegmentsStage: outgoing rows
    /// get written with a reserved dictionary id. Surface this as a
    /// distinguished flag so callers can drop the hit without interpreting it
    /// as a real source row.
    if (entry.isTombstone())
    {
        SourceRow row;
        row.is_tombstone = true;
        return row;
    }

    if (entry.dict_id >= uuid_dict.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "MaterializedIndexPartReverseLookup: dict_id {} >= uuid_dict.size {}", entry.dict_id, uuid_dict.size());

    SourceRow row;
    row.part_uuid = uuid_dict[entry.dict_id];
    row.part_offset = entry.part_offset;
    return row;
}

}
