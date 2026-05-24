#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartReverseLookup.h>

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
#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


AuxiliaryIndexPartReverseLookup::LocatorEntry AuxiliaryIndexPartReverseLookup::liveLocatorEntry(UInt32 part_uuid_id, UInt64 part_offset)
{
    return {part_uuid_id, part_offset};
}


AuxiliaryIndexPartReverseLookup::LocatorEntry AuxiliaryIndexPartReverseLookup::tombstoneLocatorEntry()
{
    return {TOMBSTONE_PART_UUID_ID, 0};
}


AuxiliaryIndexPartReverseLookup::LocatorEntry AuxiliaryIndexPartReverseLookup::readLocatorEntry(ReadBuffer & in)
{
    LocatorEntry entry;
    readBinaryLittleEndian(entry.part_uuid_id, in);
    readBinaryLittleEndian(entry.part_offset, in);
    return entry;
}


void AuxiliaryIndexPartReverseLookup::writeLocatorEntry(const LocatorEntry & entry, WriteBuffer & out)
{
    writeBinaryLittleEndian(entry.part_uuid_id, out);
    writeBinaryLittleEndian(entry.part_offset, out);
}


void AuxiliaryIndexPartReverseLookup::addLocatorHeaderFields(Poco::JSON::Object & header)
{
    header.set("tombstone_part_uuid_id", static_cast<UInt64>(TOMBSTONE_PART_UUID_ID));
}


void AuxiliaryIndexPartReverseLookup::validateLocatorHeader(const Poco::JSON::Object & header)
{
    if (!header.has("part_uuid_table"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: header.json missing part_uuid_table");

    if (!header.has("tombstone_part_uuid_id"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: header.json missing tombstone_part_uuid_id");
    const UInt64 tombstone_part_uuid_id = header.getValue<UInt64>("tombstone_part_uuid_id");
    if (tombstone_part_uuid_id != TOMBSTONE_PART_UUID_ID)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: unsupported tombstone_part_uuid_id {}, expected {}",
            tombstone_part_uuid_id, TOMBSTONE_PART_UUID_ID);
}


AuxiliaryIndexPartReverseLookup::AuxiliaryIndexPartReverseLookup(const IDataPartStorage & storage_)
    : storage(storage_)
{
    if (!storage.existsFile("header.json"))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: header.json missing in materialized-index-part {}", storage.getRelativePath());

    auto header_reader = storage.readFile("header.json", ReadSettings{}, std::nullopt);
    String header_text;
    readStringUntilEOF(header_text, *header_reader);

    Poco::JSON::Parser parser;
    auto parsed = parser.parse(header_text);
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();
    if (!obj)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: header.json is not a JSON object");

    validateLocatorHeader(*obj);

    const auto & uuid_arr = obj->getArray("part_uuid_table");
    if (!uuid_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: part_uuid_table is not an array");
    uuid_table.reserve(uuid_arr->size());
    for (size_t i = 0; i < uuid_arr->size(); ++i)
    {
        UUID uuid;
        const auto uuid_text = uuid_arr->getElement<std::string>(static_cast<unsigned int>(i));
        if (!tryParse(uuid, uuid_text))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AuxiliaryIndexPartReverseLookup: cannot parse part_uuid_table[{}] as UUID: {}",
                i, uuid_text);
        uuid_table.push_back(uuid);
    }

    auto boundaries_var = obj->get("segment_boundaries");
    if (boundaries_var.isEmpty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: header.json missing segment_boundaries");
    const auto & boundaries_arr = boundaries_var.extract<Poco::JSON::Array::Ptr>();
    if (!boundaries_arr)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: segment_boundaries is not an array");

    const auto count = boundaries_arr->size();
    segment_boundaries.reserve(count);
    for (size_t i = 0; i < count; ++i)
        segment_boundaries.push_back(boundaries_arr->get(static_cast<unsigned int>(i)).convert<UInt64>());

    if (segment_boundaries.empty() || segment_boundaries.front() != 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: segment_boundaries must start at 0");

    total_rows = segment_boundaries.back();
}


size_t AuxiliaryIndexPartReverseLookup::segmentIndexFor(UInt64 hit_id) const
{
    /// segment_boundaries == [0, s1, s2, ..., total_rows], so the half-open
    /// segment k covers ids in [boundaries[k], boundaries[k+1]). upper_bound
    /// then -1 finds k.
    auto it = std::upper_bound(segment_boundaries.begin(), segment_boundaries.end(), hit_id);
    chassert(it != segment_boundaries.begin());
    return static_cast<size_t>(std::distance(segment_boundaries.begin(), it) - 1);
}


const AuxiliaryIndexPartReverseLookup::LocatorEntry & AuxiliaryIndexPartReverseLookup::getLocatorEntry(
    size_t segment_index,
    UInt64 offset_in_segment)
{
    auto & page = segment_pages[segment_index];
    const UInt64 page_start_row = offset_in_segment / LOCATOR_PAGE_ROWS * LOCATOR_PAGE_ROWS;
    if (page.rows.empty()
        || page.page_start_row != page_start_row
        || offset_in_segment >= page.page_start_row + page.rows.size())
        loadLocatorPage(segment_index, page_start_row, page);

    return page.rows[offset_in_segment - page.page_start_row];
}


void AuxiliaryIndexPartReverseLookup::loadLocatorPage(size_t segment_index, UInt64 page_start_row, LocatorPage & page)
{
    const String segment_path = fmt::format("locator_{}.bin", segment_index);
    if (!storage.existsFile(segment_path))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: {} missing", segment_path);

    const UInt64 expected_rows = segment_boundaries[segment_index + 1] - segment_boundaries[segment_index];
    if (page_start_row >= expected_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: page_start_row {} >= expected_rows {} for {}",
            page_start_row, expected_rows, segment_path);

    const UInt64 rows_to_read = std::min(LOCATOR_PAGE_ROWS, expected_rows - page_start_row);
    auto reader = storage.readFile(segment_path, ReadSettings{}, std::nullopt);
    reader->seek(static_cast<off_t>(page_start_row * LOCATOR_ENTRY_SIZE), SEEK_SET);

    std::vector<LocatorEntry> entries;
    entries.reserve(rows_to_read);
    for (UInt64 i = 0; i < rows_to_read; ++i)
    {
        if (reader->eof())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AuxiliaryIndexPartReverseLookup: {} truncated at row {} (expected {})",
                segment_path, page_start_row + i, expected_rows);
        entries.push_back(readLocatorEntry(*reader));
    }
    if (page_start_row + rows_to_read == expected_rows)
        assertEOF(*reader);

    page.page_start_row = page_start_row;
    page.rows = std::move(entries);
}


AuxiliaryIndexPartReverseLookup::SourceRow AuxiliaryIndexPartReverseLookup::lookup(UInt64 hit_id)
{
    if (hit_id >= total_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: hit_id {} >= total_rows {}", hit_id, total_rows);

    const size_t segment_index = segmentIndexFor(hit_id);
    const UInt64 offset_in_segment = hit_id - segment_boundaries[segment_index];
    const auto & entry = getLocatorEntry(segment_index, offset_in_segment);

    /// Tombstone convention from RewriteMutableSegmentsStage: outgoing rows
    /// get written with a reserved UUID-table id. Surface this as a
    /// distinguished flag so callers can drop the hit without interpreting it
    /// as a real source row.
    if (entry.isTombstone())
    {
        SourceRow row;
        row.is_tombstone = true;
        return row;
    }

    if (entry.part_uuid_id >= uuid_table.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "AuxiliaryIndexPartReverseLookup: part_uuid_id {} >= part_uuid_table.size {}",
            entry.part_uuid_id, uuid_table.size());

    SourceRow row;
    row.part_uuid = uuid_table[entry.part_uuid_id];
    row.part_offset = entry.part_offset;
    return row;
}

}
