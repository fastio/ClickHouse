#include <Storages/MergeTree/RangesInDataPart.h>

#include <Core/ProtocolDefines.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <IO/VarInt.h>

template <>
struct fmt::formatter<DB::RangesInDataPartDescription>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::RangesInDataPartDescription & range, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", range.describe());
    }
};

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

void appendMergedMarkRange(MarkRanges & ranges, MarkRange range)
{
    if (range.begin == range.end)
        return;

    if (!ranges.empty() && ranges.back().end >= range.begin)
    {
        ranges.back().end = std::max(ranges.back().end, range.end);
        return;
    }

    ranges.push_back(range);
}

MarkRanges buildMarkRangesForPartOffsets(const RangesInDataPart & part, const std::vector<UInt64> & part_offsets)
{
    return buildMarkRangesForPartOffsetsForANNIndex(
        *part.data_part->index_granularity, part.data_part->rows_count, part.data_part->name, part_offsets);
}

MarkRanges intersectMarkRanges(const MarkRanges & lhs, const MarkRanges & rhs)
{
    MarkRanges result;
    size_t i = 0;
    size_t j = 0;
    while (i < lhs.size() && j < rhs.size())
    {
        const auto begin = std::max(lhs[i].begin, rhs[j].begin);
        const auto end = std::min(lhs[i].end, rhs[j].end);
        if (begin < end)
            appendMergedMarkRange(result, MarkRange{begin, end});

        if (lhs[i].end < rhs[j].end)
            ++i;
        else
            ++j;
    }
    return result;
}

void pruneANNIndexRangesForPart(RangesInDataPart & part, const ANNIndexHints & hints)
{
    if (!hints.covered_source_parts.contains(part.data_part->uuid))
        return;

    const auto hits_it = hints.hits_per_part.find(part.data_part->uuid);
    if (hits_it == hints.hits_per_part.end() || hits_it->second.rows.empty())
    {
        part.ranges.clear();
        part.exact_ranges.clear();
        return;
    }

    const auto ann_mark_ranges = buildMarkRangesForPartOffsets(part, hits_it->second.rows);
    part.ranges = intersectMarkRanges(part.ranges, ann_mark_ranges);
    if (!part.exact_ranges.empty())
        part.exact_ranges = intersectMarkRanges(part.exact_ranges, ann_mark_ranges);
}

}


MarkRanges buildMarkRangesForPartOffsetsForANNIndex(
    const MergeTreeIndexGranularity & granularity, UInt64 rows_count, const String & part_name, const std::vector<UInt64> & part_offsets)
{
    std::vector<UInt64> offsets(part_offsets.begin(), part_offsets.end());
    std::sort(offsets.begin(), offsets.end());
    offsets.erase(std::unique(offsets.begin(), offsets.end()), offsets.end());

    MarkRanges ranges;
    for (UInt64 offset : offsets)
    {
        if (offset >= rows_count)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "ANNIndex source part offset {} is out of range for part {} with {} rows",
                offset,
                part_name,
                rows_count);

        appendMergedMarkRange(ranges, granularity.getMarkRangeForRowOffset(offset));
    }

    return ranges;
}

void attachANNIndexHintForPart(
    const UUID & part_uuid, RangesInDataPartReadHints & read_hints, const ANNIndexHints & hints)
{
    /// Only parts with at least one hit need a reader-side entry. Covered-but-
    /// zero-hits parts have their ranges cleared in `pruneANNIndexRangesForPart`
    /// and therefore never reach the reader; writing an entry for them would be
    /// dead state.
    auto it = hints.hits_per_part.find(part_uuid);
    if (it == hints.hits_per_part.end())
        return;

    chassert(!read_hints.ann_index_search_results.has_value());
    read_hints.ann_index_search_results = it->second;
}


void applyANNIndexHints(RangesInDataParts & parts, const ANNIndexHints & hints)
{
    for (auto & part : parts)
    {
        attachANNIndexHintForPart(part.data_part->uuid, part.read_hints, hints);
        pruneANNIndexRangesForPart(part, hints);
    }
}


void RangesInDataPartDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    info.serialize(out);
    ranges.serialize(out);
    writeVarUInt(rows, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        writeBinary(projection_name, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        writeVarUInt(min_marks_per_task, out);
}

String RangesInDataPartDescription::describe() const
{
    String result;
    result += fmt::format("{}[{}]", getPartOrProjectionName(), fmt::join(ranges, ","));
    return result;
}

String RangesInDataPartDescription::getPartOrProjectionName() const
{
    if (projection_name.empty())
        return info.getPartNameV1();

    return info.getPartNameV1() + "." + projection_name;
}

void RangesInDataPartDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    info.deserialize(in);
    ranges.deserialize(in);
    readVarUInt(rows, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        readBinary(projection_name, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        readVarUInt(min_marks_per_task, in);
}

void RangesInDataPartsDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    writeVarUInt(this->size(), out);
    for (const auto & desc : *this)
        desc.serialize(out, parallel_replicas_protocol_version);
}

String RangesInDataPartsDescription::describe() const
{
    return fmt::format("{} parts: [{}]", this->size(), fmt::join(*this, ", "));
}

void RangesInDataPartsDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);
    if (new_size > 100'000'000'000)
        throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of serialized parts description is suspiciously large: {}", new_size);

    this->resize(new_size);
    for (auto & desc : *this)
        desc.deserialize(in, parallel_replicas_protocol_version);
}

void RangesInDataPartsDescription::merge(const RangesInDataPartsDescription & other)
{
    for (const auto & desc : other)
        this->emplace_back(desc);
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_,
    const DataPartPtr & parent_part_,
    size_t part_index_in_query_,
    size_t part_starting_offset_in_query_,
    const MarkRanges & ranges_,
    const RangesInDataPartReadHints & read_hints_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
    , ranges{ranges_}
    , read_hints{read_hints_}
{
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_, const DataPartPtr & parent_part_, size_t part_index_in_query_, size_t part_starting_offset_in_query_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
{
    size_t total_marks_count = data_part->index_granularity->getMarksCountWithoutFinal();
    if (total_marks_count)
        ranges.emplace_back(0, total_marks_count);
}

RangesInDataPartDescription RangesInDataPart::getDescription() const
{
    chassert(!data_part->isProjectionPart() || parent_part);
    return RangesInDataPartDescription{
        .info = data_part->isProjectionPart() ? parent_part->info : data_part->info,
        .ranges = ranges,
        .rows = getRowsCount(),
        .projection_name = data_part->isProjectionPart() ? data_part->name : "",
    };
}

size_t RangesInDataPart::getMarksCount() const
{
    return ranges.getNumberOfMarks();
}

size_t RangesInDataPart::getRowsCount() const
{
    return data_part->index_granularity->getRowsCountInRanges(ranges);
}

RangesInDataParts::RangesInDataParts(const DataPartsVector & parts)
{
    size_t num_parts = parts.size();
    reserve(num_parts);
    size_t starting_offset = 0;
    for (size_t i = 0; i < num_parts; ++i)
    {
        chassert(!parts[i]->isProjectionPart());
        emplace_back(parts[i], nullptr, i, starting_offset);
        starting_offset += parts[i]->rows_count;
    }
}

RangesInDataPartsDescription RangesInDataParts::getDescriptions() const
{
    RangesInDataPartsDescription result;
    for (const auto & part : *this)
        result.emplace_back(part.getDescription());
    return result;
}


size_t RangesInDataParts::getMarksCountAllParts() const
{
    size_t result = 0;
    for (const auto & part : *this)
        result += part.getMarksCount();
    return result;
}

size_t RangesInDataParts::getRowsCountAllParts() const
{
    size_t result = 0;
    for (const auto & part: *this)
        result += part.getRowsCount();
    return result;
}

}
