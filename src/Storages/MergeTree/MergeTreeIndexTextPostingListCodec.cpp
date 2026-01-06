#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
//#include <Common/ProfileEvents.h>
//#include <Common/ElapsedTimeProfileEventIncrement.h>

#pragma clang optimize off
namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}


void PostingListCodecImpl::insert(uint32_t row)
{
    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = row;
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

//        prev_row = row;
        current_segment.emplace_back(row);
        ++rows_in_current_segment;
        ++total_rows;
        return;
    }

    current_segment.emplace_back(row);
//    prev_row = row;
    ++rows_in_current_segment;
    ++total_rows;

    if (current_segment.size() == BLOCK_SIZE)
        compressBlock(current_segment);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::insert(std::span<uint32_t> rows)
{
    chassert(rows.size() == BLOCK_SIZE && rows_in_current_segment % BLOCK_SIZE == 0);

    if (rows_in_current_segment == 0)
    {
        segment_descriptors.emplace_back();
        segment_descriptors.back().row_begin = rows.front();
        segment_descriptors.back().compressed_data_offset = compressed_data.size();

        prev_row = rows.front();
        rows_in_current_segment += BLOCK_SIZE;
        total_rows += BLOCK_SIZE;
    }

    compressBlock(rows);

    if (rows_in_current_segment == posting_list_block_size)
        flushCurrentSegment();
}

void PostingListCodecImpl::deserialize(ReadBuffer & in, PostingList & out)
{
    SegmentHeader header;
    header.read(in);
    if (header.codec_type != static_cast<uint8_t>(codec_type))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected codec type {}, but got {}", codec_type, header.codec_type);

    prev_row = header.base_value;

    uint32_t tail_block_size = header.cardinality % BLOCK_SIZE;
    uint32_t full_block_count = header.cardinality / BLOCK_SIZE;
    current_segment.reserve(BLOCK_SIZE);
    if (header.bytes > (compressed_data.capacity() - compressed_data.size()))
        compressed_data.reserve(compressed_data.size() + header.bytes);
    compressed_data.resize(header.bytes);
    in.readStrict(compressed_data.data(), header.bytes);

    //auto * p = reinterpret_cast<unsigned char *> (compressed_data.data());
    std::span<const std::byte> compressed_data_span(reinterpret_cast<const std::byte*>(compressed_data.data()), compressed_data.size());
    size_t block_count = full_block_count + (tail_block_size > 0 ? 1 : 0);
    for (uint32_t i = 0; i < block_count; i++)
    {
        if (i % 32u == 0)
            compressed_data_span = compressed_data_span.subspan(2 * sizeof(uint32_t));
        size_t block_size = ((i == block_count -1) && tail_block_size > 0) ? tail_block_size : BLOCK_SIZE;
        decodeOneBlock(compressed_data_span, block_size, prev_row, current_segment);
        out.addMany(current_segment.size(), current_segment.data());
    }
}

void PostingListCodecImpl::serializeTo(WriteBuffer & out, TokenPostingsInfo & info) const
{
    info.offsets.reserve(segment_descriptors.size());
    info.ranges.reserve(segment_descriptors.size());

    size_t f1, f2, f3;
    for (const auto & descriptor : segment_descriptors)
    {
        info.offsets.emplace_back(out.count());
        info.ranges.emplace_back(descriptor.row_begin, descriptor.row_end);
        SegmentHeader header(static_cast<uint8_t>(codec_type), descriptor.compressed_data_size, descriptor.cardinality, descriptor.row_begin);
        header.write(out);
        f1 = out.count();
        size_t offset = descriptor.compressed_data_offset;
        for (const auto & skip_entry : descriptor.skip_entries)
        {
            skip_entry.write(out);
            f2 = out.count();
            out.write(compressed_data.data() + offset, skip_entry.size);
            offset += skip_entry.size;
        }
        f3 = out.count();
        (void) f1;
        (void) f2;
        (void) f3;
        //out.write(compressed_data.data() + descriptor.compressed_data_offset, descriptor.compressed_data_size);
    }
}

#if 0
namespace
{
void encodeU8(uint8_t x, std::span<char> & out)
{
    out[0] = static_cast<char>(x);
    out = out.subspan(1);
}

uint8_t decodeU8(std::span<const std::byte> & in)
{
    auto v = static_cast<uint8_t>(in[0]);
    in = in.subspan(1);
    return v;
}
}
#endif

void PostingListCodecImpl::compressBlock(std::span<uint32_t> segment)
{
    uint32_t row_begin = segment.front();
    uint32_t row_end = segment.back();

    auto & last_segment = segment_descriptors.back();
    last_segment.cardinality += segment.size();
    last_segment.row_end = row_end;

    std::adjacent_difference(segment.begin(), segment.end(), segment.begin());
    segment[0] -= row_begin;

    auto [required_bytes, max_bits] = BlockCodec::calculateNeededBytesAndMaxBits(segment);
    size_t memory_gap = compressed_data.capacity() - compressed_data.size();
    static constexpr size_t BLOCK_HEADER_SIZE = sizeof(uint32_t) * 3;
    size_t required_bytes_total = required_bytes + BLOCK_HEADER_SIZE;
    if (memory_gap < required_bytes_total)
    {
        auto min_need = required_bytes_total - memory_gap;
        compressed_data.reserve(compressed_data.size() + 2 * min_need);
    }

    /// Block Layout: [block header][payload]
    size_t header_offset = compressed_data.size();
    BlockHeader header(max_bits, row_begin, row_end);
    compressed_data.resize(compressed_data.size() + required_bytes_total);
    std::span<char> header_span(compressed_data.data() + header_offset, BLOCK_HEADER_SIZE);
    header.write(header_span);


   // compressed_data.resize(compressed_data.size() + required_bytes);
    std::span<char> compressed_data_span(compressed_data.data() + header_offset + BLOCK_HEADER_SIZE, required_bytes);

    auto used = BlockCodec::encode(segment, max_bits, compressed_data_span);
    chassert(used == required_bytes && compressed_data_span.empty());

    last_segment.compressed_data_size += (compressed_data.size() - header_offset);

    size_t block_count = (last_segment.cardinality + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if ((block_count - 1) % 32u == 0)
    {
        static constexpr size_t SKIP_ENTRY_SIZE = sizeof(uint32_t) * 2;
        last_segment.skip_entries.emplace_back();
        last_segment.compressed_data_size += SKIP_ENTRY_SIZE;
    }

    auto & last_skip_entry = last_segment.skip_entries.back();
    last_skip_entry.row_end = row_end;
    last_skip_entry.size += (compressed_data.size() - header_offset);

    current_segment.clear();
}

void PostingListCodecImpl::decodeOneBlock(
        std::span<const std::byte> & in, size_t count, uint32_t &, std::vector<uint32_t> & current_segment)
{
    if (in.empty())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Corrupted data: expected at least {} bytes, but got {}", 1, in.size());

    BlockHeader header;
    header.read(in);

    current_segment.clear();
    current_segment.resize(count);

    /// Decode postings to buffer named temp.
    std::span<uint32_t> current_span(current_segment.data(), current_segment.size());
    BlockCodec::decode(in, count, header.bits, current_span);

    /// Restore the original array from the decompressed delta values.
    std::inclusive_scan(current_segment.begin(), current_segment.end(), current_segment.begin(), std::plus<uint32_t>{}, header.row_begin);
}

PostingListCodecSIMDComp::PostingListCodecSIMDComp()
    : IPostingListCodec(Type::Bitpacking)
{
}

void PostingListCodecSIMDComp::decode(ReadBuffer & in, PostingList & postings) const
{
    PostingListCodecImpl impl;
    impl.deserialize(in, postings);
}

void PostingListCodecSIMDComp::encode(
        const PostingListBuilder & postings, size_t posting_list_block_size, TokenPostingsInfo & info, WriteBuffer & out) const
{
    PostingListCodecImpl impl(posting_list_block_size);

    if (postings.isSmall())
    {
        const auto & small_postings = postings.getSmall();
        size_t small_size = postings.size();
        for (size_t i = 0; i < small_size; ++i)
            impl.insert(small_postings[i]);
    }
    else
    {
        std::vector<uint32_t> rowids;
        rowids.resize(postings.size());
        const auto & large_postings = postings.getLarge();
        large_postings.toUint32Array(rowids.data());
        std::span<uint32_t> values(rowids.data(), rowids.size());

        auto block_size = BLOCK_SIZE;
        while (values.size() >= block_size)
        {
            auto front = values.first(block_size);
            impl.insert(front);
            values = values.subspan(block_size);
        }

        if (!values.empty())
        {
            for (auto v : values)
                impl.insert(v);
        }
    }
    impl.serialize(out, info);
}
}

