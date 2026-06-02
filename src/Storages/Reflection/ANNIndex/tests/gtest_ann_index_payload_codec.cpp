#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/ANNIndexPayloadCodec.h>

#include <array>
#include <limits>


using namespace DB::ANNIndexPayloadCodec;

TEST(ANNIndexPayloadCodec, ChooseVersionBoundaries)
{
    /// `max_part_rows == 0` is the degenerate empty-build case; pick the
    /// cheaper width.
    EXPECT_EQ(chooseVersion(0), Version::V1_OFFSET32);

    EXPECT_EQ(chooseVersion(1), Version::V1_OFFSET32);
    EXPECT_EQ(chooseVersion(std::numeric_limits<UInt32>::max()), Version::V1_OFFSET32);

    /// First value past the UInt32 ceiling triggers V2.
    EXPECT_EQ(chooseVersion(static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1ULL),
              Version::V2_OFFSET64);
    EXPECT_EQ(chooseVersion(std::numeric_limits<UInt64>::max()), Version::V2_OFFSET64);
}

TEST(ANNIndexPayloadCodec, RecordSize)
{
    EXPECT_EQ(recordSize(Version::V1_OFFSET32), 8u);
    EXPECT_EQ(recordSize(Version::V2_OFFSET64), 12u);
}

TEST(ANNIndexPayloadCodec, V1RoundTrip)
{
    /// A handful of part_id × offset combinations including extreme but
    /// still-valid offsets (UINT32_MAX - 1; UINT32_MAX itself is reserved
    /// elsewhere as a per-row sentinel by the matcher in tombstone form,
    /// but the codec does not reserve it for offsets).
    struct Case { UInt32 part_id; UInt64 offset; };
    const std::array<Case, 6> cases = {{
        {0, 0},
        {1, 1},
        {42, 1'000'000},
        {std::numeric_limits<UInt32>::max() - 1, 0},
        {0, std::numeric_limits<UInt32>::max() - 1},
        {12345, std::numeric_limits<UInt32>::max()},
    }};

    std::array<uint8_t, recordSize(Version::V1_OFFSET32)> buf{};
    for (const auto & c : cases)
    {
        pack(buf.data(), Version::V1_OFFSET32, c.part_id, c.offset);

        UInt32 decoded_part_id = 0;
        UInt64 decoded_offset = 0;
        unpack(buf.data(), Version::V1_OFFSET32, decoded_part_id, decoded_offset);
        EXPECT_EQ(decoded_part_id, c.part_id);
        EXPECT_EQ(decoded_offset, c.offset);
    }
}

TEST(ANNIndexPayloadCodec, V2RoundTrip)
{
    /// V2 must preserve the full UInt64 offset.
    struct Case { UInt32 part_id; UInt64 offset; };
    const std::array<Case, 5> cases = {{
        {0, 0},
        {7, static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1ULL},
        {99, 1ULL << 40},
        {1, std::numeric_limits<UInt64>::max() - 1},
        {std::numeric_limits<UInt32>::max() - 1, 1ULL << 50},
    }};

    std::array<uint8_t, recordSize(Version::V2_OFFSET64)> buf{};
    for (const auto & c : cases)
    {
        pack(buf.data(), Version::V2_OFFSET64, c.part_id, c.offset);

        UInt32 decoded_part_id = 0;
        UInt64 decoded_offset = 0;
        unpack(buf.data(), Version::V2_OFFSET64, decoded_part_id, decoded_offset);
        EXPECT_EQ(decoded_part_id, c.part_id);
        EXPECT_EQ(decoded_offset, c.offset);
    }
}

TEST(ANNIndexPayloadCodec, V1OffsetSilentTruncation)
{
    /// `pack` does not validate offset fit — that is `chooseVersion`'s job.
    /// Document the truncation behaviour so a regression is caught.
    std::array<uint8_t, recordSize(Version::V1_OFFSET32)> buf{};
    const UInt64 wide_offset = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 5ULL;
    pack(buf.data(), Version::V1_OFFSET32, /*part_id=*/3, wide_offset);

    UInt32 decoded_part_id = 0;
    UInt64 decoded_offset = 0;
    unpack(buf.data(), Version::V1_OFFSET32, decoded_part_id, decoded_offset);
    EXPECT_EQ(decoded_part_id, 3u);
    EXPECT_EQ(decoded_offset, static_cast<UInt32>(wide_offset));
}

TEST(ANNIndexPayloadCodec, TombstoneSentinel)
{
    EXPECT_TRUE(isTombstone(PART_ID_TOMBSTONE));
    EXPECT_FALSE(isTombstone(0));
    EXPECT_FALSE(isTombstone(42));
    EXPECT_FALSE(isTombstone(PART_ID_TOMBSTONE - 1));
}
