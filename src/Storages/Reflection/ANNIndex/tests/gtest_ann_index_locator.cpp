#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/ANNIndexLocator.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>


using namespace DB;
using namespace DB::ANNIndexLocator;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

UUID uuidFromText(const String & text)
{
    UUID uuid;
    if (!tryParse(uuid, text))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad UUID literal in test");
    return uuid;
}

}

TEST(ANNIndexLocator, StableIdsRoundTripThroughBuffer)
{
    const std::vector<StableId> ids{
        {1, 10},
        {2, 20},
        {std::numeric_limits<UInt64>::max(), 42},
    };

    WriteBufferFromOwnString out;
    writeStableIdsToBuffer(ids, out);
    const auto body = out.str();

    ReadBufferFromString in(body);
    EXPECT_EQ(readStableIdsFromBuffer(in, body.size()), ids);
}

TEST(ANNIndexLocator, OffsetsRoundTripThroughBuffer)
{
    const std::vector<UInt64> offsets{
        0,
        7,
        static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1ULL,
        OFFSET_TOMBSTONE,
    };

    WriteBufferFromOwnString out;
    writeOffsetsToBuffer(offsets, out);
    const auto body = out.str();

    ReadBufferFromString in(body);
    EXPECT_EQ(readOffsetsFromBuffer(in, body.size()), offsets);
}

TEST(ANNIndexLocator, RejectsTruncatedBinaryBuffers)
{
    String body(15, '\0');
    ReadBufferFromString stable_id_in(body);
    EXPECT_THROW(readStableIdsFromBuffer(stable_id_in, body.size()), DB::Exception);

    body.resize(7);
    ReadBufferFromString offset_in(body);
    EXPECT_THROW(readOffsetsFromBuffer(offset_in, body.size()), DB::Exception);
}

TEST(ANNIndexLocator, RangeSegmentsRoundTripThroughJson)
{
    const auto uuid_a = uuidFromText("00000000-0000-0000-0000-000000000001");
    const auto uuid_b = uuidFromText("00000000-0000-0000-0000-000000000002");
    const std::vector<LocatorRangeSegment> segments{
        {0, 3, uuid_a},
        {3, 9, uuid_b},
    };

    const auto body = serializeRangeSegments(segments);
    EXPECT_EQ(parseRangeSegments(body), segments);
}

TEST(ANNIndexLocator, GraphRangeMapLookup)
{
    const auto uuid_a = uuidFromText("00000000-0000-0000-0000-000000000001");
    const auto uuid_b = uuidFromText("00000000-0000-0000-0000-000000000002");
    GraphRangeMap ranges({
        {0, 3, uuid_a},
        {3, 9, uuid_b},
    });

    EXPECT_EQ(ranges.lookup(0).target_part_uuid, uuid_a);
    EXPECT_EQ(ranges.lookup(2).target_part_uuid, uuid_a);
    EXPECT_EQ(ranges.lookup(3).target_part_uuid, uuid_b);
    EXPECT_EQ(ranges.lookup(8).target_part_uuid, uuid_b);
    EXPECT_EQ(ranges.rows(), 9u);
    EXPECT_THROW(ranges.lookup(9), DB::Exception);
}

TEST(ANNIndexLocator, RejectsInvalidRanges)
{
    const auto uuid = uuidFromText("00000000-0000-0000-0000-000000000001");

    EXPECT_THROW(validateRangeSegments({{1, 2, uuid}}), DB::Exception);
    EXPECT_THROW(validateRangeSegments({{0, 0, uuid}}), DB::Exception);
    EXPECT_THROW(validateRangeSegments({{0, 2, uuid}, {3, 4, uuid}}), DB::Exception);
}
