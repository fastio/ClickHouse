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
    const std::vector<OffsetEntry> offsets{
        {0, 0},
        {7, 42},
        {std::numeric_limits<UInt32>::max(), static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1ULL},
        {3, OFFSET_TOMBSTONE},
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

    body.resize(11);
    ReadBufferFromString offset_in(body);
    EXPECT_THROW(readOffsetsFromBuffer(offset_in, body.size()), DB::Exception);
}
