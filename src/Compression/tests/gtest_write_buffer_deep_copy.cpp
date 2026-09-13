#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstring>

namespace
{
using namespace DB;

class MemorySink : public BufferWithOwnMemory<WriteBuffer>
{
public:
    using BufferWithOwnMemory<WriteBuffer>::BufferWithOwnMemory;
    String data;

private:
    void nextImpl() override
    {
        data.append(buffer().begin(), offset());
    }
};

TEST(WriteBufferDeepCopy, SharedWorkspace)
{
    const auto hash = [](const String & data)
    {
        CityHash_v1_0_2::uint128 state(0, 0);
        for (size_t pos = 0; pos < data.size(); pos += 7)
            state = CityHash_v1_0_2::CityHash128WithSeed(data.data() + pos, std::min(size_t(7), data.size() - pos), state);
        return state;
    };
    for (bool compressed : {false, true})
    for (size_t target_size : {size_t(8), size_t(4096)})
    {
        SCOPED_TRACE(::testing::Message() << "compressed=" << compressed << ", target_size=" << target_size);
        MemorySink source_sink(4096), target_sink(target_size);
        HashingWriteBuffer source_inner(source_sink, 7), target_inner(target_sink, 7);
        CompressedWriteBuffer source_compressor(source_inner, nullptr, 64), target_compressor(target_inner, nullptr, target_size);
        HashingWriteBuffer source(compressed ? static_cast<WriteBuffer &>(source_compressor) : source_sink, 7);
        HashingWriteBuffer target(compressed ? static_cast<WriteBuffer &>(target_compressor) : target_sink, 7);
        String prefix;
        if (compressed)
        {
            prefix = "0123456789abcdef";
            source.write(prefix.data(), prefix.size());
            source.next();
            source_inner.next();
            ASSERT_GT(source_sink.data.size(), 7);
            ASSERT_NE(source_sink.data.size() % 7, 0);
            source.write(prefix.data(), prefix.size());
            source.next();
            prefix += prefix;
            ASSERT_GT(source_inner.offset(), 0);
        }
        source.write("fresh", 5);
        prefix += "fresh";
        ASSERT_EQ(source_sink.offset(), 0);
        ASSERT_EQ(source_compressor.offset(), 0);
        source_sink.deepCopyOwnMemoryTo(target_sink);
        target_sink.data = source_sink.data;
        memset(target_sink.buffer().begin(), 0xA5, target_sink.buffer().size());
        if (compressed)
        {
            source_inner.deepCopyTo(target_inner);
            EXPECT_EQ(target_inner.buffer().begin(), target_sink.buffer().begin());
            source_compressor.deepCopyTo(target_compressor);
            memset(target_compressor.buffer().begin(), 0xA5, target_compressor.buffer().size());
        }
        source.deepCopyTo(target);
        EXPECT_EQ(target.buffer().begin(), compressed ? target_compressor.buffer().begin() : target_sink.buffer().begin());
        EXPECT_NE(source.buffer().begin(), target.buffer().begin());
        source.write("-source", 7);
        target.write("-target", 7);
        for (bool cloned : {false, true})
        {
            auto & outer = cloned ? target : source;
            auto & inner = cloned ? target_inner : source_inner;
            auto & sink = cloned ? target_sink : source_sink;
            auto & compressor = cloned ? target_compressor : source_compressor;
            const String expected = prefix + (cloned ? "-target" : "-source");
            EXPECT_EQ(outer.getHash(), hash(expected));
            outer.finalize();
            compressor.finalize();
            const auto compressed_hash = inner.getHash();
            inner.finalize();
            sink.finalize();
            if (compressed)
            {
                EXPECT_EQ(compressed_hash, hash(sink.data));
                ReadBufferFromString input(sink.data);
                CompressedReadBuffer decoded(input);
                String actual;
                EXPECT_NO_THROW(readStringUntilEOF(actual, decoded));
                EXPECT_EQ(actual, expected);
            }
            else
                EXPECT_EQ(sink.data, expected);
        }
    }
}
}
