#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Columns/ColumnsNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <random>
#include <set>
#include <vector>

using namespace DB;
namespace fs = std::filesystem;

namespace
{

/// Build a `TokenPostingsInfo` with an embedded Roaring bitmap from sorted doc IDs.
TokenPostingsInfo makeEmbeddedInfo(const std::vector<uint32_t> & doc_ids)
{
    TokenPostingsInfo info;
    info.cardinality = static_cast<UInt32>(doc_ids.size());

    auto bitmap = std::make_shared<roaring::Roaring>();
    for (auto id : doc_ids)
        bitmap->add(id);
    info.embedded_postings = bitmap;

    if (!doc_ids.empty())
    {
        info.ranges.emplace_back(doc_ids.front(), doc_ids.back());
        info.offsets.emplace_back(); // dummy, not used for embedded
    }

    return info;
}

/// Construct an embedded cursor from a `TokenPostingsInfo`.
PostingListCursorPtr makeEmbeddedCursor(const TokenPostingsInfo & info)
{
    return std::make_shared<PostingListCursor>(info);
}

/// Generate an arithmetic sequence: {start, start+step, start+2*step, ...} of `count` elements.
std::vector<uint32_t> generateRange(uint32_t start, uint32_t count, uint32_t step = 1)
{
    std::vector<uint32_t> result;
    result.reserve(count);
    for (uint32_t i = 0; i < count; ++i)
        result.push_back(start + i * step);
    return result;
}

/// Drain all remaining doc IDs from a cursor via `next`.
std::vector<uint32_t> drainCursor(PostingListCursorPtr cursor)
{
    std::vector<uint32_t> result;
    while (cursor->valid())
    {
        result.push_back(cursor->value());
        cursor->next();
    }
    return result;
}

/// Perform `linearOr` into a buffer and return the positions that were set.
std::vector<uint32_t> linearOrToDocIds(PostingListCursorPtr cursor, size_t row_offset, size_t num_rows)
{
    std::vector<UInt8> buf(num_rows, 0);
    cursor->linearOr(buf.data(), row_offset, num_rows);
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (buf[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Perform intersection via `lazyIntersectPostingLists` and return matching doc IDs.
/// Use density_threshold=100.0 to force leapfrog, or density_threshold=0.0 to force brute-force.
std::vector<uint32_t> intersectAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows,
    float density_threshold = 100.0f)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyIntersectPostingLists(*col, postings, tokens, 0, row_offset, num_rows, density_threshold);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Perform union via `lazyUnionPostingLists` and return matching doc IDs.
std::vector<uint32_t> unionAndCollect(
    PostingListCursorMap & postings,
    const std::vector<String> & tokens,
    size_t row_offset,
    size_t num_rows)
{
    auto col = ColumnUInt8::create(num_rows, UInt8(0));
    lazyUnionPostingLists(*col, postings, tokens, 0, row_offset, num_rows);
    const auto & data = col->getData();
    std::vector<uint32_t> result;
    for (size_t i = 0; i < num_rows; ++i)
        if (data[i])
            result.push_back(static_cast<uint32_t>(row_offset + i));
    return result;
}

/// Result of building compressed posting list test data.
struct CompressedTestData
{
    std::string buffer;                   /// The serialized .pst-like data
    TokenPostingsInfo info;               /// Populated TokenPostingsInfo (with HasBlockIndex flag)
    std::vector<uint32_t> all_docs;       /// All doc IDs in order (for verification)

    /// Temp directory and stream must outlive the cursor.
    fs::path tmp_dir;
    std::shared_ptr<MergeTreeReaderStreamSingleColumnWholePart> stream;
};

/// Build compressed posting list test data from a vector of sorted doc IDs.
/// Uses PostingListCodecBitpackingImpl to encode, which produces V2 format
/// (with Index Section / HasBlockIndex flag).
///
/// @param doc_ids       Sorted doc IDs to encode.
/// @param segment_size  Max row IDs per segment (controls multi-segment splitting).
///                      Use a large value (e.g., 1<<20) for single-segment tests.
CompressedTestData makeCompressedData(
    const std::vector<uint32_t> & doc_ids,
    size_t segment_size = 1 << 20)
{
    CompressedTestData result;
    result.all_docs = doc_ids;

    if (doc_ids.empty())
        return result;

    /// Encode using the production codec.
    PostingListCodecBitpackingImpl codec(segment_size);
    for (auto id : doc_ids)
        codec.insert(id);

    WriteBufferFromOwnString wb;
    codec.encode(wb, result.info);
    result.buffer = wb.str();

    /// Set the V2 flag so PostingListCursor reads the Index Section.
    result.info.header = PostingsSerialization::Flags::IsCompressed
                       | PostingsSerialization::Flags::HasBlockIndex;

    /// The codec's serializeTo fills offsets/ranges but not cardinality —
    /// in production that is set by the dictionary layer. Set it here for tests.
    result.info.cardinality = static_cast<UInt32>(doc_ids.size());

    return result;
}

/// Create a PostingListCursor backed by a real MergeTreeReaderStream for compressed data.
/// Writes the encoded buffer to a temporary file on DiskLocal, constructs
/// MergeTreeReaderStreamSingleColumnWholePart (uncompressed mode), and returns a cursor.
///
/// The returned CompressedTestData::stream must outlive the cursor.
PostingListCursorPtr makeCompressedCursor(CompressedTestData & data)
{
    /// Create a unique temp directory.
    data.tmp_dir = fs::temp_directory_path() / ("gtest_plc_" + std::to_string(reinterpret_cast<uintptr_t>(&data)));
    fs::create_directories(data.tmp_dir / "part");

    /// Write the binary buffer to a .pst file.
    {
        auto out_path = data.tmp_dir / "part" / "stream.pst";
        std::ofstream ofs(out_path, std::ios::binary);
        ofs.write(data.buffer.data(), static_cast<std::streamsize>(data.buffer.size()));
        ofs.close();
    }

    /// Construct the stream infrastructure.
    auto disk = std::make_shared<DiskLocal>("test_disk", data.tmp_dir.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("test_vol", disk);
    auto storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "part");

    auto settings = MergeTreeReaderSettings::createFromSettings();
    settings.is_compressed = false;

    static constexpr size_t marks_count = 1;
    data.stream = std::make_shared<MergeTreeReaderStreamSingleColumnWholePart>(
        std::static_pointer_cast<const IDataPartStorage>(storage),
        "stream",
        ".pst",
        marks_count,
        MarkRanges{{0, marks_count}},
        settings,
        /*uncompressed_cache=*/nullptr,
        data.buffer.size(),
        /*marks_loader=*/nullptr,
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);

    /// Trigger lazy initialization of the underlying read buffer.
    /// PostingListCursor::prepareSegment calls seekToMark(MarkInCompressedFile)
    /// which requires the plain_file_buffer to be already initialized.
    data.stream->getDataBuffer();

    return std::make_shared<PostingListCursor>(*data.stream, data.info);
}

/// Helper: seek to first doc, then drain all remaining doc IDs via next.
std::vector<uint32_t> advanceAndDrainCursor(PostingListCursorPtr cursor, uint32_t first_doc)
{
    cursor->advance(first_doc);
    std::vector<uint32_t> result;
    while (cursor->valid())
    {
        result.push_back(cursor->value());
        cursor->next();
    }
    return result;
}

/// Cleanup temp directories created by makeCompressedCursor.
struct CompressedTestDataCleanup
{
    CompressedTestData & data;
    ~CompressedTestDataCleanup()
    {
        data.stream.reset();
        if (!data.tmp_dir.empty())
            fs::remove_all(data.tmp_dir);
    }
};

} // anonymous namespace


// =============================================================================
// Section 1: Basic Embedded Cursor
// =============================================================================

TEST(PostingListCursorTest, EmptyEmbeddedCursor)
{
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, SingleDocEmbedded)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);

    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 42u);
    EXPECT_EQ(cursor->cardinality(), 1u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, MultipleDocsSequentialIteration)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, LargeEmbeddedCursor)
{
    auto docs = generateRange(0, 6);
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, SparseEmbeddedCursor)
{
    auto docs = generateRange(100, 5, 100); // 100, 200, 300, 400, 500
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto drained = drainCursor(cursor);
    EXPECT_EQ(drained, docs);
}

TEST(PostingListCursorTest, NextAfterInvalidIsNoop)
{
    auto info = makeEmbeddedInfo({5});
    auto cursor = makeEmbeddedCursor(info);
    cursor->next();
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // should not cause exception
    EXPECT_FALSE(cursor->valid());
    cursor->next(); // multiple calls after invalid
    EXPECT_FALSE(cursor->valid());
}


// =============================================================================
// Section 2: Advance Operations
// =============================================================================

TEST(PostingListCursorTest, AdvanceToExactValue)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, AdvanceToNonExistentGoesToNext)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);
}

TEST(PostingListCursorTest, AdvanceBeyondLastInvalidates)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(31);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, AdvanceToFirstDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);
}

TEST(PostingListCursorTest, AdvanceToLastDoc)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(30);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, AdvanceToZero)
{
    std::vector<uint32_t> docs = {0, 5, 10};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(0);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 0u);
}

TEST(PostingListCursorTest, AdvanceBeforeFirst)
{
    std::vector<uint32_t> docs = {100, 200, 300};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);
}

TEST(PostingListCursorTest, AdvanceProgressivelyForward)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50, 60};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(25);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 30u);

    cursor->advance(55);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 60u);

    cursor->advance(61);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, AdvanceThenNext)
{
    std::vector<uint32_t> docs = {5, 10, 15, 20, 25, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(15);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 15u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 20u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 25u);
}


// =============================================================================
// Section 3: Density and Cardinality
// =============================================================================

TEST(PostingListCursorTest, CardinalityReflectsDocCount)
{
    auto info = makeEmbeddedInfo({1, 2, 3, 4, 5});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_EQ(cursor->cardinality(), 5u);
}

TEST(PostingListCursorTest, DensityPerfectlyDense)
{
    auto docs = generateRange(0, 5); // 0, 1, 2, 3, 4
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}

TEST(PostingListCursorTest, DensitySparse)
{
    std::vector<uint32_t> docs = {0, 100};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_NEAR(cursor->density(), 2.0 / 101.0, 1e-6);
}

TEST(PostingListCursorTest, DensitySingleDoc)
{
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}


// =============================================================================
// Section 4: linearOr
// =============================================================================

TEST(PostingListCursorTest, LinearOrFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 60);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, LinearOrPartialRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 15, 30); // rows [15, 45)
    std::vector<uint32_t> expected = {20, 30, 40};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, LinearOrNoOverlap)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 100, 50);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, LinearOrSingleRowMatch)
{
    std::vector<uint32_t> docs = {42};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 42, 1);
    EXPECT_EQ(result, std::vector<uint32_t>{42});
}

TEST(PostingListCursorTest, LinearOrDenseRange)
{
    auto docs = generateRange(100, 6); // 100..105
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 110);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 5: linearAnd
// =============================================================================

TEST(PostingListCursorTest, LinearAndFullRange)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(60, 0);
    cursor->linearAnd(buf.data(), 0, 60);

    for (auto d : docs)
        EXPECT_EQ(buf[d], 1u) << "Expected buf[" << d << "] == 1";
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[15], 0u);
}

TEST(PostingListCursorTest, LinearAndIncrementsExisting)
{
    std::vector<uint32_t> docs = {10, 20, 30};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(40, 1);
    cursor->linearAnd(buf.data(), 0, 40);

    EXPECT_EQ(buf[10], 2u);
    EXPECT_EQ(buf[20], 2u);
    EXPECT_EQ(buf[30], 2u);
    EXPECT_EQ(buf[0], 1u);
    EXPECT_EQ(buf[5], 1u);
}


// =============================================================================
// Section 6: Two-cursor Intersection (leapfrog)
// =============================================================================

TEST(PostingListCursorTest, IntersectTwoIdentical)
{
    std::vector<uint32_t> docs = {10, 20, 30, 40, 50};
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 60, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectTwoDisjoint)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({15, 25, 35});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 50, 100.0f);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectTwoPartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60, 70});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 80, 100.0f);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectTwoSingleCommon)
{
    auto info1 = makeEmbeddedInfo({1, 50, 100});
    auto info2 = makeEmbeddedInfo({49, 50, 51});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 150, 100.0f);
    EXPECT_EQ(result, std::vector<uint32_t>{50});
}


// =============================================================================
// Section 7: Three-cursor Intersection
// =============================================================================

TEST(PostingListCursorTest, IntersectThreeAllMatch)
{
    auto docs = generateRange(0, 5, 10); // 0, 10, 20, 30, 40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectThreePartialOverlap)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50, 60});
    auto info2 = makeEmbeddedInfo({15, 20, 30, 45, 60});
    auto info3 = makeEmbeddedInfo({20, 25, 30, 55, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 70, 100.0f);
    std::vector<uint32_t> expected = {20, 30, 60};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectThreeNoCommon)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30});
    auto info2 = makeEmbeddedInfo({11, 21, 31});
    auto info3 = makeEmbeddedInfo({12, 22, 32});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = intersectAndCollect(postings, {"a", "b", "c"}, 0, 50, 100.0f);
    EXPECT_TRUE(result.empty());
}


// =============================================================================
// Section 8: Four-cursor Intersection
// =============================================================================

TEST(PostingListCursorTest, IntersectFourAllOverlap)
{
    auto docs = generateRange(0, 5, 10); // 0, 10, 20, 30, 40
    auto info1 = makeEmbeddedInfo(docs);
    auto info2 = makeEmbeddedInfo(docs);
    auto info3 = makeEmbeddedInfo(docs);
    auto info4 = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, IntersectFourMixedSelectivity)
{
    // LCM(2, 3, 5, 7) = 210 — all sequences must cover at least [0, 210].
    auto docs1 = generateRange(0, 106, 2);  // every 2nd: 0, 2, ..., 210
    auto docs2 = generateRange(0, 71, 3);   // every 3rd: 0, 3, ..., 210
    auto docs3 = generateRange(0, 43, 5);   // every 5th: 0, 5, ..., 210
    auto docs4 = generateRange(0, 31, 7);   // every 7th: 0, 7, ..., 210

    auto info1 = makeEmbeddedInfo(docs1);
    auto info2 = makeEmbeddedInfo(docs2);
    auto info3 = makeEmbeddedInfo(docs3);
    auto info4 = makeEmbeddedInfo(docs4);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d"}, 0, 220, 100.0f);

    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 220; i += 210)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 9: Five+ Cursors (linear / heap leapfrog)
// =============================================================================

TEST(PostingListCursorTest, IntersectFiveCursors)
{
    // Primes: 2, 3, 5, 7, 11 -> LCM = 2310
    auto docs1 = generateRange(0, 50, 2);
    auto docs2 = generateRange(0, 34, 3);
    auto docs3 = generateRange(0, 20, 5);
    auto docs4 = generateRange(0, 15, 7);
    auto docs5 = generateRange(0, 10, 11);

    auto info1 = makeEmbeddedInfo(docs1);
    auto info2 = makeEmbeddedInfo(docs2);
    auto info3 = makeEmbeddedInfo(docs3);
    auto info4 = makeEmbeddedInfo(docs4);
    auto info5 = makeEmbeddedInfo(docs5);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);
    postings["d"] = makeEmbeddedCursor(info4);
    postings["e"] = makeEmbeddedCursor(info5);

    auto result = intersectAndCollect(postings, {"a", "b", "c", "d", "e"}, 0, 120, 100.0f);
    // Only 0 is common (LCM = 2310, next would be 2310 which is >= 120)
    EXPECT_EQ(result, std::vector<uint32_t>{0});
}

TEST(PostingListCursorTest, IntersectNineCursorsHeap)
{
    // Nine identical cursors — triggers heap leapfrog (n > 8)
    auto docs = generateRange(0, 3, 20); // 0, 20, 40
    std::vector<TokenPostingsInfo> infos(9);
    PostingListCursorMap postings;
    std::vector<String> tokens;

    for (int i = 0; i < 9; ++i)
    {
        infos[i] = makeEmbeddedInfo(docs);
        String name = "t" + std::to_string(i);
        tokens.push_back(name);
    }
    for (int i = 0; i < 9; ++i)
        postings[tokens[i]] = makeEmbeddedCursor(infos[i]);

    auto result = intersectAndCollect(postings, tokens, 0, 50, 100.0f);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 10: Brute-force Intersection
// =============================================================================

TEST(PostingListCursorTest, BruteForceIntersectionDense)
{
    // Dense postings with low density threshold force brute-force path
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40, 50});
    auto info2 = makeEmbeddedInfo({20, 30, 60});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // density_threshold = 0.0 forces brute-force path (min_density >= 0)
    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 70, 0.0f);
    std::vector<uint32_t> expected = {20, 30};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, BruteForceVsLeapfrogConsistency)
{
    std::mt19937 rng(42); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    std::uniform_int_distribution<uint32_t> dist(0, 99);

    for (int trial = 0; trial < 10; ++trial)
    {
        std::vector<TokenPostingsInfo> infos_bf(3);
        std::vector<TokenPostingsInfo> infos_lf(3);
        std::vector<std::vector<uint32_t>> all_docs(3);

        for (int i = 0; i < 3; ++i)
        {
            std::set<uint32_t> s;
            size_t count = 10 + trial * 5;
            while (s.size() < count)
                s.insert(dist(rng));
            all_docs[i].assign(s.begin(), s.end());
            infos_bf[i] = makeEmbeddedInfo(all_docs[i]);
            infos_lf[i] = makeEmbeddedInfo(all_docs[i]);
        }

        // Brute force (density_threshold = 0.0)
        PostingListCursorMap postings_bf;
        postings_bf["a"] = makeEmbeddedCursor(infos_bf[0]);
        postings_bf["b"] = makeEmbeddedCursor(infos_bf[1]);
        postings_bf["c"] = makeEmbeddedCursor(infos_bf[2]);
        auto bf_result = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, 100, 0.0f);

        // Leapfrog (density_threshold = 100.0 to force leapfrog)
        PostingListCursorMap postings_lf;
        postings_lf["a"] = makeEmbeddedCursor(infos_lf[0]);
        postings_lf["b"] = makeEmbeddedCursor(infos_lf[1]);
        postings_lf["c"] = makeEmbeddedCursor(infos_lf[2]);
        auto lf_result = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, 100, 100.0f);

        EXPECT_EQ(bf_result, lf_result) << "Brute-force vs leapfrog mismatch at trial " << trial;
    }
}


// =============================================================================
// Section 11: Union Operations
// =============================================================================

TEST(PostingListCursorTest, UnionTwoOverlapping)
{
    auto info1 = makeEmbeddedInfo({10, 20, 30, 40});
    auto info2 = makeEmbeddedInfo({20, 30, 50});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 60);
    std::vector<uint32_t> expected = {10, 20, 30, 40, 50};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionThreeDisjoint)
{
    auto info1 = makeEmbeddedInfo({1, 4, 7});
    auto info2 = makeEmbeddedInfo({2, 5, 8});
    auto info3 = makeEmbeddedInfo({3, 6, 9});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);
    postings["c"] = makeEmbeddedCursor(info3);

    auto result = unionAndCollect(postings, {"a", "b", "c"}, 0, 10);
    std::vector<uint32_t> expected = {1, 2, 3, 4, 5, 6, 7, 8, 9};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, UnionSingleCursor)
{
    std::vector<uint32_t> docs = {5, 15, 25};
    auto info = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = unionAndCollect(postings, {"a"}, 0, 30);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, UnionEmptyMap)
{
    PostingListCursorMap postings;
    auto result = unionAndCollect(postings, {}, 0, 100);
    EXPECT_TRUE(result.empty());
}


// =============================================================================
// Section 12: Edge Cases
// =============================================================================

TEST(PostingListCursorTest, AdvanceOnInvalidCursorIsNoop)
{
    auto info = makeEmbeddedInfo({10, 20});
    auto cursor = makeEmbeddedCursor(info);

    cursor->advance(30); // beyond range, invalidates
    EXPECT_FALSE(cursor->valid());

    cursor->advance(10); // advance on invalid cursor
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, LinearOrOnEmbeddedWithOffset)
{
    std::vector<uint32_t> docs = {100, 200, 300, 400, 500};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    // Window [200, 400) with row_offset=200, num_rows=200
    auto result = linearOrToDocIds(cursor, 200, 200);
    std::vector<uint32_t> expected = {200, 300};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectWithMissingToken)
{
    auto info = makeEmbeddedInfo({1, 2, 3});

    PostingListCursorMap postings;
    postings["exists"] = makeEmbeddedCursor(info);

    // "missing" token is not in map. Only 1 cursor found, n=1 path triggers `linearOr`.
    auto result = intersectAndCollect(postings, {"exists", "missing"}, 0, 10, 100.0f);
    std::vector<uint32_t> expected = {1, 2, 3};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectWithRowOffset)
{
    auto info1 = makeEmbeddedInfo({100, 200, 300, 400, 500});
    auto info2 = makeEmbeddedInfo({100, 200, 300, 400, 500});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info1);
    postings["b"] = makeEmbeddedCursor(info2);

    // Only look at rows [200, 400)
    auto result = intersectAndCollect(postings, {"a", "b"}, 200, 200, 100.0f);
    std::vector<uint32_t> expected = {200, 300};
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectZeroCursors)
{
    PostingListCursorMap postings;
    auto result = intersectAndCollect(postings, {}, 0, 100, 100.0f);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectSingleCursor)
{
    auto info = makeEmbeddedInfo({5, 10, 15, 20, 25});

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info);

    auto result = intersectAndCollect(postings, {"a"}, 0, 30, 100.0f);
    std::vector<uint32_t> expected = {5, 10, 15, 20, 25};
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 13: Extreme Selectivity and Large-scale Consistency
// =============================================================================

TEST(PostingListCursorTest, IntersectExtremeSelectivityDifference)
{
    // Cursor A: every 3rd doc (40 docs), Cursor B: every 30th doc (4 docs)
    // over range [0, 120). Selectivity ratio 10x. Intersection = multiples of LCM(3, 30) = 30.
    const uint32_t range = 120;
    std::vector<uint32_t> docs_a;
    std::vector<uint32_t> docs_b;
    std::vector<uint32_t> expected;

    for (uint32_t i = 0; i < range; i += 3)
        docs_a.push_back(i);
    for (uint32_t i = 0; i < range; i += 30)
        docs_b.push_back(i);
    for (uint32_t i = 0; i < range; i += 30)
        expected.push_back(i);

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["dense"] = makeEmbeddedCursor(info_a);
    postings["ultrarare"] = makeEmbeddedCursor(info_b);

    auto result = intersectAndCollect(postings, {"dense", "ultrarare"}, 0, range, 100.0f);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectFourExtremeSelectivity)
{
    // Cursors: every 2 / every 5 / every 10 / every 30
    // LCM(2, 5, 10, 30) = 30
    const uint32_t range = 120;
    std::vector<uint32_t> docs_2;
    std::vector<uint32_t> docs_5;
    std::vector<uint32_t> docs_10;
    std::vector<uint32_t> docs_30;
    std::vector<uint32_t> expected;

    for (uint32_t i = 0; i < range; i += 2)
        docs_2.push_back(i);
    for (uint32_t i = 0; i < range; i += 5)
        docs_5.push_back(i);
    for (uint32_t i = 0; i < range; i += 10)
        docs_10.push_back(i);
    for (uint32_t i = 0; i < range; i += 30)
        docs_30.push_back(i);
    for (uint32_t i = 0; i < range; i += 30)
        expected.push_back(i);

    auto info_2 = makeEmbeddedInfo(docs_2);
    auto info_5 = makeEmbeddedInfo(docs_5);
    auto info_10 = makeEmbeddedInfo(docs_10);
    auto info_30 = makeEmbeddedInfo(docs_30);

    PostingListCursorMap postings;
    postings["dense"] = makeEmbeddedCursor(info_2);
    postings["medium"] = makeEmbeddedCursor(info_5);
    postings["rare"] = makeEmbeddedCursor(info_10);
    postings["ultrarare"] = makeEmbeddedCursor(info_30);

    auto result = intersectAndCollect(postings, {"dense", "medium", "rare", "ultrarare"}, 0, range, 100.0f);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, BruteForceHighDensity)
{
    // Two 90%+ density cursors. With density_threshold = 0.5, should take brute-force path.
    const uint32_t range = 100;
    std::vector<uint32_t> docs_a;
    std::vector<uint32_t> docs_b;
    std::vector<uint32_t> expected;

    // Cursor A: every doc except multiples of 10 (90% density)
    for (uint32_t i = 0; i < range; ++i)
        if (i % 10 != 0)
            docs_a.push_back(i);

    // Cursor B: every doc except multiples of 7 (≈85.7% density)
    for (uint32_t i = 0; i < range; ++i)
        if (i % 7 != 0)
            docs_b.push_back(i);

    // Expected: docs present in both
    for (uint32_t i = 0; i < range; ++i)
        if (i % 10 != 0 && i % 7 != 0)
            expected.push_back(i);

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info_a);
    postings["b"] = makeEmbeddedCursor(info_b);

    // density_threshold = 0.5 → both cursors have density > 0.5 → brute-force
    auto result = intersectAndCollect(postings, {"a", "b"}, 0, range, 0.5f);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, LeapfrogVsBruteForceConsistencyLargeScale)
{
    // 120-doc range with multiple selectivities. Compare threshold=100 (force leapfrog)
    // vs threshold=0 (force brute-force) for result consistency.
    const uint32_t range = 120;

    auto make_docs = [&](uint32_t step)
    {
        std::vector<uint32_t> docs;
        for (uint32_t i = 0; i < range; i += step)
            docs.push_back(i);
        return docs;
    };

    auto docs_a = make_docs(3);
    auto docs_b = make_docs(7);
    auto docs_c = make_docs(13);

    // Leapfrog pass (threshold = 100.0)
    {
        auto info_a = makeEmbeddedInfo(docs_a);
        auto info_b = makeEmbeddedInfo(docs_b);
        auto info_c = makeEmbeddedInfo(docs_c);

        PostingListCursorMap postings_lf;
        postings_lf["a"] = makeEmbeddedCursor(info_a);
        postings_lf["b"] = makeEmbeddedCursor(info_b);
        postings_lf["c"] = makeEmbeddedCursor(info_c);
        auto lf_result = intersectAndCollect(postings_lf, {"a", "b", "c"}, 0, range, 100.0f);

        // Brute-force pass (threshold = 0.0)
        auto info_a2 = makeEmbeddedInfo(docs_a);
        auto info_b2 = makeEmbeddedInfo(docs_b);
        auto info_c2 = makeEmbeddedInfo(docs_c);

        PostingListCursorMap postings_bf;
        postings_bf["a"] = makeEmbeddedCursor(info_a2);
        postings_bf["b"] = makeEmbeddedCursor(info_b2);
        postings_bf["c"] = makeEmbeddedCursor(info_c2);
        auto bf_result = intersectAndCollect(postings_bf, {"a", "b", "c"}, 0, range, 0.0f);

        EXPECT_EQ(lf_result, bf_result);

        // Verify against ground truth: LCM(3, 7, 13) = 273
        std::vector<uint32_t> expected;
        for (uint32_t i = 0; i < range; i += 273)
            expected.push_back(i);
        EXPECT_EQ(lf_result, expected);
    }
}


// =============================================================================
// Section 14: linearOr Skip Optimizations
// =============================================================================

TEST(PostingListCursorTest, DenseEmbeddedFullCoverage)
{
    /// A dense embedded cursor (all rows present) should trigger the Level 1 memset path.
    auto docs = generateRange(0, 100); // 0..99, density = 1.0
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 100);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, DenseEmbeddedWithRowClipping)
{
    /// Dense cursor with row clipping: memset should only cover the clipped range.
    auto docs = generateRange(10, 50); // 10..59
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    /// Window [20, 50) — only rows 20..49 should be set.
    auto result = linearOrToDocIds(cursor, 20, 30);
    auto expected = generateRange(20, 30); // 20..49
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, SparseEmbeddedNoFalseSkip)
{
    /// A sparse cursor must NOT trigger the dense memset path —
    /// only the actual doc IDs should appear in output.
    std::vector<uint32_t> docs = {0, 10, 20, 30, 40};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    auto result = linearOrToDocIds(cursor, 0, 50);
    EXPECT_EQ(result, docs);

    /// Verify positions that should NOT be set.
    std::vector<UInt8> buf(50, 0);
    auto cursor2_info = makeEmbeddedInfo(docs);
    auto cursor2 = makeEmbeddedCursor(cursor2_info);
    cursor2->linearOr(buf.data(), 0, 50);
    EXPECT_EQ(buf[1], 0);
    EXPECT_EQ(buf[5], 0);
    EXPECT_EQ(buf[15], 0);
}

TEST(PostingListCursorTest, MultiCursorUnionCoverageSkip)
{
    /// Two cursors in union: the first (dense) fills the output buffer completely,
    /// so the second cursor should effectively be a no-op.
    auto dense_docs = generateRange(0, 100); // 0..99, dense
    auto sparse_docs = std::vector<uint32_t>{10, 50, 90};

    auto info_dense = makeEmbeddedInfo(dense_docs);
    auto info_sparse = makeEmbeddedInfo(sparse_docs);

    PostingListCursorMap postings;
    postings["dense"] = makeEmbeddedCursor(info_dense);
    postings["sparse"] = makeEmbeddedCursor(info_sparse);

    auto result = unionAndCollect(postings, {"dense", "sparse"}, 0, 100);
    EXPECT_EQ(result, dense_docs);
}

TEST(PostingListCursorTest, MultiCursorPartialOverlap)
{
    /// Two cursors with partial overlap: union should contain all unique doc IDs.
    /// This tests that block-level skipping does not lose data when the overlap is partial.
    auto docs_a = generateRange(0, 50);    // 0..49
    auto docs_b = generateRange(25, 50);   // 25..74

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info_a);
    postings["b"] = makeEmbeddedCursor(info_b);

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 75);
    auto expected = generateRange(0, 75); // 0..74 — full union
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 15: linearAnd Dense Shortcut
// =============================================================================

TEST(PostingListCursorTest, LinearAndDenseMemsetShortcut)
{
    /// Dense cursor (cardinality == range span) should trigger the dense shortcut:
    /// increment entire clipped region without binary search.
    auto docs = generateRange(10, 50); // 10..59, density = 1.0
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(80, 0);
    cursor->linearAnd(buf.data(), 0, 80);

    /// Rows 10..59 should be incremented to 1.
    for (size_t i = 10; i < 60; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
    /// Rows outside should remain 0.
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[9], 0u);
    EXPECT_EQ(buf[60], 0u);
    EXPECT_EQ(buf[79], 0u);
}

TEST(PostingListCursorTest, LinearAndDenseWithClipping)
{
    /// Dense cursor with row window smaller than the posting range.
    /// Only the clipped portion should be incremented.
    auto docs = generateRange(0, 100); // 0..99
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    /// Window [30, 70).
    std::vector<UInt8> buf(40, 0);
    cursor->linearAnd(buf.data(), 30, 40);

    for (size_t i = 0; i < 40; ++i)
        EXPECT_EQ(buf[i], 1u) << "offset " << i;
}

TEST(PostingListCursorTest, LinearAndDenseIncrementsPriorValues)
{
    /// Dense cursor applied to a buffer with pre-existing values of 2.
    /// Dense shortcut should increment each to 3.
    auto docs = generateRange(0, 50); // 0..49
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(50, 2);
    cursor->linearAnd(buf.data(), 0, 50);

    for (size_t i = 0; i < 50; ++i)
        EXPECT_EQ(buf[i], 3u) << "row " << i;
}

TEST(PostingListCursorTest, LinearAndSparseNoDenseShortcut)
{
    /// Sparse cursor must NOT trigger the dense shortcut — only matching positions increment.
    std::vector<uint32_t> docs = {5, 15, 25, 35, 45};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(50, 1);
    cursor->linearAnd(buf.data(), 0, 50);

    for (auto d : docs)
        EXPECT_EQ(buf[d], 2u) << "doc " << d;
    EXPECT_EQ(buf[0], 1u);
    EXPECT_EQ(buf[10], 1u);
    EXPECT_EQ(buf[49], 1u);
}

TEST(PostingListCursorTest, LinearAndPartialRange)
{
    /// linearAnd with row window that clips the posting list on both sides.
    std::vector<uint32_t> docs = {5, 10, 15, 20, 25, 30, 35};
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    /// Window [12, 32) → only docs 15, 20, 25, 30 are in range.
    std::vector<UInt8> buf(20, 0);
    cursor->linearAnd(buf.data(), 12, 20);

    EXPECT_EQ(buf[15 - 12], 1u);
    EXPECT_EQ(buf[20 - 12], 1u);
    EXPECT_EQ(buf[25 - 12], 1u);
    EXPECT_EQ(buf[30 - 12], 1u);
    /// Docs 5 and 10 are before the window, 35 is after.
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[19], 0u);
}


// =============================================================================
// Section 16: linearOr Dense Shortcut for Non-zero Offset
// =============================================================================

TEST(PostingListCursorTest, LinearOrDenseNonZeroOffset)
{
    /// Dense cursor with non-zero row_offset should memset only the overlapping range.
    auto docs = generateRange(100, 50); // 100..149
    auto info = makeEmbeddedInfo(docs);
    auto cursor = makeEmbeddedCursor(info);

    /// Window [90, 160) — wider than the posting range on both sides.
    std::vector<UInt8> buf(70, 0);
    cursor->linearOr(buf.data(), 90, 70);

    /// Rows 90..99 should remain 0 (before posting range).
    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ(buf[i], 0u) << "offset " << i;
    /// Rows 100..149 should be set.
    for (size_t i = 10; i < 60; ++i)
        EXPECT_EQ(buf[i], 1u) << "offset " << i;
    /// Rows 150..159 should remain 0 (after posting range).
    for (size_t i = 60; i < 70; ++i)
        EXPECT_EQ(buf[i], 0u) << "offset " << i;
}


// =============================================================================
// Section 17: Intersection with Non-zero Row Offset
// =============================================================================

TEST(PostingListCursorTest, IntersectTwoWithRowOffset)
{
    /// Intersection with row_offset > 0: only docs within [row_offset, row_offset + num_rows) count.
    auto docs_a = generateRange(0, 100, 3);  // 0, 3, 6, ..., 297
    auto docs_b = generateRange(0, 100, 5);  // 0, 5, 10, ..., 495

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info_a);
    postings["b"] = makeEmbeddedCursor(info_b);

    /// Window [50, 150). Intersection = multiples of LCM(3,5)=15 in [50, 150).
    auto result = intersectAndCollect(postings, {"a", "b"}, 50, 100, 100.0f);
    std::vector<uint32_t> expected;
    for (uint32_t i = 60; i < 150; i += 15)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, IntersectBruteForceWithRowOffset)
{
    /// Same as above but force brute-force with density_threshold=0.0.
    auto docs_a = generateRange(0, 100, 3);
    auto docs_b = generateRange(0, 100, 5);

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info_a);
    postings["b"] = makeEmbeddedCursor(info_b);

    auto result = intersectAndCollect(postings, {"a", "b"}, 50, 100, 0.0f);
    std::vector<uint32_t> expected;
    for (uint32_t i = 60; i < 150; i += 15)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 18: Union with Non-zero Row Offset
// =============================================================================

TEST(PostingListCursorTest, UnionWithRowOffset)
{
    /// Union with row_offset clips the result to the window.
    std::vector<uint32_t> docs_a = {10, 20, 30, 40, 50};
    std::vector<uint32_t> docs_b = {25, 35, 45, 55};

    auto info_a = makeEmbeddedInfo(docs_a);
    auto info_b = makeEmbeddedInfo(docs_b);

    PostingListCursorMap postings;
    postings["a"] = makeEmbeddedCursor(info_a);
    postings["b"] = makeEmbeddedCursor(info_b);

    /// Window [20, 50). Only 20, 25, 30, 35, 40, 45 are in range.
    auto result = unionAndCollect(postings, {"a", "b"}, 20, 30);
    std::vector<uint32_t> expected = {20, 25, 30, 35, 40, 45};
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 19: Leapfrog 6-7-8 Cursors (intersectLeapfrogLinear boundary)
// =============================================================================

TEST(PostingListCursorTest, IntersectSixCursorsLinear)
{
    /// Six cursors: exercises the linear leapfrog (5 <= n <= 8) with more cursors.
    /// Step pattern: 2, 3, 5, 7, 11, 13. LCM(2,3,5,7,11,13) = 30030.
    /// Over [0, 128) the only common value is 0.
    const uint32_t range = 128;
    const std::vector<uint32_t> steps = {2, 3, 5, 7, 11, 13};
    std::vector<String> tokens = {"c0", "c1", "c2", "c3", "c4", "c5"};
    std::vector<TokenPostingsInfo> infos(steps.size());
    PostingListCursorMap postings;

    for (size_t c = 0; c < steps.size(); ++c)
    {
        std::vector<uint32_t> docs;
        for (uint32_t i = 0; i < range; i += steps[c])
            docs.push_back(i);
        infos[c] = makeEmbeddedInfo(docs);
    }
    for (size_t c = 0; c < steps.size(); ++c)
        postings[tokens[c]] = makeEmbeddedCursor(infos[c]);

    auto result = intersectAndCollect(postings, tokens, 0, range, 100.0f);
    /// LCM = 30030 > 128, so only doc 0 is in the intersection.
    EXPECT_EQ(result, std::vector<uint32_t>{0});
}

TEST(PostingListCursorTest, IntersectEightCursorsLinear)
{
    /// Eight cursors: boundary of linear leapfrog (n == 8).
    /// All cursors have even numbers, intersection = even numbers.
    const uint32_t range = 100;
    std::vector<String> tokens = {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"};
    std::vector<TokenPostingsInfo> infos(8);
    PostingListCursorMap postings;

    auto even_docs = generateRange(0, 50, 2); // 0, 2, 4, ..., 98
    for (size_t c = 0; c < 8; ++c)
        infos[c] = makeEmbeddedInfo(even_docs);
    for (size_t c = 0; c < 8; ++c)
        postings[tokens[c]] = makeEmbeddedCursor(infos[c]);

    auto result = intersectAndCollect(postings, tokens, 0, range, 100.0f);
    EXPECT_EQ(result, even_docs);
}


// =============================================================================
// Section 20: Brute-force Intersection with Many Cursors
// =============================================================================

TEST(PostingListCursorTest, BruteForceIntersectFiveCursors)
{
    /// Five cursors all identical — brute-force should find all docs.
    auto docs = generateRange(0, 50, 3); // 0, 3, 6, ..., 147
    std::vector<String> tokens = {"c0", "c1", "c2", "c3", "c4"};
    std::vector<TokenPostingsInfo> infos(5);
    PostingListCursorMap postings;

    for (size_t c = 0; c < 5; ++c)
        infos[c] = makeEmbeddedInfo(docs);
    for (size_t c = 0; c < 5; ++c)
        postings[tokens[c]] = makeEmbeddedCursor(infos[c]);

    auto result = intersectAndCollect(postings, tokens, 0, 150, 0.0f);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, BruteForceVsLeapfrogSixCursors)
{
    /// Six cursors with different selectivities. Verify brute-force matches leapfrog.
    const uint32_t range = 128;
    const std::vector<uint32_t> steps = {2, 3, 4, 6, 8, 12};
    const std::vector<String> token_names = {"c0", "c1", "c2", "c3", "c4", "c5"};

    auto make_postings = [&](std::vector<TokenPostingsInfo> & infos)
    {
        PostingListCursorMap postings;
        infos.resize(steps.size());
        for (size_t c = 0; c < steps.size(); ++c)
        {
            std::vector<uint32_t> docs;
            for (uint32_t i = 0; i < range; i += steps[c])
                docs.push_back(i);
            infos[c] = makeEmbeddedInfo(docs);
        }
        for (size_t c = 0; c < steps.size(); ++c)
            postings[token_names[c]] = makeEmbeddedCursor(infos[c]);
        return postings;
    };

    std::vector<TokenPostingsInfo> infos1;
    auto postings_leapfrog = make_postings(infos1);
    auto result_leapfrog = intersectAndCollect(postings_leapfrog, token_names, 0, range, 100.0f);

    std::vector<TokenPostingsInfo> infos2;
    auto postings_brute = make_postings(infos2);
    auto result_brute = intersectAndCollect(postings_brute, token_names, 0, range, 0.0f);

    EXPECT_EQ(result_leapfrog, result_brute);

    /// LCM(2,3,4,6,8,12) = 24. Expected: 0, 24, 48, 72, 96, 120.
    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < range; i += 24)
        expected.push_back(i);
    EXPECT_EQ(result_leapfrog, expected);
}


// =============================================================================
// Section 21: Edge Cases — Empty and Single-element Scenarios
// =============================================================================

TEST(PostingListCursorTest, LinearOrEmptyCursor)
{
    /// linearOr on an empty cursor should be a no-op.
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(10, 0);
    cursor->linearOr(buf.data(), 0, 10);

    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ(buf[i], 0u);
}

TEST(PostingListCursorTest, LinearAndEmptyCursor)
{
    /// linearAnd on an empty cursor should be a no-op (buffer unchanged).
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(10, 5);
    cursor->linearAnd(buf.data(), 0, 10);

    for (size_t i = 0; i < 10; ++i)
        EXPECT_EQ(buf[i], 5u);
}

TEST(PostingListCursorTest, LinearOrSingleDoc)
{
    /// linearOr with a single-doc cursor.
    auto info = makeEmbeddedInfo({42});
    auto cursor = makeEmbeddedCursor(info);

    std::vector<UInt8> buf(50, 0);
    cursor->linearOr(buf.data(), 40, 10);
    EXPECT_EQ(buf[2], 1u); // doc 42 at offset 42-40=2
    EXPECT_EQ(buf[0], 0u);
    EXPECT_EQ(buf[9], 0u);
}

TEST(PostingListCursorTest, AdvanceOnEmptyCursorIsNoop)
{
    /// advance on a cursor constructed with no embedded_postings is a no-op.
    TokenPostingsInfo info;
    info.cardinality = 0;
    auto cursor = std::make_shared<PostingListCursor>(info);
    EXPECT_FALSE(cursor->valid());
    cursor->advance(100);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, NextOnEmptyCursorIsNoop)
{
    /// next on an empty cursor is a no-op.
    auto info = makeEmbeddedInfo({});
    auto cursor = makeEmbeddedCursor(info);
    EXPECT_FALSE(cursor->valid());
    cursor->next();
    EXPECT_FALSE(cursor->valid());
}


// =============================================================================
// Section 22: Union/Intersection with Missing Tokens
// =============================================================================

TEST(PostingListCursorTest, UnionWithAllMissingTokens)
{
    /// Union where none of the search tokens exist in the postings map.
    PostingListCursorMap postings;
    auto result = unionAndCollect(postings, {"missing_a", "missing_b"}, 0, 100);
    EXPECT_TRUE(result.empty());
}

TEST(PostingListCursorTest, IntersectWithOneMissingToken)
{
    /// Intersection where one token is missing from the map.
    /// Should still produce correct results (intersection of available cursors only).
    auto docs = generateRange(0, 50, 2);
    auto info = makeEmbeddedInfo(docs);

    PostingListCursorMap postings;
    postings["present"] = makeEmbeddedCursor(info);

    /// "absent" is not in postings — only 1 cursor found, treated as n==1.
    auto result = intersectAndCollect(postings, {"present", "absent"}, 0, 100, 100.0f);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 23: Intersection Consistency — Leapfrog vs Brute-force (Randomized)
// =============================================================================

TEST(PostingListCursorTest, LeapfrogVsBruteForceRandomConsistencyMultiCursor)
{
    /// Randomized consistency check with 3-8 cursors per trial.
    /// Seed is fixed for deterministic, reproducible results.
    std::mt19937 rng(12345); // NOLINT(cert-msc32-c, cert-msc51-cpp)
    constexpr size_t trials = 20;
    constexpr uint32_t range = 128;
    const std::vector<String> all_names = {"t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"};

    for (size_t trial = 0; trial < trials; ++trial)
    {
        size_t num_cursors = 3 + (rng() % 6); // 3..8 cursors
        std::vector<String> tokens(all_names.begin(), all_names.begin() + num_cursors);
        std::uniform_int_distribution<uint32_t> step_dist(1, 10);

        /// Pre-generate doc sets so both runs use the same data.
        std::vector<std::vector<uint32_t>> doc_sets(num_cursors);
        for (size_t c = 0; c < num_cursors; ++c)
        {
            uint32_t step = step_dist(rng);
            uint32_t start = rng() % std::min(step, range);
            for (uint32_t i = start; i < range; i += step)
                doc_sets[c].push_back(i);
        }

        auto make_postings = [&]()
        {
            std::vector<TokenPostingsInfo> infos(num_cursors);
            PostingListCursorMap postings;
            for (size_t c = 0; c < num_cursors; ++c)
                infos[c] = makeEmbeddedInfo(doc_sets[c]);
            for (size_t c = 0; c < num_cursors; ++c)
                postings[tokens[c]] = makeEmbeddedCursor(infos[c]);
            return std::make_pair(std::move(postings), std::move(infos));
        };

        auto [postings1, infos1] = make_postings();
        auto result_leapfrog = intersectAndCollect(postings1, tokens, 0, range, 100.0f);

        auto [postings2, infos2] = make_postings();
        auto result_brute = intersectAndCollect(postings2, tokens, 0, range, 0.0f);

        EXPECT_EQ(result_leapfrog, result_brute) << "Trial " << trial << " with " << num_cursors << " cursors";
    }
}


// =============================================================================
// Section 24: Compressed Cursor — Basic Iteration (V2 format)
// =============================================================================

TEST(PostingListCursorTest, CompressedSmallPostingList)
{
    /// Small posting list (< BLOCK_SIZE) — single tail block.
    auto docs = generateRange(0, 50);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedExactlyOneBlock)
{
    /// Exactly BLOCK_SIZE (128) docs — one full packed block, no tail.
    auto docs = generateRange(0, BLOCK_SIZE);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedMultipleBlocks)
{
    /// 300 docs — 2 full blocks (128 each) + tail block (44).
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedSparseDocIds)
{
    /// Sparse doc IDs with large gaps — tests delta encoding correctness.
    auto docs = generateRange(100, 200, 7); // 100, 107, 114, ..., 1493
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, docs.front());
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedLargePostingList)
{
    /// 1000 docs — 7 full blocks + tail block. Exercises advanceImpl across many blocks.
    auto docs = generateRange(0, 1000);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 25: Compressed Cursor — Advance (advanceImpl binary search)
// =============================================================================

TEST(PostingListCursorTest, CompressedAdvanceToMiddle)
{
    /// Advance to a doc in the middle of a multi-block posting list.
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(250);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 250u);
}

TEST(PostingListCursorTest, CompressedAdvanceToBlockBoundary)
{
    /// Advance to exactly the last doc of first packed block (doc 127).
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(127);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 127u);

    /// Then advance to first doc of second packed block (doc 128).
    cursor->advance(128);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 128u);
}

TEST(PostingListCursorTest, CompressedAdvanceBeyondEnd)
{
    auto docs = generateRange(0, 100);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(100);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, CompressedAdvanceProgressively)
{
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 50u);

    cursor->advance(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);

    cursor->advance(400);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 400u);

    cursor->advance(500);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, CompressedAdvanceToGap)
{
    /// Sparse docs: advance to a value that falls between actual doc IDs.
    auto docs = generateRange(0, 100, 5); // 0, 5, 10, ..., 495
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(7); // between 5 and 10 → should land on 10
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);
}


// =============================================================================
// Section 26: Compressed Cursor — linearOr
// =============================================================================

TEST(PostingListCursorTest, CompressedLinearOrFullRange)
{
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 0, 300);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedLinearOrPartialRange)
{
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Window [100, 300) — only docs 100..299.
    auto result = linearOrToDocIds(cursor, 100, 200);
    auto expected = generateRange(100, 200);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedLinearOrSparse)
{
    auto docs = generateRange(0, 100, 3); // 0, 3, 6, ..., 297
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 0, 300);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 27: Compressed Cursor — linearAnd
// =============================================================================

TEST(PostingListCursorTest, CompressedLinearAndFullRange)
{
    /// linearAnd on compressed cursors has an "all-zeros skip" optimization:
    /// if the buffer region is all-zero, it skips entirely (designed for brute-force
    /// intersection where linearOr runs first). So pre-fill the buffer with 1.
    auto docs = generateRange(0, 200);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(200, 1);
    cursor->linearAnd(buf.data(), 0, 200);

    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 2u) << "row " << i;
}

TEST(PostingListCursorTest, CompressedLinearAndIncrementsExisting)
{
    auto docs = generateRange(0, 150);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(200, 1);
    cursor->linearAnd(buf.data(), 0, 200);

    for (size_t i = 0; i < 150; ++i)
        EXPECT_EQ(buf[i], 2u) << "row " << i;
    for (size_t i = 150; i < 200; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
}


// =============================================================================
// Section 28: Compressed Cursor — Multi-segment
// =============================================================================

TEST(PostingListCursorTest, CompressedMultiSegment)
{
    /// Use a small segment_size (256) to force multiple segments.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    EXPECT_GT(data.info.offsets.size(), 1u) << "Expected multiple segments";

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedMultiSegmentAdvanceAcross)
{
    /// Multiple segments, advance across segment boundaries.
    auto docs = generateRange(0, 800);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Advance to a doc that should be in a later segment.
    cursor->advance(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 500u);

    cursor->advance(700);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 700u);
}

TEST(PostingListCursorTest, CompressedMultiSegmentLinearOr)
{
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 0, 600);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 29: Compressed Cursor — Intersection
// =============================================================================

TEST(PostingListCursorTest, CompressedIntersectTwoCursors)
{
    auto docs_a = generateRange(0, 200, 2); // 0, 2, 4, ..., 398
    auto docs_b = generateRange(0, 134, 3); // 0, 3, 6, ..., 399

    auto data_a = makeCompressedData(docs_a);
    auto data_b = makeCompressedData(docs_b);
    auto cursor_a = makeCompressedCursor(data_a);
    auto cursor_b = makeCompressedCursor(data_b);
    CompressedTestDataCleanup cleanup_a{data_a};
    CompressedTestDataCleanup cleanup_b{data_b};

    PostingListCursorMap postings;
    postings["a"] = cursor_a;
    postings["b"] = cursor_b;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 400, 100.0f);

    /// LCM(2,3)=6. Expected: 0, 6, 12, ..., 396.
    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 400; i += 6)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedUnionTwoCursors)
{
    auto docs_a = generateRange(0, 100);    // 0..99
    auto docs_b = generateRange(200, 100);  // 200..299

    auto data_a = makeCompressedData(docs_a);
    auto data_b = makeCompressedData(docs_b);
    auto cursor_a = makeCompressedCursor(data_a);
    auto cursor_b = makeCompressedCursor(data_b);
    CompressedTestDataCleanup cleanup_a{data_a};
    CompressedTestDataCleanup cleanup_b{data_b};

    PostingListCursorMap postings;
    postings["a"] = cursor_a;
    postings["b"] = cursor_b;

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 300);

    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d < 100; ++d) expected.push_back(d);
    for (uint32_t d = 200; d < 300; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedBruteForceVsLeapfrog)
{
    /// Verify brute-force and leapfrog produce the same result on compressed cursors.
    auto docs_a = generateRange(0, 200, 3); // 0, 3, ..., 597
    auto docs_b = generateRange(0, 120, 5); // 0, 5, ..., 595

    auto make_postings = [&]()
    {
        auto data_a = std::make_shared<CompressedTestData>(makeCompressedData(docs_a));
        auto data_b = std::make_shared<CompressedTestData>(makeCompressedData(docs_b));
        auto cursor_a = makeCompressedCursor(*data_a);
        auto cursor_b = makeCompressedCursor(*data_b);
        PostingListCursorMap postings;
        postings["a"] = cursor_a;
        postings["b"] = cursor_b;
        return std::make_tuple(postings, data_a, data_b);
    };

    auto [postings_lf, da1, db1] = make_postings();
    auto result_leapfrog = intersectAndCollect(postings_lf, {"a", "b"}, 0, 600, 100.0f);

    auto [postings_bf, da2, db2] = make_postings();
    auto result_brute = intersectAndCollect(postings_bf, {"a", "b"}, 0, 600, 0.0f);

    EXPECT_EQ(result_leapfrog, result_brute);

    /// LCM(3,5)=15. Expected: 0, 15, 30, ..., 585.
    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i < 600; i += 15)
        expected.push_back(i);
    EXPECT_EQ(result_leapfrog, expected);

    /// Cleanup.
    da1->stream.reset(); da2->stream.reset();
    db1->stream.reset(); db2->stream.reset();
    fs::remove_all(da1->tmp_dir); fs::remove_all(da2->tmp_dir);
    fs::remove_all(db1->tmp_dir); fs::remove_all(db2->tmp_dir);
}


// =============================================================================
// Section 30: Compressed Cursor — Mixed Embedded + Compressed
// =============================================================================

TEST(PostingListCursorTest, CompressedIntersectWithEmbedded)
{
    /// Compressed cursor A: [0..199]
    /// Embedded cursor B: {50, 100, 150, 250}
    /// Intersection: {50, 100, 150}
    auto docs_a = generateRange(0, 200);
    auto data_a = makeCompressedData(docs_a);
    auto cursor_a = makeCompressedCursor(data_a);
    CompressedTestDataCleanup cleanup_a{data_a};

    auto info_b = makeEmbeddedInfo({50, 100, 150, 250});

    PostingListCursorMap postings;
    postings["compressed"] = cursor_a;
    postings["embedded"] = makeEmbeddedCursor(info_b);

    auto result = intersectAndCollect(postings, {"compressed", "embedded"}, 0, 300, 100.0f);
    std::vector<uint32_t> expected = {50, 100, 150};
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 31: Compressed Cursor — V1 Format (no BlockIndex)
// =============================================================================

TEST(PostingListCursorTest, CompressedV1SmallPostingList)
{
    /// V1 format: clear HasBlockIndex flag so prepareSegment scans payload
    /// to rebuild block metadata instead of reading the Index Section.
    auto docs = generateRange(0, 50);
    auto data = makeCompressedData(docs);
    data.info.header = PostingsSerialization::Flags::IsCompressed; // no HasBlockIndex
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedV1MultipleBlocks)
{
    /// V1 with multiple packed blocks — exercises the V1 block scanning loop.
    auto docs = generateRange(0, 400);
    auto data = makeCompressedData(docs);
    data.info.header = PostingsSerialization::Flags::IsCompressed;
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedV1AdvanceWithBinarySearch)
{
    /// V1 format: verify that advanceImpl still works after V1 rebuilds block metadata.
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    data.info.header = PostingsSerialization::Flags::IsCompressed;
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(250);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 250u);

    cursor->advance(400);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 400u);
}

TEST(PostingListCursorTest, CompressedV1LinearOr)
{
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    data.info.header = PostingsSerialization::Flags::IsCompressed;
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 50, 200);
    auto expected = generateRange(50, 200);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedV1SparseDocIds)
{
    /// V1 with sparse doc IDs — larger deltas stress the bit-width scanning.
    auto docs = generateRange(0, 200, 7);
    auto data = makeCompressedData(docs);
    data.info.header = PostingsSerialization::Flags::IsCompressed;
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, docs.front());
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 32: Compressed Cursor — next() Crossing Block/Segment Boundaries
// =============================================================================

TEST(PostingListCursorTest, CompressedNextCrossesBlockBoundary)
{
    /// 300 docs = 2 full blocks (128 each) + tail (44).
    /// Advance to doc 127 (last in first block), then next() should cross to block 1.
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(127);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 127u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 128u);
}

TEST(PostingListCursorTest, CompressedNextCrossesSegmentBoundary)
{
    /// Two segments. next() at end of segment 0 should cross to segment 1.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    ASSERT_GT(data.info.offsets.size(), 1u);

    /// Advance to near end of first segment, then drain across boundary.
    cursor->advance(254);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 254u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 255u);

    cursor->next();
    ASSERT_TRUE(cursor->valid());
    /// Next doc should be 256 (first doc of segment 1).
    EXPECT_EQ(cursor->value(), 256u);
}

TEST(PostingListCursorTest, CompressedNextExhaustsAllSegments)
{
    /// Drain a multi-segment cursor entirely via next().
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 33: Compressed Cursor — Dense Segment Optimizations
// =============================================================================

TEST(PostingListCursorTest, CompressedDenseSegmentLinearOrMemset)
{
    /// Dense consecutive docs → segment_doc_count == range_span → triggers Level 1 memset.
    auto docs = generateRange(0, 200); // density = 1.0
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(250, 0);
    cursor->linearOr(buf.data(), 0, 250);

    /// Rows 0..199 should be set via memset.
    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
    /// Rows 200..249 should remain 0.
    for (size_t i = 200; i < 250; ++i)
        EXPECT_EQ(buf[i], 0u) << "row " << i;
}

TEST(PostingListCursorTest, CompressedDenseSegmentLinearAndShortcut)
{
    /// Dense consecutive docs → triggers Level 1 dense shortcut in linearAnd.
    auto docs = generateRange(0, 200);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(250, 1);
    cursor->linearAnd(buf.data(), 0, 250);

    /// Rows 0..199: incremented from 1 to 2.
    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 2u) << "row " << i;
    /// Rows 200..249: remain 1 (not in posting list).
    for (size_t i = 200; i < 250; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
}


// =============================================================================
// Section 34: Compressed Cursor — Skip Optimizations
// =============================================================================

TEST(PostingListCursorTest, CompressedLinearOrAlreadyCoveredSkip)
{
    /// Level 2a: if the output region is already all-ones, linearOr should skip.
    /// We pre-fill the buffer, then call linearOr — result should be the same.
    auto docs = generateRange(0, 200);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(200, 1); // pre-filled with all ones
    cursor->linearOr(buf.data(), 0, 200);

    /// Should still be all 1 (skip path is a no-op).
    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
}

TEST(PostingListCursorTest, CompressedLinearAndAllZerosSkip)
{
    /// Level 2a: if the output region is all zeros, linearAnd should skip
    /// (incrementing zeros won't help the final count==n check).
    auto docs = generateRange(0, 200);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(200, 0); // all zeros
    cursor->linearAnd(buf.data(), 0, 200);

    /// Should remain all 0 (skip path).
    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 0u) << "row " << i;
}

TEST(PostingListCursorTest, CompressedLinearOrPartialCoverageNoSkip)
{
    /// Level 2b: partial coverage — some blocks covered, some not.
    /// The covered blocks should be skipped, uncovered blocks should be decoded.
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(300, 0);
    /// Pre-fill first 128 rows (first packed block) with 1.
    memset(buf.data(), 1, 128);

    cursor->linearOr(buf.data(), 0, 300);

    /// All 300 rows should be 1.
    for (size_t i = 0; i < 300; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
}


// =============================================================================
// Section 35: Compressed Cursor — Cardinality and Density
// =============================================================================

TEST(PostingListCursorTest, CompressedCardinality)
{
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    EXPECT_EQ(cursor->cardinality(), 500u);
}

TEST(PostingListCursorTest, CompressedDensityDense)
{
    auto docs = generateRange(0, 100); // 0..99, density = 1.0
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    EXPECT_DOUBLE_EQ(cursor->density(), 1.0);
}

TEST(PostingListCursorTest, CompressedDensitySparse)
{
    auto docs = generateRange(0, 50, 10); // 0, 10, ..., 490
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    double expected = 50.0 / (490.0 + 1.0);
    EXPECT_NEAR(cursor->density(), expected, 1e-6);
}


// =============================================================================
// Section 36: Compressed Cursor — Edge Cases
// =============================================================================

TEST(PostingListCursorTest, CompressedSingleDoc)
{
    auto data = makeCompressedData({42});
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 42);
    EXPECT_EQ(result, std::vector<uint32_t>{42});
}

TEST(PostingListCursorTest, CompressedAdvanceThenNext)
{
    /// Interleave advance and next on a compressed cursor.
    auto docs = generateRange(0, 500);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(100);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);

    cursor->next();
    EXPECT_EQ(cursor->value(), 101u);

    cursor->advance(300);
    EXPECT_EQ(cursor->value(), 300u);

    cursor->next();
    EXPECT_EQ(cursor->value(), 301u);

    cursor->next();
    EXPECT_EQ(cursor->value(), 302u);
}

TEST(PostingListCursorTest, CompressedLinearOrNoOverlap)
{
    /// linearOr with a window that doesn't overlap the posting range at all.
    auto docs = generateRange(0, 100);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(50, 0);
    cursor->linearOr(buf.data(), 500, 50); // window [500, 550) — no overlap

    for (size_t i = 0; i < 50; ++i)
        EXPECT_EQ(buf[i], 0u);
}

TEST(PostingListCursorTest, CompressedLinearOrWindowBeforePostings)
{
    /// Posting range starts at 100, window is [0, 50).
    auto docs = generateRange(100, 200);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(50, 0);
    cursor->linearOr(buf.data(), 0, 50);

    for (size_t i = 0; i < 50; ++i)
        EXPECT_EQ(buf[i], 0u);
}

TEST(PostingListCursorTest, CompressedMultiSegmentLinearAnd)
{
    /// Multi-segment linearAnd with pre-filled buffer.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(700, 1);
    cursor->linearAnd(buf.data(), 0, 700);

    for (size_t i = 0; i < 600; ++i)
        EXPECT_EQ(buf[i], 2u) << "row " << i;
    for (size_t i = 600; i < 700; ++i)
        EXPECT_EQ(buf[i], 1u) << "row " << i;
}

TEST(PostingListCursorTest, CompressedExactlyTwoBlocks)
{
    /// Exactly 256 docs → 2 full packed blocks, no tail.
    auto docs = generateRange(0, 256);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedTailBlockOnly)
{
    /// 10 docs → single tail block (no full packed block).
    auto docs = generateRange(0, 10);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedV1MultiSegment)
{
    /// V1 format with multiple segments.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    data.info.header = PostingsSerialization::Flags::IsCompressed;
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    ASSERT_GT(data.info.offsets.size(), 1u);
    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 38: Compressed V2 — Multi-Segment with Sparse Doc IDs
// =============================================================================

TEST(PostingListCursorTest, CompressedMultiSegmentSparse)
{
    /// Sparse doc IDs with large gaps spanning segment boundaries.
    /// segment_size=256 but step=7, so doc IDs are {0, 7, 14, ..., 6993}.
    auto docs = generateRange(0, 1000, 7);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    ASSERT_GT(data.info.offsets.size(), 1u) << "Expected multiple segments";
    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedMultiSegmentSparseAdvance)
{
    /// Sparse multi-segment: advance to doc IDs near segment boundaries.
    auto docs = generateRange(0, 1000, 7);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Advance to a gap — should land on next doc.
    cursor->advance(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 504u); // next multiple of 7 >= 500 is 504

    cursor->advance(1800);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1806u); // next multiple of 7 >= 1800

    cursor->advance(7000);
    EXPECT_FALSE(cursor->valid()); // max is 6993
}


// =============================================================================
// Section 39: Compressed V2 — Multi-Segment Intersection and Union
// =============================================================================

TEST(PostingListCursorTest, CompressedMultiSegmentIntersectTwoCursors)
{
    /// Two multi-segment compressed cursors intersected.
    auto docs_a = generateRange(0, 400, 2); // 0, 2, 4, ..., 798
    auto docs_b = generateRange(0, 270, 3); // 0, 3, 6, ..., 807

    auto data_a = makeCompressedData(docs_a, 256);
    auto data_b = makeCompressedData(docs_b, 256);
    ASSERT_GT(data_a.info.offsets.size(), 1u);
    ASSERT_GT(data_b.info.offsets.size(), 1u);

    auto cursor_a = makeCompressedCursor(data_a);
    auto cursor_b = makeCompressedCursor(data_b);
    CompressedTestDataCleanup cleanup_a{data_a};
    CompressedTestDataCleanup cleanup_b{data_b};

    PostingListCursorMap postings;
    postings["a"] = cursor_a;
    postings["b"] = cursor_b;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 810, 100.0f);

    /// LCM(2,3)=6. Expected: 0, 6, 12, ..., up to min(798, 807).
    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i <= 798; i += 6)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedMultiSegmentIntersectBruteForce)
{
    /// Same as above but force brute-force path (density_threshold=0).
    auto docs_a = generateRange(0, 400, 2);
    auto docs_b = generateRange(0, 270, 3);

    auto data_a = makeCompressedData(docs_a, 256);
    auto data_b = makeCompressedData(docs_b, 256);
    auto cursor_a = makeCompressedCursor(data_a);
    auto cursor_b = makeCompressedCursor(data_b);
    CompressedTestDataCleanup cleanup_a{data_a};
    CompressedTestDataCleanup cleanup_b{data_b};

    PostingListCursorMap postings;
    postings["a"] = cursor_a;
    postings["b"] = cursor_b;

    auto result = intersectAndCollect(postings, {"a", "b"}, 0, 810, 0.0f);

    std::vector<uint32_t> expected;
    for (uint32_t i = 0; i <= 798; i += 6)
        expected.push_back(i);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedMultiSegmentUnionTwoCursors)
{
    /// Two disjoint multi-segment cursors unioned.
    auto docs_a = generateRange(0, 400);    // 0..399
    auto docs_b = generateRange(500, 400);  // 500..899

    auto data_a = makeCompressedData(docs_a, 256);
    auto data_b = makeCompressedData(docs_b, 256);
    auto cursor_a = makeCompressedCursor(data_a);
    auto cursor_b = makeCompressedCursor(data_b);
    CompressedTestDataCleanup cleanup_a{data_a};
    CompressedTestDataCleanup cleanup_b{data_b};

    PostingListCursorMap postings;
    postings["a"] = cursor_a;
    postings["b"] = cursor_b;

    auto result = unionAndCollect(postings, {"a", "b"}, 0, 900);

    std::vector<uint32_t> expected;
    for (uint32_t d = 0; d < 400; ++d) expected.push_back(d);
    for (uint32_t d = 500; d < 900; ++d) expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 40: Compressed V2 — Window Spanning Segment Boundary
// =============================================================================

TEST(PostingListCursorTest, CompressedMultiSegmentLinearOrSpanBoundary)
{
    /// linearOr with a window [200, 400) that spans segment 0 and segment 1 (segment_size=256).
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    ASSERT_GT(data.info.offsets.size(), 1u);
    auto result = linearOrToDocIds(cursor, 200, 200);
    auto expected = generateRange(200, 200);
    EXPECT_EQ(result, expected);
}

TEST(PostingListCursorTest, CompressedMultiSegmentLinearAndSpanBoundary)
{
    /// linearAnd with a window [200, 400) spanning segment boundary.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    std::vector<UInt8> buf(200, 1);
    cursor->linearAnd(buf.data(), 200, 200);

    /// All rows in [200, 400) are in the posting list → incremented to 2.
    for (size_t i = 0; i < 200; ++i)
        EXPECT_EQ(buf[i], 2u) << "row " << (200 + i);
}

TEST(PostingListCursorTest, CompressedMultiSegmentLinearOrSparseSpanBoundary)
{
    /// Sparse docs across segment boundary — window [200, 400).
    auto docs = generateRange(0, 300, 3); // 0, 3, 6, ..., 897
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 200, 200);

    std::vector<uint32_t> expected;
    for (auto d : docs)
        if (d >= 200 && d < 400)
            expected.push_back(d);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 41: Compressed V2 — Non-Zero Starting Doc IDs
// =============================================================================

TEST(PostingListCursorTest, CompressedNonZeroStartDrain)
{
    /// Doc IDs starting from 1000: {1000, 1001, ..., 1299}.
    auto docs = generateRange(1000, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 1000);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedNonZeroStartAdvance)
{
    auto docs = generateRange(1000, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Advance to before the first doc — should land on 1000.
    cursor->advance(500);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1000u);

    /// Advance to middle.
    cursor->advance(1150);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 1150u);

    /// Advance beyond end.
    cursor->advance(1300);
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, CompressedNonZeroStartLinearOr)
{
    auto docs = generateRange(1000, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Window covers the posting range exactly.
    auto result = linearOrToDocIds(cursor, 1000, 300);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedNonZeroStartLinearOrPartial)
{
    /// Doc IDs {1000..1299}, window [1100, 1250).
    auto docs = generateRange(1000, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 1100, 150);
    auto expected = generateRange(1100, 150);
    EXPECT_EQ(result, expected);
}


// =============================================================================
// Section 42: Compressed V2 — High Bit-Width Deltas
// =============================================================================

TEST(PostingListCursorTest, CompressedHighBitWidthDeltas)
{
    /// Very large gaps between doc IDs — forces high bit-width in bitpacking.
    std::vector<uint32_t> docs = {0, 100000, 200000, 300000, 400000};
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = advanceAndDrainCursor(cursor, 0);
    EXPECT_EQ(result, docs);
}

TEST(PostingListCursorTest, CompressedHighBitWidthAdvance)
{
    std::vector<uint32_t> docs = {0, 100000, 200000, 300000, 400000};
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(150000); // between 100000 and 200000
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200000u);

    cursor->advance(400000);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 400000u);

    cursor->next();
    EXPECT_FALSE(cursor->valid());
}

TEST(PostingListCursorTest, CompressedHighBitWidthLinearOr)
{
    /// Large gaps: linearOr on a window covering the whole range.
    std::vector<uint32_t> docs = {1000, 50000, 99999};
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    auto result = linearOrToDocIds(cursor, 0, 100000);
    EXPECT_EQ(result, docs);
}


// =============================================================================
// Section 43: Compressed V2 — Advance Within Already-Decoded Block
// =============================================================================

TEST(PostingListCursorTest, CompressedAdvanceWithinDecodedBlock)
{
    /// advance to doc 10, then advance to doc 50 — both in block 0 (docs 0..127).
    /// Second advance should reuse already-decoded block, not re-decode.
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(10);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 10u);

    /// Still in block 0 — advanceImpl should find target in decoded_values.
    cursor->advance(50);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 50u);

    /// Advance to last doc of block 0.
    cursor->advance(127);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 127u);

    /// Now cross to block 1.
    cursor->advance(128);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 128u);

    /// Advance within block 1.
    cursor->advance(200);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 200u);
}

TEST(PostingListCursorTest, CompressedAdvanceToCurrentValue)
{
    /// advance(value()) should be a no-op.
    auto docs = generateRange(0, 300);
    auto data = makeCompressedData(docs);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    cursor->advance(100);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);

    cursor->advance(100); // same value
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 100u);

    cursor->next();
    EXPECT_EQ(cursor->value(), 101u);
}


// =============================================================================
// Section 44: Compressed V2 — Advance to Segment Boundary
// =============================================================================

TEST(PostingListCursorTest, CompressedMultiSegmentAdvanceToSegmentStart)
{
    /// Advance to the exact first doc of segment 1.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    ASSERT_GT(data.info.offsets.size(), 1u);

    /// Doc 256 is the first doc of segment 1.
    cursor->advance(256);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 256u);

    /// Verify continuation works.
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 257u);
}

TEST(PostingListCursorTest, CompressedMultiSegmentAdvanceToSegmentEnd)
{
    /// Advance to the last doc of segment 0.
    auto docs = generateRange(0, 600);
    auto data = makeCompressedData(docs, 256);
    auto cursor = makeCompressedCursor(data);
    CompressedTestDataCleanup cleanup{data};

    /// Doc 255 is the last doc of segment 0.
    cursor->advance(255);
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 255u);

    /// next() should cross to segment 1.
    cursor->next();
    ASSERT_TRUE(cursor->valid());
    EXPECT_EQ(cursor->value(), 256u);
}
