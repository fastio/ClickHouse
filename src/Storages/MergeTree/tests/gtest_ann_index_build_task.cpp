#include <future>
#include <stdexcept>
#include <string_view>
#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>
#include <Storages/Reflection/ANNIndex/BuildTask.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartReverseLookup.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartBuilder.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/TemporaryFile.h>

using namespace DB;


namespace
{

/// Minimal algorithm stub that satisfies IANNIndexAlgorithm's pure
/// virtuals without exercising any real build / search logic. Early change
/// packs in this test file drive the state machine past all five stages with
/// no IO, so the three build-phase overrides are counter-only.
class BuildOnlyMockAlgorithm : public IANNIndexAlgorithm
{
public:
    String getName() const override { return "mock"; }
    String getFamily() const override { return "mock"; }

    void validateBuildParameters(const ASTPtr &, ContextPtr) override {}
    void validateIndexedExpression(const ASTPtr &, const StorageInMemoryMetadata &) override {}
    void initialize(const ANNIndexContext &) override {}

    std::optional<MatchDescriptor> match(const QueryFeatures &) const override { return std::nullopt; }

    AlgorithmCostEstimate estimateCost(const MatchDescriptor &, const CoverageSnapshot &) const override { return {}; }

    InternalSearchResult search(
        const MatchDescriptor &,
        const ReadyANNIndexPartSnapshot &,
        size_t,
        ContextPtr) const override
    {
        return {};
    }

    void prepareBuild(const AlgorithmBuildContext &, const Block &) override { ++prepare_calls; }
    void buildAlgorithmPrivate(const AlgorithmBuildContext &) override { ++build_calls; }
    void finishBuild(const AlgorithmBuildContext &) override { ++finish_calls; }

    std::unique_ptr<IANNIndexAlgorithm> cloneForBuild() const override
    {
        /// Mid-layer BuildTask is driven directly with this algorithm pointer
        /// in these unit tests; the framework-side `ANNIndexBuildTask`
        /// (which is what calls `cloneForBuild` in production) is not exercised
        /// here, so a stub is sufficient.
        return std::make_unique<BuildOnlyMockAlgorithm>();
    }

    size_t prepare_calls = 0;
    size_t build_calls = 0;
    size_t finish_calls = 0;
};

class ThrowingBuildAlgorithm : public BuildOnlyMockAlgorithm
{
public:
    void buildAlgorithmPrivate(const AlgorithmBuildContext &) override
    {
        throw std::runtime_error("injected ANNIndex build failure");
    }
};

}


TEST(ANNIndexBuildTaskTest, EmptyStagesStateMachineAdvances)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        /*source_parts_=*/{},
        &algorithm,
        /*storage_=*/nullptr,
        /*inner_storage_holder_=*/nullptr,
        /*new_part_name_=*/"materialized-index-0_0_0_0",
        /*source_storage_=*/nullptr,
        /*source_snapshot_=*/nullptr,
        /*source_metadata_=*/nullptr,
        /*context_=*/nullptr,
        /*output_storage_=*/nullptr,
        /*intermediate_storage_=*/nullptr,
        /*memory_budget_bytes_=*/0);

    /// Five stages: the driver performs five iterations total (four return
    /// true, the last returns false after fulfilling the promise).
    size_t true_returns = 0;
    while (task.execute())
        ++true_returns;

    EXPECT_EQ(true_returns, 4U);

    /// Stage 1 never sees a block (no source parts), so prepareBuild is not
    /// invoked. Stages 2 and 3 are one-shot unconditional algorithm calls,
    /// so both fire exactly once on the empty path.
    EXPECT_EQ(algorithm.prepare_calls, 0U);
    EXPECT_EQ(algorithm.build_calls, 1U);
    EXPECT_EQ(algorithm.finish_calls, 1U);
}


TEST(ANNIndexBuildTaskTest, SkeletonPromiseResolvesWithEmptyPart)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0);

    while (task.execute()) {}

    auto future = task.getFuture();
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready);

    /// The skeleton stage 6 leaves new_ann_index_part null on purpose; the promise
    /// is still fulfilled so waiters never block.
    auto part = future.get();
    EXPECT_EQ(part, nullptr);
}


TEST(ANNIndexBuildTaskTest, StageExceptionResolvesPromiseWithException)
{
    ThrowingBuildAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0);

    auto future = task.getFuture();
    while (task.execute()) {}

    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_THROW(static_cast<void>(future.get()), std::runtime_error);
}


TEST(ANNIndexBuildTaskTest, CancelIsIdempotent)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        0);

    /// Two back-to-back cancels must be safe before and after state machine
    /// drives to completion.
    task.cancel();
    task.cancel();
    while (task.execute()) {}
    task.cancel();

    SUCCEED();
}


/// Stage-6 scoped fixture. Drives the full 6-stage pipeline with an empty
/// source-part vector against a real DiskLocal-backed output storage so the
/// metadata-writing stage is exercised end to end. A non-empty data path
/// would additionally require a real source MergeTreeData + pipeline; that
/// is deliberately covered by functional (.sql) tests in a later change
/// because the fixture surface to spin it up here would be outsized relative
/// to the assertion content this change pack is after.
class ANNIndexBuildTaskStage6Test : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        disk = std::make_shared<DiskLocal>("ann_index_build_test_disk", temp_dir->path());
        disk->createDirectories("output/part");
        volume = std::make_shared<SingleDiskVolume>("ann_index_build_test_vol", disk, 0);
        output_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "output", "part");
    }

    void TearDown() override
    {
        output_storage.reset();
        volume.reset();
        disk.reset();
        temp_dir.reset();
    }

    std::string readFile(const std::string & relpath) const
    {
        auto buf = output_storage->readFile(relpath, ReadSettings{}, std::nullopt);
        std::string body;
        readStringUntilEOF(body, *buf);
        return body;
    }

    MergeTreeDataPartChecksums readChecksums() const
    {
        auto buf = output_storage->readFile("checksums.txt", ReadSettings{}, std::nullopt);
        MergeTreeDataPartChecksums checksums;
        checksums.read(*buf);
        assertEOF(*buf);
        return checksums;
    }

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<DiskLocal> disk;
    VolumePtr volume;
    MutableDataPartStoragePtr output_storage;
};


TEST_F(ANNIndexBuildTaskStage6Test, WritesStandardMetadataFiles)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        /*source_parts_=*/{},
        &algorithm,
        /*storage_=*/nullptr,
        /*inner_storage_holder_=*/nullptr,
        /*new_part_name_=*/"materialized-index-0_0_0_0",
        /*source_storage_=*/nullptr,
        /*source_snapshot_=*/nullptr,
        /*source_metadata_=*/nullptr,
        /*context_=*/nullptr,
        output_storage,
        /*intermediate_storage_=*/nullptr,
        /*memory_budget_bytes_=*/0);

    while (task.execute()) {}

    EXPECT_TRUE(output_storage->existsFile("header.json"));
    EXPECT_TRUE(output_storage->existsFile("coverage.json"));
    EXPECT_TRUE(output_storage->existsFile("columns.txt"));
    EXPECT_TRUE(output_storage->existsFile("count.txt"));
    EXPECT_TRUE(output_storage->existsFile("partition.dat"));
    EXPECT_TRUE(output_storage->existsFile("minmax__source_partition_id.idx"));
    EXPECT_TRUE(output_storage->existsFile("uuid.txt"));
    EXPECT_TRUE(output_storage->existsFile("metadata_version.txt"));
    EXPECT_TRUE(output_storage->existsFile("checksums.txt"));
    EXPECT_FALSE(output_storage->existsFile("checksum.txt"));
    EXPECT_FALSE(output_storage->existsFile("txn_version.txt"));
    EXPECT_FALSE(output_storage->existsFile(String{"part_uuid_"} + "dict.bin"));
    EXPECT_FALSE(output_storage->existsFile(String{"partition_"} + "dict.bin"));
    EXPECT_FALSE(output_storage->existsFile(String{"stable_"} + "mapping_0.bin"));
    EXPECT_FALSE(output_storage->existsFile(String{"mutable_"} + "mapping_0.bin"));
}


TEST_F(ANNIndexBuildTaskStage6Test, PartFormatFromDiskDetectsANNIndexLayout)
{
    auto writer = output_storage->writeFile("header.json", 4096, WriteSettings{});
    constexpr std::string_view empty_header = "{}";
    writer->write(empty_header.data(), empty_header.size());
    writer->finalize();

    auto [storage, part_type] = MergeTreeDataPartBuilder::getPartStorageAndType(
        volume,
        "output",
        "part",
        ReadSettings{});

    ASSERT_TRUE(storage);
    ASSERT_TRUE(part_type);
    EXPECT_EQ(part_type->getValue(), MergeTreeDataPartType::ANNIndex);
}


TEST_F(ANNIndexBuildTaskStage6Test, HeaderJsonCarriesAlgorithmIdentityAndVersion)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        output_storage,
        nullptr,
        0);

    while (task.execute()) {}

    Poco::JSON::Parser parser;
    const auto parsed = parser.parse(readFile("header.json"));
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();

    EXPECT_EQ(obj->getValue<int>("version"), 1);
    EXPECT_EQ(obj->getValue<std::string>("algorithm_family"), "mock");
    EXPECT_EQ(obj->getValue<std::string>("algorithm_impl"), "mock");
    EXPECT_EQ(obj->getValue<UInt64>("total_rows"), 0U);
    EXPECT_FALSE(obj->has(String{"locator_"} + "format_version"));
    EXPECT_FALSE(obj->has(String{"locator_tombstone_"} + "dict_id"));
    EXPECT_TRUE(obj->has("part_uuid_table"));
    EXPECT_EQ(obj->getArray("part_uuid_table")->size(), 0U);
    EXPECT_EQ(obj->getValue<UInt64>("tombstone_part_uuid_id"), ANNIndexPartReverseLookup::TOMBSTONE_PART_UUID_ID);
    EXPECT_EQ(obj->getValue<UInt64>("segment_count"), 0U);
    EXPECT_EQ(obj->getValue<UInt64>("coverage_source_part_count"), 0U);
}


TEST_F(ANNIndexBuildTaskStage6Test, DoesNotWriteTxnVersionMetadata)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        output_storage,
        nullptr,
        0);

    while (task.execute()) {}

    EXPECT_FALSE(output_storage->existsFile("txn_version.txt"));
}


TEST_F(ANNIndexBuildTaskStage6Test, CoverageJsonEmptyForZeroSourceParts)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        output_storage,
        nullptr,
        0);

    while (task.execute()) {}

    Poco::JSON::Parser parser;
    const auto parsed = parser.parse(readFile("coverage.json"));
    const auto & obj = parsed.extract<Poco::JSON::Object::Ptr>();

    EXPECT_EQ(obj->getValue<int>("format_version"), 1);
    auto covered = obj->getArray("covered");
    ASSERT_TRUE(covered);
    EXPECT_EQ(covered->size(), 0u);
}


TEST_F(ANNIndexBuildTaskStage6Test, ChecksumsTxtCoversANNIndexEnvelope)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
        nullptr,
        nullptr,
        "materialized-index-0_0_0_0",
        nullptr,
        nullptr,
        nullptr,
        nullptr,
        output_storage,
        nullptr,
        0);

    while (task.execute()) {}

    const auto checksums = readChecksums();
    EXPECT_TRUE(checksums.has("header.json"));
    EXPECT_TRUE(checksums.has("coverage.json"));
    EXPECT_TRUE(checksums.has("count.txt"));
    EXPECT_TRUE(checksums.has("partition.dat"));
    EXPECT_TRUE(checksums.has("minmax__source_partition_id.idx"));
    EXPECT_TRUE(checksums.has("uuid.txt"));
    EXPECT_FALSE(checksums.has("columns.txt"));
    EXPECT_FALSE(checksums.has("metadata_version.txt"));
    EXPECT_FALSE(checksums.has("checksums.txt"));
}
