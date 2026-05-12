#include <future>
#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <Storages/MaterializedIndex/IMaterializedIndexAlgorithm.h>
#include <Storages/MaterializedIndex/BuildTask.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartReverseLookup.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/TemporaryFile.h>

using namespace DB;


namespace
{

/// Minimal algorithm stub that satisfies IMaterializedIndexAlgorithm's pure
/// virtuals without exercising any real build / search logic. Early change
/// packs in this test file drive the state machine past all six stages with
/// no IO, so the three build-phase overrides are counter-only.
class BuildOnlyMockAlgorithm : public IMaterializedIndexAlgorithm
{
public:
    String getName() const override { return "mock"; }
    String getFamily() const override { return "mock"; }

    void validateBuildParameters(const ASTPtr &, ContextPtr) override {}
    void validateIndexedExpression(const ASTPtr &, const StorageInMemoryMetadata &) override {}
    void initialize(const MaterializedIndexContext &) override {}

    std::optional<MatchDescriptor> match(const QueryFeatures &) const override { return std::nullopt; }

    AlgorithmCostEstimate estimateCost(const MatchDescriptor &, const CoverageSnapshot &) const override { return {}; }

    InternalSearchResult search(
        const MatchDescriptor &,
        const ReadyMaterializedIndexPartSnapshot &,
        size_t,
        ContextPtr) const override
    {
        return {};
    }

    void prepareBuild(const AlgorithmBuildContext &, const Block &) override { ++prepare_calls; }
    void buildAlgorithmPrivate(const AlgorithmBuildContext &) override { ++build_calls; }
    void finishBuild(const AlgorithmBuildContext &) override { ++finish_calls; }

    size_t prepare_calls = 0;
    size_t build_calls = 0;
    size_t finish_calls = 0;
};

}


TEST(MaterializedIndexBuildTaskTest, EmptyStagesStateMachineAdvances)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        /*source_parts_=*/{},
        &algorithm,
        /*storage_=*/nullptr,
        /*new_part_name_=*/"materialized-index-0_0_0_0",
        /*source_storage_=*/nullptr,
        /*source_snapshot_=*/nullptr,
        /*source_metadata_=*/nullptr,
        /*context_=*/nullptr,
        /*output_storage_=*/nullptr,
        /*intermediate_storage_=*/nullptr,
        /*memory_budget_bytes_=*/0);

    /// Six stages whose execute() all return false: the driver performs
    /// six iterations total (five return true, the last returns false after
    /// fulfilling the promise).
    size_t true_returns = 0;
    while (task.execute())
        ++true_returns;

    EXPECT_EQ(true_returns, 5U);

    /// Stage 1 never sees a block (no source parts), so prepareBuild is not
    /// invoked. Stages 3 and 4 are one-shot unconditional algorithm calls,
    /// so both fire exactly once on the empty path.
    EXPECT_EQ(algorithm.prepare_calls, 0U);
    EXPECT_EQ(algorithm.build_calls, 1U);
    EXPECT_EQ(algorithm.finish_calls, 1U);
}


TEST(MaterializedIndexBuildTaskTest, SkeletonPromiseResolvesWithEmptyPart)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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

    /// The skeleton stage 6 leaves new_mi_part null on purpose; the promise
    /// is still fulfilled so waiters never block.
    auto part = future.get();
    EXPECT_EQ(part, nullptr);
}


TEST(MaterializedIndexBuildTaskTest, CancelIsIdempotent)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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
class MaterializedIndexBuildTaskStage6Test : public ::testing::Test
{
protected:
    void SetUp() override
    {
        temp_dir = std::make_unique<Poco::TemporaryFile>();
        temp_dir->createDirectories();
        disk = std::make_shared<DiskLocal>("materialized_index_build_test_disk", temp_dir->path());
        disk->createDirectories("output/part");
        volume = std::make_shared<SingleDiskVolume>("materialized_index_build_test_vol", disk, 0);
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

    std::unique_ptr<Poco::TemporaryFile> temp_dir;
    std::shared_ptr<DiskLocal> disk;
    VolumePtr volume;
    MutableDataPartStoragePtr output_storage;
};


TEST_F(MaterializedIndexBuildTaskStage6Test, WritesAllFourMetadataFiles)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        /*source_parts_=*/{},
        &algorithm,
        /*storage_=*/nullptr,
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
    EXPECT_TRUE(output_storage->existsFile("checksum.txt"));
    EXPECT_TRUE(output_storage->existsFile("txn_version.txt"));
}


TEST_F(MaterializedIndexBuildTaskStage6Test, HeaderJsonCarriesAlgorithmIdentityAndVersion)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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
    EXPECT_EQ(obj->getValue<UInt64>("locator_format_version"), MaterializedIndexPartReverseLookup::LOCATOR_FORMAT_VERSION);
    EXPECT_EQ(obj->getValue<UInt64>("locator_tombstone_dict_id"), MaterializedIndexPartReverseLookup::TOMBSTONE_DICT_ID);
    EXPECT_EQ(obj->getValue<UInt64>("segment_count"), 0U);
    EXPECT_EQ(obj->getValue<UInt64>("coverage_source_part_count"), 0U);
}


TEST_F(MaterializedIndexBuildTaskStage6Test, TxnVersionReservesZero)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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

    /// D-23: literal "0\n" (3 bytes).
    EXPECT_EQ(readFile("txn_version.txt"), std::string{"0\n"});
}


TEST_F(MaterializedIndexBuildTaskStage6Test, CoverageJsonEmptyForZeroSourceParts)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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


TEST_F(MaterializedIndexBuildTaskStage6Test, ChecksumTxtEmptyWhenNoDataFilesProduced)
{
    BuildOnlyMockAlgorithm algorithm;

    BuildTask task(
        {},
        &algorithm,
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

    /// Zero source parts => no stable_mapping / mutable_mapping / dict files,
    /// so the checksum file exists but is empty. Meta files are always
    /// excluded from the checksum set.
    EXPECT_EQ(readFile("checksum.txt"), std::string{});
}
