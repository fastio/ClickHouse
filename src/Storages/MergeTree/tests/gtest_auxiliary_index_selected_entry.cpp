#include <gtest/gtest.h>

#include <Core/UUID.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexSelectedEntry.h>

using namespace DB;

namespace DB
{
String makeAuxiliaryIndexReplicationTaskKeyForTest(
    const FutureAuxiliaryIndexPart & future_part,
    const String & family,
    const String & impl);
}


namespace
{

FutureAuxiliaryIndexPartPtr makeFuturePart(const String & name)
{
    auto fp = std::make_shared<FutureAuxiliaryIndexPart>();
    fp->new_part_name = name;
    fp->kind = FutureAuxiliaryIndexPart::Kind::Build;
    return fp;
}

}


// Tagger ctor / finalize touch StorageANN internals (mutex +
// set), and the storage itself depends on the full MergeTreeData lifecycle.
// Construct-and-finalize behaviour is therefore covered end-to-end by the
// Pack 6 stateless cases. Pack 1's local gtest restricts itself to the
// pure-data semantics: idempotent finalize, delegation to the (optional)
// tagger, and finalize-from-destructor safety net.

TEST(AuxiliaryIndexSelectedEntryTest, BuildEntryFinalizeIsIdempotent)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_0");
    AuxiliaryIndexBuildSelectedEntry entry(std::move(fp), nullptr);

    EXPECT_FALSE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
}

TEST(AuxiliaryIndexSelectedEntryTest, RemapEntryFinalizeIsIdempotent)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_1");
    fp->kind = FutureAuxiliaryIndexPart::Kind::Remap;
    AuxiliaryIndexRemapSelectedEntry entry(std::move(fp), nullptr);

    EXPECT_FALSE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
}

TEST(AuxiliaryIndexSelectedEntryTest, BuildEntryDestructorFinalizesWithoutThrow)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_2");
    {
        AuxiliaryIndexBuildSelectedEntry entry(std::move(fp), nullptr);
        EXPECT_FALSE(entry.finalized);
    }
    SUCCEED();
}

TEST(AuxiliaryIndexSelectedEntryTest, FuturePartCarriesKindBuild)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_3");
    EXPECT_EQ(fp->kind, FutureAuxiliaryIndexPart::Kind::Build);
    EXPECT_EQ(fp->new_part_name, "materialized-index-0_0_0_3");
    EXPECT_TRUE(fp->source_parts_snapshot.empty());
    EXPECT_TRUE(fp->affected_auxiliary_index_parts.empty());
    EXPECT_TRUE(fp->delta_in_source_parts.empty());
    EXPECT_TRUE(fp->delta_out_source_uuids.empty());
    EXPECT_TRUE(fp->replicated_leader_lease_path.empty());
    EXPECT_TRUE(fp->replicated_leader_lease_payload.empty());
    EXPECT_TRUE(fp->replicated_task_lock_path.empty());
    EXPECT_TRUE(fp->replicated_task_lock_payload.empty());
}

TEST(AuxiliaryIndexSelectedEntryTest, ReplicationTaskKeyIgnoresGeneratedOutputIdentity)
{
    auto lhs = makeFuturePart("materialized-index-build_100_100_0");
    lhs->new_part_uuid = UUIDHelpers::makeUUIDv4FromHash("lhs output");
    lhs->delta_out_source_uuids.push_back(UUIDHelpers::makeUUIDv4FromHash("logical source"));

    auto rhs = makeFuturePart("materialized-index-build_200_200_0");
    rhs->new_part_uuid = UUIDHelpers::makeUUIDv4FromHash("rhs output");
    rhs->delta_out_source_uuids = lhs->delta_out_source_uuids;

    EXPECT_EQ(
        makeAuxiliaryIndexReplicationTaskKeyForTest(*lhs, "diskann", "plain"),
        makeAuxiliaryIndexReplicationTaskKeyForTest(*rhs, "diskann", "plain"));
}

TEST(AuxiliaryIndexSelectedEntryTest, ReplicationTaskKeyIncludesLogicalTaskKind)
{
    auto build = makeFuturePart("materialized-index-build_100_100_0");
    build->delta_out_source_uuids.push_back(UUIDHelpers::makeUUIDv4FromHash("logical source"));

    auto remap = makeFuturePart("materialized-index-remap_100_100_0");
    remap->kind = FutureAuxiliaryIndexPart::Kind::Remap;
    remap->remap_kind = AuxiliaryIndexRemapKind::ObsoleteCoverageCleanup;
    remap->delta_out_source_uuids = build->delta_out_source_uuids;

    EXPECT_NE(
        makeAuxiliaryIndexReplicationTaskKeyForTest(*build, "diskann", "plain"),
        makeAuxiliaryIndexReplicationTaskKeyForTest(*remap, "diskann", "plain"));
}

TEST(AuxiliaryIndexSelectedEntryTest, ReplicationTaskKeySortsDeltaOutSourceUUIDs)
{
    auto lhs = makeFuturePart("materialized-index-build_100_100_0");
    lhs->delta_out_source_uuids = {
        UUIDHelpers::makeUUIDv4FromHash("logical source 2"),
        UUIDHelpers::makeUUIDv4FromHash("logical source 1")};

    auto rhs = makeFuturePart("materialized-index-build_100_100_0");
    rhs->delta_out_source_uuids = {
        UUIDHelpers::makeUUIDv4FromHash("logical source 1"),
        UUIDHelpers::makeUUIDv4FromHash("logical source 2")};

    EXPECT_EQ(
        makeAuxiliaryIndexReplicationTaskKeyForTest(*lhs, "diskann", "plain"),
        makeAuxiliaryIndexReplicationTaskKeyForTest(*rhs, "diskann", "plain"));
}

TEST(AuxiliaryIndexSelectedEntryTest, ReplicationTaskKeyIncludesAlgorithmIdentity)
{
    auto fp = makeFuturePart("materialized-index-build_100_100_0");
    fp->delta_out_source_uuids.push_back(UUIDHelpers::makeUUIDv4FromHash("logical source"));

    EXPECT_NE(
        makeAuxiliaryIndexReplicationTaskKeyForTest(*fp, "diskann", "plain"),
        makeAuxiliaryIndexReplicationTaskKeyForTest(*fp, "diskann", "other"));
    EXPECT_NE(
        makeAuxiliaryIndexReplicationTaskKeyForTest(*fp, "diskann", "plain"),
        makeAuxiliaryIndexReplicationTaskKeyForTest(*fp, "other", "plain"));
}
