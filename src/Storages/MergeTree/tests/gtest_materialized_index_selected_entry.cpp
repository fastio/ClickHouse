#include <gtest/gtest.h>

#include <Storages/MaterializedIndex/MaterializedIndexSelectedEntry.h>

using namespace DB;


namespace
{

FutureMaterializedIndexPartPtr makeFuturePart(const String & name)
{
    auto fp = std::make_shared<FutureMaterializedIndexPart>();
    fp->new_part_name = name;
    fp->kind = FutureMaterializedIndexPart::Kind::Build;
    return fp;
}

}


// Tagger ctor / finalize touch StorageMaterializedIndex internals (mutex +
// set), and the storage itself depends on the full MergeTreeData lifecycle.
// Construct-and-finalize behaviour is therefore covered end-to-end by the
// Pack 6 stateless cases. Pack 1's local gtest restricts itself to the
// pure-data semantics: idempotent finalize, delegation to the (optional)
// tagger, and finalize-from-destructor safety net.

TEST(MaterializedIndexSelectedEntryTest, BuildEntryFinalizeIsIdempotent)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_0");
    MaterializedIndexBuildSelectedEntry entry(std::move(fp), nullptr);

    EXPECT_FALSE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
}

TEST(MaterializedIndexSelectedEntryTest, RemapEntryFinalizeIsIdempotent)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_1");
    fp->kind = FutureMaterializedIndexPart::Kind::Remap;
    MaterializedIndexRemapSelectedEntry entry(std::move(fp), nullptr);

    EXPECT_FALSE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
    entry.finalize();
    EXPECT_TRUE(entry.finalized);
}

TEST(MaterializedIndexSelectedEntryTest, BuildEntryDestructorFinalizesWithoutThrow)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_2");
    {
        MaterializedIndexBuildSelectedEntry entry(std::move(fp), nullptr);
        EXPECT_FALSE(entry.finalized);
    }
    SUCCEED();
}

TEST(MaterializedIndexSelectedEntryTest, FuturePartCarriesKindBuild)
{
    auto fp = makeFuturePart("materialized-index-0_0_0_3");
    EXPECT_EQ(fp->kind, FutureMaterializedIndexPart::Kind::Build);
    EXPECT_EQ(fp->new_part_name, "materialized-index-0_0_0_3");
    EXPECT_TRUE(fp->source_parts_snapshot.empty());
    EXPECT_TRUE(fp->affected_mi_parts.empty());
    EXPECT_TRUE(fp->delta_in_source_parts.empty());
    EXPECT_TRUE(fp->delta_out_source_uuids.empty());
}
