#include <gtest/gtest.h>

#include <Storages/Reflection/ANNIndex/CoverageMap.h>

#include <Core/UUID.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

using namespace DB;

namespace
{

UUID mkUuid(UInt64 lo, UInt64 hi = 0)
{
    return UUID{UInt128{lo, hi}};
}

CoverageEntry mkEntry(UUID uuid, UInt64 rows)
{
    CoverageEntry entry;
    entry.source_part_uuid = uuid;
    entry.rows = rows;
    return entry;
}

CoverageEntry mkEntryWithPartInfo(UUID uuid, UInt64 rows, String partition_id, Int64 min_block, Int64 max_block, UInt32 level, Int64 mutation)
{
    auto entry = mkEntry(uuid, rows);
    entry.source_part_name = partition_id + "_" + std::to_string(min_block) + "_" + std::to_string(max_block);
    entry.partition_id = std::move(partition_id);
    entry.min_block = min_block;
    entry.max_block = max_block;
    entry.level = level;
    entry.mutation = mutation;
    entry.has_part_info = true;
    return entry;
}

}

TEST(CoverageMapTest, ReplaceAllThenReadUuids)
{
    /// Two materialized-index-parts cover {U1,U2,U3} and {U2,U3,U4} respectively. The covered
    /// set must dedupe to four UUIDs (U2 / U3 are shared).
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);
    UUID u4 = mkUuid(4);

    std::vector<std::pair<UUID, std::vector<CoverageEntry>>> snapshot;
    snapshot.emplace_back(ann_index_a, std::vector<CoverageEntry>{mkEntry(u1, 100), mkEntry(u2, 200), mkEntry(u3, 300)});
    snapshot.emplace_back(ann_index_b, std::vector<CoverageEntry>{mkEntry(u2, 200), mkEntry(u3, 300), mkEntry(u4, 400)});

    m.replaceAll(std::move(snapshot));

    auto covered = m.coveredSourceUuids();
    EXPECT_EQ(covered.size(), 4u);
    EXPECT_TRUE(covered.contains(u1));
    EXPECT_TRUE(covered.contains(u2));
    EXPECT_TRUE(covered.contains(u3));
    EXPECT_TRUE(covered.contains(u4));
}

TEST(CoverageMapTest, AppendFromBuildAccumulates)
{
    /// Build path adds two materialized-index-parts with disjoint source UUIDs; rows must sum
    /// across distinct source UUIDs (max-aggregation does not collapse them
    /// since the keys differ).
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10), mkEntry(u2, 20)});
    m.appendFromBuild(ann_index_b, {mkEntry(u3, 30)});

    auto covered = m.coveredSourceUuids();
    EXPECT_EQ(covered.size(), 3u);
    EXPECT_TRUE(covered.contains(u1));
    EXPECT_TRUE(covered.contains(u2));
    EXPECT_TRUE(covered.contains(u3));
    EXPECT_EQ(m.coveredRows(), 60u);
}

TEST(CoverageMapTest, ConcurrentBuildCommitsMergeDisjointCoverage)
{
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);
    UUID u4 = mkUuid(4);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10), mkEntry(u2, 20)});
    m.appendFromBuild(ann_index_b, {mkEntry(u3, 30), mkEntry(u4, 40)});

    auto by_ann_index_part = m.coverageEntriesByMiPartUuid();
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_a));
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_b));
    EXPECT_EQ(by_ann_index_part.at(ann_index_a).size(), 2u);
    EXPECT_EQ(by_ann_index_part.at(ann_index_b).size(), 2u);
    std::unordered_set<UUID> active{u1, u2, u3, u4};
    EXPECT_TRUE(m.isFullyCovering(active));
    EXPECT_EQ(m.coveredRows(), 100u);
}

TEST(CoverageMapTest, EntriesBySourceUuidPreservesPartInfo)
{
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID u1 = mkUuid(1);

    m.appendFromBuild(ann_index_a, {mkEntryWithPartInfo(u1, 10, "p", 1, 3, 2, 0)});

    auto entries = m.coverageEntriesBySourceUuid();
    ASSERT_TRUE(entries.contains(u1));
    const auto & entry = entries.at(u1);
    EXPECT_EQ(entry.rows, 10u);
    EXPECT_EQ(entry.partition_id, "p");
    EXPECT_EQ(entry.min_block, 1);
    EXPECT_EQ(entry.max_block, 3);
    EXPECT_EQ(entry.level, 2u);
    EXPECT_EQ(entry.mutation, 0);
    EXPECT_TRUE(entry.has_part_info);
}

TEST(CoverageMapTest, EntriesByMiPartAndReverseLookup)
{
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10), mkEntry(u2, 20)});
    m.appendFromBuild(ann_index_b, {mkEntry(u3, 30)});

    auto by_ann_index_part = m.coverageEntriesByMiPartUuid();
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_a));
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_b));
    EXPECT_EQ(by_ann_index_part.at(ann_index_a).size(), 2u);
    EXPECT_EQ(by_ann_index_part.at(ann_index_b).size(), 1u);

    auto affected = m.miPartUuidsCoveringAnySourceUuid({u2, u3});
    EXPECT_EQ(affected.size(), 2u);
    EXPECT_TRUE(affected.contains(ann_index_a));
    EXPECT_TRUE(affected.contains(ann_index_b));
}

TEST(CoverageMapTest, ApplyRemapReplacesOldMiPart)
{
    /// mi_A initially covers {U1,U2}. A remap produces mi_B covering
    /// {U1,U2,U3}, retiring mi_A. Resulting covered set must be exactly
    /// {U1,U2,U3}; mi_A's entries must no longer be referenced.
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10), mkEntry(u2, 20)});
    m.applyRemap(ann_index_b, ann_index_a, {mkEntry(u1, 10), mkEntry(u2, 20), mkEntry(u3, 30)}, {});

    auto covered = m.coveredSourceUuids();
    EXPECT_EQ(covered.size(), 3u);
    EXPECT_TRUE(covered.contains(u1));
    EXPECT_TRUE(covered.contains(u2));
    EXPECT_TRUE(covered.contains(u3));

    /// Drop ann_index_a again — it must be a no-op since `applyRemap` already retired it.
    m.dropMiPart(ann_index_a);
    EXPECT_EQ(m.coveredSourceUuids().size(), 3u);
}

TEST(CoverageMapTest, ApplyRemapBatchReplacesOldMiPartsAtomically)
{
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID ann_index_c = mkUuid(0xC);
    UUID ann_index_d = mkUuid(0xD);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);
    UUID u4 = mkUuid(4);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10)});
    m.appendFromBuild(ann_index_b, {mkEntry(u2, 20)});
    m.applyRemapBatch({
        {ann_index_c, ann_index_a, {mkEntry(u1, 10), mkEntry(u3, 30)}},
        {ann_index_d, ann_index_b, {mkEntry(u2, 20), mkEntry(u4, 40)}},
    });

    auto by_ann_index_part = m.coverageEntriesByMiPartUuid();
    EXPECT_FALSE(by_ann_index_part.contains(ann_index_a));
    EXPECT_FALSE(by_ann_index_part.contains(ann_index_b));
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_c));
    ASSERT_TRUE(by_ann_index_part.contains(ann_index_d));
    EXPECT_EQ(by_ann_index_part.at(ann_index_c).size(), 2u);
    EXPECT_EQ(by_ann_index_part.at(ann_index_d).size(), 2u);

    auto covered = m.coveredSourceUuids();
    EXPECT_EQ(covered.size(), 4u);
    EXPECT_TRUE(covered.contains(u1));
    EXPECT_TRUE(covered.contains(u2));
    EXPECT_TRUE(covered.contains(u3));
    EXPECT_TRUE(covered.contains(u4));
    EXPECT_EQ(m.coveredRows(), 100u);
}

TEST(CoverageMapTest, ApplyCompactReplacesSeveralOldMiParts)
{
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID ann_index_b = mkUuid(0xB);
    UUID ann_index_c = mkUuid(0xC);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 10)});
    m.appendFromBuild(ann_index_b, {mkEntry(u2, 20)});
    m.applyCompact(
        ann_index_c,
        {ann_index_a, ann_index_b},
        {mkEntry(u1, 10), mkEntry(u2, 20), mkEntry(u3, 30)});

    auto covered = m.coveredSourceUuids();
    EXPECT_EQ(covered.size(), 3u);
    EXPECT_TRUE(covered.contains(u1));
    EXPECT_TRUE(covered.contains(u2));
    EXPECT_TRUE(covered.contains(u3));

    m.dropMiPart(ann_index_a);
    m.dropMiPart(ann_index_b);
    EXPECT_EQ(m.coveredSourceUuids().size(), 3u);
}

TEST(CoverageMapTest, DropMiPartIdempotent)
{
    /// Two consecutive drops of the same materialized-index-part must not throw, and after
    /// the only materialized-index-part is dropped the covered set is empty.
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID u1 = mkUuid(1);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 1)});
    m.dropMiPart(ann_index_a);
    EXPECT_NO_THROW(m.dropMiPart(ann_index_a));
    EXPECT_TRUE(m.coveredSourceUuids().empty());
}

TEST(CoverageMapTest, IsFullyCoveringSuperset)
{
    /// Coverage strictly larger than the active set still satisfies the
    /// "covers everything currently active" predicate.
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 1), mkEntry(u2, 2), mkEntry(u3, 3)});

    std::unordered_set<UUID> active{u1, u2};
    EXPECT_TRUE(m.isFullyCovering(active));
}

TEST(CoverageMapTest, IsFullyCoveringMissingFails)
{
    /// One active source UUID is missing from coverage — predicate must fail.
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID u1 = mkUuid(1);
    UUID u2 = mkUuid(2);
    UUID u3 = mkUuid(3);

    m.appendFromBuild(ann_index_a, {mkEntry(u1, 1), mkEntry(u2, 2)});

    std::unordered_set<UUID> active{u1, u2, u3};
    EXPECT_FALSE(m.isFullyCovering(active));
}

TEST(CoverageMapTest, WaitForFullCoverageWakesOnAppend)
{
    /// A waiter blocks; another thread appends after ~50 ms; the wait must
    /// return true and the elapsed time must be at least the producer's delay.
    CoverageMap m;
    UUID ann_index_a = mkUuid(0xA);
    UUID u1 = mkUuid(1);
    std::unordered_set<UUID> active{u1};

    constexpr auto producer_delay = std::chrono::milliseconds(50);
    constexpr auto wait_budget = std::chrono::milliseconds(1000);

    std::atomic<bool> producer_started{false};
    std::thread producer([&]
    {
        producer_started.store(true);
        std::this_thread::sleep_for(producer_delay);
        m.appendFromBuild(ann_index_a, {mkEntry(u1, 1)});
    });

    /// Make sure the producer thread has been scheduled before we start
    /// timing — otherwise spawn latency leaks into our `elapsed` budget.
    while (!producer_started.load())
        std::this_thread::yield();

    auto t0 = std::chrono::steady_clock::now();
    bool ok = m.waitForFullCoverage(active, wait_budget);
    auto elapsed = std::chrono::steady_clock::now() - t0;

    producer.join();
    EXPECT_TRUE(ok);
    EXPECT_LT(elapsed, wait_budget);
}

TEST(CoverageMapTest, WaitForFullCoverageTimesOut)
{
    /// Nobody appends — the wait must time out cleanly and return false after
    /// roughly the requested budget.
    CoverageMap m;
    UUID u1 = mkUuid(1);
    std::unordered_set<UUID> active{u1};

    constexpr auto budget = std::chrono::milliseconds(50);
    auto t0 = std::chrono::steady_clock::now();
    bool ok = m.waitForFullCoverage(active, budget);
    auto elapsed = std::chrono::steady_clock::now() - t0;

    EXPECT_FALSE(ok);
    EXPECT_GE(elapsed, budget);
}
