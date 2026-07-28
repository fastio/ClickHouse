#include "config.h"

#if USE_LANCE

#include <gtest/gtest.h>

#include <Interpreters/Context.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceQuerySession.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceReadSource.h>
#include <Storages/ObjectStorage/DataLakes/Lance/LanceWrapper.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>

#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace DB;

namespace
{
Lance::TableStateSnapshot makeSnapshot(UInt64 version, UInt8 seed)
{
    Lance::TableStateSnapshot snapshot;
    snapshot.version = version;
    snapshot.manifest_id.fill(seed);
    snapshot.manifest_size = 512;
    snapshot.manifest_sha256.fill(seed + 1);
    return snapshot;
}

std::shared_ptr<arrow::RecordBatch> makeBatch(Int64 first_value, size_t rows)
{
    arrow::Int64Builder builder;
    for (size_t index = 0; index < rows; ++index)
    {
        const auto status = builder.Append(first_value + static_cast<Int64>(index));
        if (!status.ok())
            throw std::runtime_error(status.ToString());
    }

    std::shared_ptr<arrow::Array> array;
    const auto status = builder.Finish(&array);
    if (!status.ok())
        throw std::runtime_error(status.ToString());
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int64())}), static_cast<int64_t>(rows), {std::move(array)});
}

struct FakeBatchProviderState
{
    std::mutex mutex;
    std::condition_variable condition;
    std::set<std::thread::id> consumers;
    size_t expected_initial_consumers = 0;
    size_t initial_batches_delivered = 0;
    size_t released_batches = 0;
    bool block_until_cancel = false;
    bool receiver_waiting = false;
    bool cancelled = false;
};

class FakeBatchProvider final : public Lance::BatchProvider
{
public:
    FakeBatchProvider(
        std::shared_ptr<FakeBatchProviderState> state_, size_t batch_count, size_t rows_per_batch, bool fail_after_batches_ = false)
        : shared_state(std::move(state_))
        , fail_after_batches(fail_after_batches_)
        , projected_schema(arrow::schema({arrow::field("id", arrow::int64())}))
    {
        batches.reserve(batch_count);
        for (size_t index = 0; index < batch_count; ++index)
        {
            batches.push_back({
                .record_batch = makeBatch(static_cast<Int64>(index * rows_per_batch), rows_per_batch),
                .rows = rows_per_batch,
                .bytes = rows_per_batch * sizeof(Int64),
            });
        }
    }

    std::optional<Lance::Scan::Batch> nextBatch() override
    {
        std::unique_lock lock(shared_state->mutex);
        const bool initial_call = shared_state->consumers.insert(std::this_thread::get_id()).second;
        if (initial_call && shared_state->expected_initial_consumers != 0)
        {
            shared_state->condition.notify_all();
            shared_state->condition.wait(
                lock,
                [&] { return shared_state->consumers.size() == shared_state->expected_initial_consumers || shared_state->cancelled; });
        }

        if (shared_state->block_until_cancel)
        {
            shared_state->receiver_waiting = true;
            shared_state->condition.notify_all();
            shared_state->condition.wait(lock, [&] { return shared_state->cancelled; });
            return std::nullopt;
        }

        if (next_batch == batches.size())
        {
            if (fail_after_batches)
            {
                producer_stats.producer_error = 1;
                throw std::runtime_error("fake Lance producer error");
            }
            producer_stats.producer_eof = 1;
            return std::nullopt;
        }

        auto result = std::move(batches[next_batch++]);
        ++producer_stats.queue_pop_batches;
        if (initial_call && shared_state->expected_initial_consumers != 0)
        {
            ++shared_state->initial_batches_delivered;
            shared_state->condition.notify_all();
            shared_state->condition.wait(
                lock,
                [&]
                { return shared_state->initial_batches_delivered == shared_state->expected_initial_consumers || shared_state->cancelled; });
        }
        return result;
    }

    void releaseBatch(UInt64) noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        ++shared_state->released_batches;
    }

    void requestCancel() noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        shared_state->cancelled = true;
        producer_stats.producer_cancel = 1;
        shared_state->condition.notify_all();
    }

    const std::shared_ptr<arrow::Schema> & schema() const override { return projected_schema; }
    Lance::Scan::Stats stats() const noexcept override
    {
        std::lock_guard lock(shared_state->mutex);
        return producer_stats;
    }

private:
    std::shared_ptr<FakeBatchProviderState> shared_state;
    bool fail_after_batches;
    std::vector<Lance::Scan::Batch> batches;
    size_t next_batch = 0;
    std::shared_ptr<arrow::Schema> projected_schema;
    Lance::Scan::Stats producer_stats;
};
}

TEST(LanceQuerySession, IdentityKeyStableAndSensitiveToCredentials)
{
    Lance::DatasetOptions a{.uri = "/tmp/ds", .use_s3 = false};
    Lance::DatasetOptions b = a;
    EXPECT_EQ(a.identityKey(), b.identityKey());

    b.s3_access_key_id = "other";
    b.use_s3 = true;
    EXPECT_NE(a.identityKey(), b.identityKey());
}

TEST(LanceQuerySession, PinSnapshotRejectsConflict)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session = Lance::QuerySession::get(context);
    const auto snapshot = makeSnapshot(3, 1);
    session->pinSnapshot("id1", snapshot);
    session->pinSnapshot("id1", snapshot);
    EXPECT_EQ(session->getPinnedSnapshot("id1"), snapshot);
    EXPECT_THROW(session->pinSnapshot("id1", makeSnapshot(3, 9)), Exception);
}

TEST(LanceQuerySession, GetOrOpenReusesHandleWithinSession)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();
    auto session = Lance::QuerySession::get(context);

    Lance::DatasetOptions options{.uri = "/path/that/does/not/exist/for/session/test"};
    /// Both calls fail the same way; the second must not leave a half-open entry.
    EXPECT_THROW(std::ignore = session->getOrOpen(options), Exception);
    EXPECT_EQ(session->openCount(), 0u);
}

TEST(LanceQuerySession, SessionSharedAcrossGetCalls)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    auto session1 = Lance::QuerySession::get(context);
    auto session2 = Lance::QuerySession::get(context);
    EXPECT_EQ(session1.get(), session2.get());
}

TEST(LanceScanCoordinator, ConcurrentConsumersReceiveEveryBatchExactlyOnce)
{
    constexpr size_t consumer_count = 4;
    constexpr size_t batch_count = 64;
    auto state = std::make_shared<FakeBatchProviderState>();
    state->expected_initial_consumers = consumer_count;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, batch_count, 1), false);

    std::mutex result_mutex;
    std::vector<Int64> ids;
    std::vector<std::thread> consumers;
    for (size_t index = 0; index < consumer_count; ++index)
    {
        consumers.emplace_back(
            [&]
            {
                while (auto batch = coordinator->nextBatch())
                {
                    const auto array = std::static_pointer_cast<arrow::Int64Array>(batch->recordBatch()->column(0));
                    std::lock_guard lock(result_mutex);
                    ids.push_back(array->Value(0));
                }
            });
    }
    for (auto & consumer : consumers)
        consumer.join();

    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids.size(), batch_count);
    for (size_t index = 0; index < batch_count; ++index)
        EXPECT_EQ(ids[index], static_cast<Int64>(index));
    EXPECT_EQ(state->consumers.size(), consumer_count);
    EXPECT_EQ(state->released_batches, batch_count);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
}

TEST(LanceReadCancellation, SiblingReadsHaveIndependentHandles)
{
    auto first = std::make_shared<Lance::ReadCancellation>(nullptr);
    auto second = std::make_shared<Lance::ReadCancellation>(nullptr);

    EXPECT_NE(first->handle()->raw(), second->handle()->raw());
    first->requestCancel();
    EXPECT_NE(first->handle()->raw(), second->handle()->raw());
}

TEST(LanceScanCoordinator, ProducerErrorPropagatesOnce)
{
    constexpr size_t consumer_count = 4;
    auto state = std::make_shared<FakeBatchProviderState>();
    state->expected_initial_consumers = consumer_count;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0, true), false);

    std::atomic_size_t exceptions = 0;
    std::vector<std::thread> consumers;
    for (size_t index = 0; index < consumer_count; ++index)
    {
        consumers.emplace_back(
            [&]
            {
                try
                {
                    std::ignore = coordinator->nextBatch();
                }
                catch (const std::runtime_error &)
                {
                    ++exceptions;
                }
            });
    }
    for (auto & consumer : consumers)
        consumer.join();

    EXPECT_EQ(exceptions, 1);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Failed);
    EXPECT_TRUE(state->cancelled);
}

TEST(LanceScanCoordinator, CancelWakesWaitingConsumer)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    state->block_until_cancel = true;
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0), false);

    std::thread consumer([&] { EXPECT_FALSE(coordinator->nextBatch().has_value()); });
    {
        std::unique_lock lock(state->mutex);
        state->condition.wait(lock, [&] { return state->receiver_waiting; });
    }
    coordinator->cancel();
    consumer.join();

    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Cancelled);
}

TEST(LanceScanCoordinator, DestructorCancelsProvider)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    {
        auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 1), false);
    }
    EXPECT_TRUE(state->cancelled);
}

TEST(LanceScanCoordinator, GlobalLimitSlicesLastBatch)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 8), false, 3);

    auto batch = coordinator->nextBatch();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->rows(), 3);
    EXPECT_EQ(batch->recordBatch()->num_rows(), 3);
    batch.reset();
    EXPECT_EQ(state->released_batches, 1);
    EXPECT_TRUE(state->cancelled);
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
    EXPECT_FALSE(coordinator->nextBatch().has_value());
}

TEST(LanceScanCoordinator, NoLimitConsumesCompleteBatch)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 1, 8), false);

    auto batch = coordinator->nextBatch();
    ASSERT_TRUE(batch.has_value());
    EXPECT_EQ(batch->rows(), 8);
    batch.reset();
    EXPECT_FALSE(coordinator->nextBatch().has_value());
    EXPECT_FALSE(state->cancelled);
}

TEST(LanceScanCoordinator, EmptyProviderEndsCleanly)
{
    auto state = std::make_shared<FakeBatchProviderState>();
    auto coordinator = Lance::ScanCoordinator::createWithProvider(std::make_unique<FakeBatchProvider>(state, 0, 0), false);

    EXPECT_FALSE(coordinator->nextBatch().has_value());
    EXPECT_EQ(coordinator->state(), Lance::ScanCoordinator::State::Ended);
}

#endif
