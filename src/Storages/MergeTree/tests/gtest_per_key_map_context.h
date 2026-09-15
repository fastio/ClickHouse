#pragma once
#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTFunction.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageSnapshot.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

#include <DataTypes/DataTypeFactory.h>
#include <Poco/Util/MapConfiguration.h>
#include <filesystem>
#include <unistd.h>

class PerKeyMapStorageTest : public testing::Test
{
protected:
    DB::ContextMutablePtr context;
    std::shared_ptr<DB::StorageMergeTree> storage;
    std::shared_ptr<DB::StorageInMemoryMetadata> metadata;
    std::filesystem::path directory;

    void SetUp() override
    {
        using namespace DB;
        directory = std::filesystem::path("tmp") / ("per_key_context_" + std::to_string(getpid()) + "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(directory);
        MainThreadStatus::getInstance();
        tryRegisterFunctions();
        tryRegisterAggregateFunctions();

        getActivePartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getOutdatedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getUnexpectedPartsLoadingThreadPool().initializeWithDefaultSettingsIfNotInitialized();
        getPartsCleaningThreadPool().initializeWithDefaultSettingsIfNotInitialized();

        context = Context::createCopy(getContext().context);
        /// unit_tests_dbms is not a Poco Application. TreeRewriter::normalize
        /// lazily creates UDF storage via getConfigRef, which otherwise throws
        /// NullPointerException from Application::instance().
        context->setConfig(Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration));
        context->setSetting("optimize_functions_to_subcolumns", true);

        metadata = std::make_shared<StorageInMemoryMetadata>();

        ColumnsDescription columns;
        columns.add(ColumnDescription("a", std::make_shared<DataTypeUInt64>()));
        columns.add(ColumnDescription("m", DataTypeFactory::instance().get("Map(String, LowCardinality(Nullable(String)))")));
        metadata->setColumns(columns);

        auto order_by_ast = makeASTFunction("tuple");
        metadata->sorting_key = KeyDescription::getKeyFromAST(order_by_ast, metadata->columns, {}, context);
        metadata->primary_key = KeyDescription::getKeyFromAST(order_by_ast, metadata->columns, {}, context);
        metadata->primary_key.definition_ast = nullptr;

        metadata->partition_key = KeyDescription::getKeyFromAST(nullptr, metadata->columns, {}, context);

        auto minmax_columns = metadata->getColumnsRequiredForPartitionKey();
        auto partition_key = metadata->partition_key.expression_list_ast->clone();
        metadata->minmax_count_projection.emplace(
            ProjectionDescription::getMinMaxCountProjection(columns, partition_key, minmax_columns, metadata->primary_key, &metadata->partition_key, context));

        auto storage_settings = std::make_unique<MergeTreeSettings>(context->getMergeTreeSettings());

        storage_settings->set("map_serialization_version", String("with_key_columns"));
        storage_settings->set("map_serialization_version_for_zero_level_parts", String("with_key_columns"));
        storage = std::make_shared<StorageMergeTree>(
            StorageID("test_db", "test_table"),
            directory.string() + "/table/",
            *metadata,
            LoadingStrictnessLevel::ATTACH,
            context,
            /*date_column_name=*/"",
            MergeTreeData::MergingParams{},
            std::move(storage_settings));

    }

    void TearDown() override
    {
        if (storage)
            storage->flushAndShutdown();
        storage.reset();
        std::filesystem::remove_all(directory);
    }
};
