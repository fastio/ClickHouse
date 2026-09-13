#include <Storages/MergeTree/tests/gtest_per_key_map_context.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeMapKeyColumns.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Compression/CompressionFactory.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{
class MapKeyColumnsMergeTestAccessor
{
public:
    static void checkFinalize(
        const std::shared_ptr<StorageMergeTree> & storage, const StorageMetadataPtr & metadata,
        const ContextMutablePtr & context, const std::filesystem::path & directory, bool map_key)
    {
        auto global = std::make_shared<MergeTask::GlobalRuntimeContext>();
        auto local = std::make_shared<MergeTask::VerticalMergeRuntimeContext>();
        global->context = context;
        global->data = storage.get();
        global->data_settings = storage->getSettings();
        global->metadata_snapshot = metadata;
        PartitionActionBlocker blocker;
        global->merges_blocker = &blocker;
        auto future = std::make_shared<FutureMergedMutatedPart>();
        future->part_info = MergeTreePartInfo("all", 0, 1, 1);
        future->name = future->part_info.getPartNameV1();
        MergeListElement entry(storage->getStorageID(), future, context);
        global->merge_list_element_ptr = &entry;

        auto disk = std::make_shared<DiskLocal>("test_disk", std::filesystem::absolute(directory).string() + "/");
        auto volume = std::make_shared<SingleDiskVolume>("test_volume", disk);
        auto part_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "", "output");
        auto part = std::make_shared<MergeTreeDataPartWide>(
            *storage, *global->data_settings, future->name, future->part_info, part_storage, nullptr, PartDirIntent::CreateFresh);
        SerializationInfoSettings info;
        info.map_serialization_version = MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS;
        part->setColumns(metadata->getColumns().getAllPhysical(), SerializationInfoByName(metadata->getColumns().getAllPhysical(), info), 0);
        global->new_data_part = part;
        auto map_type = metadata->getColumns().getPhysical("m").type;
        auto physical_type = getValueTypeForMapKeyColumn(assert_cast<const DataTypeMap &>(*map_type).getValueType());
        NameAndTypePair column = map_key ? NameAndTypePair("m", "key_a", map_type, physical_type)
                                        : metadata->getColumns().getPhysical("a");
        global->gathering_columns = {column};
        local->it_name_and_type = global->gathering_columns.begin();
        local->column_sizes.emplace(std::map<String, UInt64>{{column.name, 1}}, NamesAndTypesList{}, global->gathering_columns);
        auto granularity = std::make_shared<MergeTreeIndexGranularityConstant>(4, 4, 1, false);
        SerializationByName serializations;
        if (map_key)
            serializations.emplace(column.name, map_type->getSubcolumnSerialization("key_a", map_type->getSerialization(info)));
        local->column_to = std::make_unique<MergedColumnOnlyOutputStream>(
            part, global->data_settings, metadata, global->gathering_columns, MergeTreeIndices{},
            CompressionCodecFactory::instance().get("LZ4", {}), granularity, 0, nullptr, false, nullptr, serializations);
        auto values = column.type->createColumn();
        for (size_t row = 0; row < 4; ++row)
            values->insert(map_key ? Field(String("value")) : Field(UInt64(row)));
        local->column_to->write(Block{{std::move(values), column.type, column.name}});
        local->column_elems_written = 4;
        global->rows_written = 4;
        local->max_delayed_streams = 40;

        auto source = std::make_shared<NullSource>(std::make_shared<const Block>(Block{{column.type, column.name}}));
        std::weak_ptr<NullSource> reader_resource = source;
        local->column_parts_pipeline = QueryPipeline(source);
        source.reset();
        MergeTask::VerticalMergeStage stage;
        stage.setRuntimeContext(local, global);
        stage.finalizeVerticalMergeForOneColumn();
        if (map_key)
        {
            EXPECT_TRUE(reader_resource.expired());
            EXPECT_TRUE(local->delayed_streams.empty());
        }
        else
            EXPECT_EQ(local->delayed_streams.size(), 1);
        for (auto & writer : local->delayed_streams)
            writer->finish(false);
        local->delayed_streams.clear();
    }
};
}

namespace
{
using PerKeyMapMerge = PerKeyMapStorageTest;

TEST_F(PerKeyMapMerge, ReleasesReaderAndWriterAtKeyBoundary)
{
    DB::MapKeyColumnsMergeTestAccessor::checkFinalize(storage, metadata, context, directory, true);
}

TEST_F(PerKeyMapMerge, OrdinaryColumnsRetainDelayedWrites)
{
    DB::MapKeyColumnsMergeTestAccessor::checkFinalize(storage, metadata, context, directory, false);
}
}
