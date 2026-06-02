#include <Storages/Reflection/ANNIndex/ANNIndexPartWriter.h>

#include <Compression/CompressionFactory.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Common/TransactionID.h>


namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsFloat ratio_of_defaults_for_sparse_serialization;
    extern const MergeTreeSettingsMergeTreeSerializationInfoVersion serialization_info_version;
    extern const MergeTreeSettingsMergeTreeStringSerializationVersion string_serialization_version;
    extern const MergeTreeSettingsMergeTreeNullableSerializationVersion nullable_serialization_version;
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
    extern const MergeTreeSettingsBool propagate_types_serialization_versions_to_nested_types;
}

AnnPartStream openAnnPartStream(
    MergeTreeData & inner,
    const String & part_name,
    UUID part_uuid,
    const String & source_partition_id,
    const MutableDataPartStoragePtr & output_storage)
{
    AnnPartStream b;
    b.inner_metadata = inner.getInMemoryMetadataPtr(inner.getContext(), /*bypass_metadata_cache=*/false);
    b.columns = b.inner_metadata->getColumns().getAllPhysical();

    auto part_info = markAsANNIndexPartInfo(MergeTreePartInfo::fromPartName(part_name, inner.format_version));
    auto settings = inner.getSettings();
    b.part = std::make_shared<MergeTreeDataPartWide>(
        inner, *settings, part_name, part_info, output_storage, /*parent_part_=*/nullptr);
    b.part->is_temp = true;
    b.part->uuid = part_uuid;

    SerializationInfo::Settings info_settings
    {
        (*settings)[MergeTreeSetting::ratio_of_defaults_for_sparse_serialization],
        false,
        (*settings)[MergeTreeSetting::serialization_info_version],
        (*settings)[MergeTreeSetting::string_serialization_version],
        (*settings)[MergeTreeSetting::nullable_serialization_version],
        (*settings)[MergeTreeSetting::map_serialization_version],
        (*settings)[MergeTreeSetting::propagate_types_serialization_versions_to_nested_types],
    };
    SerializationInfoByName infos(b.columns, info_settings);
    b.part->setColumns(b.columns, infos, b.inner_metadata->getMetadataVersion());

    /// All rows in the part share the same `_source_partition_id`, so the
    /// partition value and minmax are constant and can be seeded once.
    b.part->partition.value = Row{Field(source_partition_id)};
    b.minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();
    {
        Block seed;
        seed.insert(ColumnWithTypeAndName(
            DataTypeString().createColumnConst(1, Field(source_partition_id))->convertToFullColumnIfConst(),
            std::make_shared<DataTypeString>(),
            ANN_INDEX_SOURCE_PARTITION_ID_COLUMN));
        b.minmax_idx->update(seed, MergeTreeData::getMinMaxColumns(b.inner_metadata->getPartitionKey(), settings));
    }
    b.part->setMinMaxIndex(b.minmax_idx);

    auto codec = inner.getContext()->chooseCompressionCodec(0, 0);
    const auto & index_factory = MergeTreeIndexFactory::instance();
    b.stream = std::make_unique<MergedBlockOutputStream>(
        b.part,
        settings,
        b.inner_metadata,
        b.columns,
        index_factory.getMany(b.inner_metadata->getSecondaryIndices()),
        codec,
        std::make_shared<MergeTreeIndexGranularityAdaptive>(),
        Tx::NonTransactionalTID,
        /*part_uncompressed_bytes=*/0,
        /*reset_columns_=*/false,
        /*blocks_are_granules_size=*/false,
        inner.getContext()->getWriteSettings(),
        /*written_offset_substreams=*/nullptr);
    return b;
}

}
