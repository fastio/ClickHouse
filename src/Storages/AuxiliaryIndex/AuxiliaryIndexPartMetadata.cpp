#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartMetadata.h>

#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/copyData.h>
#include <Storages/AuxiliaryIndex/AuxiliaryIndexPartName.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <functional>
#include <unordered_set>


namespace DB
{

namespace
{

NamesAndTypesList getAuxiliaryIndexColumns(const MergeTreeData * inner_storage)
{
    if (inner_storage)
    {
        auto metadata_snapshot = inner_storage->getInMemoryMetadataPtr(inner_storage->getContext(), false);
        return metadata_snapshot->getColumns().getAllPhysical();
    }

    return {NameAndTypePair{"_index_marker", std::make_shared<DataTypeUInt8>()}};
}

int32_t getAuxiliaryIndexMetadataVersion(const MergeTreeData * inner_storage)
{
    if (!inner_storage)
        return 0;

    auto metadata_snapshot = inner_storage->getInMemoryMetadataPtr(inner_storage->getContext(), false);
    return metadata_snapshot->getMetadataVersion();
}

WriteSettings getWriteSettings(const MergeTreeData * inner_storage)
{
    if (inner_storage)
        return inner_storage->getContext()->getWriteSettings();
    return {};
}

ReadSettings getReadSettings(const MergeTreeData * inner_storage)
{
    if (inner_storage)
        return inner_storage->getContext()->getReadSettings();
    return {};
}

void writePlainFile(
    IDataPartStorage & part_storage,
    const String & file_name,
    const WriteSettings & write_settings,
    const std::function<void(WriteBuffer &)> & write)
{
    auto out = part_storage.writeFile(file_name, 4096, write_settings);
    write(*out);
    out->finalize();
}

MergeTreeDataPartChecksum checksumFile(
    const IDataPartStorage & part_storage,
    const String & file_name,
    const ReadSettings & read_settings)
{
    auto in = part_storage.readFile(file_name, read_settings, std::nullopt);
    NullWriteBuffer null_out;
    HashingWriteBuffer hashing_out(null_out);
    copyData(*in, hashing_out);
    hashing_out.finalize();
    return {hashing_out.count(), hashing_out.getHash()};
}

bool isFileWithoutChecksum(const String & file_name)
{
    static const std::unordered_set<String> files_without_checksums =
    {
        "checksums.txt",
        "columns.txt",
        IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME,
        IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME,
        IMergeTreeDataPart::METADATA_VERSION_FILE_NAME,
        IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME,
    };

    return files_without_checksums.contains(file_name);
}

MergeTreeDataPartChecksums calculateChecksums(
    const IDataPartStorage & part_storage,
    const ReadSettings & read_settings)
{
    MergeTreeDataPartChecksums checksums;
    for (auto it = part_storage.iterate(); it->isValid(); it->next())
    {
        const String file_name = it->name();
        if (!it->isFile() || isFileWithoutChecksum(file_name))
            continue;

        checksums.addFile(file_name, checksumFile(part_storage, file_name, read_settings));
    }
    return checksums;
}

String getAuxiliaryIndexSourcePartitionMinMaxFileName(
    const IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage)
{
    String column_file_name;
    if (inner_storage)
        column_file_name = IMergeTreeDataPart::MinMaxIndex::getFileColumnName(
            AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN,
            inner_storage->getSettings(),
            part_storage);
    else
        column_file_name = escapeForFileName(AUXILIARY_INDEX_SOURCE_PARTITION_ID_COLUMN);

    return "minmax_" + column_file_name + ".idx";
}

void writeAuxiliaryIndexPartitionMetadata(
    IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage,
    const String & source_partition_id,
    const WriteSettings & write_settings)
{
    auto data_type = std::make_shared<DataTypeString>();
    const Field source_partition_field{source_partition_id};

    writePlainFile(part_storage, "partition.dat", write_settings, [&](WriteBuffer & out)
    {
        data_type->getDefaultSerialization()->serializeBinary(source_partition_field, out, {});
    });

    const String minmax_file_name = getAuxiliaryIndexSourcePartitionMinMaxFileName(part_storage, inner_storage);
    writePlainFile(part_storage, minmax_file_name, write_settings, [&](WriteBuffer & out)
    {
        auto serialization = data_type->getDefaultSerialization();
        serialization->serializeBinary(source_partition_field, out, {});
        serialization->serializeBinary(source_partition_field, out, {});
    });
}

}

MergeTreeDataPartChecksums writeAuxiliaryIndexPartMetadata(
    IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage,
    UInt64 rows_count,
    const String & source_partition_id,
    UUID part_uuid)
{
    auto write_settings = getWriteSettings(inner_storage);
    auto read_settings = getReadSettings(inner_storage);

    /// These were the pre-exchange materialized-index metadata files. Keeping
    /// them outside `checksums.txt` makes replicated fetch compare different
    /// checksum sets on the sender and receiver, so new parts do not write them.
    part_storage.removeFileIfExists("checksum.txt");
    part_storage.removeFileIfExists(IMergeTreeDataPart::TXN_VERSION_METADATA_FILE_NAME);
    part_storage.removeFileIfExists("checksums.txt");

    writePlainFile(part_storage, "columns.txt", write_settings, [&](WriteBuffer & out)
    {
        getAuxiliaryIndexColumns(inner_storage).writeText(out);
    });

    writePlainFile(part_storage, "count.txt", write_settings, [&](WriteBuffer & out)
    {
        writeIntText(rows_count, out);
    });

    writeAuxiliaryIndexPartitionMetadata(part_storage, inner_storage, source_partition_id, write_settings);

    if (part_uuid != UUIDHelpers::Nil)
    {
        writePlainFile(part_storage, IMergeTreeDataPart::UUID_FILE_NAME, write_settings, [&](WriteBuffer & out)
        {
            writeUUIDText(part_uuid, out);
        });
    }

    writePlainFile(part_storage, IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, write_settings, [&](WriteBuffer & out)
    {
        writeIntText(getAuxiliaryIndexMetadataVersion(inner_storage), out);
    });

    auto checksums = calculateChecksums(part_storage, read_settings);
    writePlainFile(part_storage, "checksums.txt", write_settings, [&](WriteBuffer & out)
    {
        checksums.write(out);
    });

    return checksums;
}

}
