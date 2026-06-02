#include <Storages/Reflection/ANNIndex/ANNIndexPartMetadata.h>

#include <Common/Exception.h>
#include <Common/escapeForFileName.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IDisk.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteSettings.h>
#include <IO/copyData.h>
#include <Storages/Reflection/ANNIndex/ANNIndexPartName.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <functional>
#include <unordered_set>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

String relativeToPartRoot(const String & part_root, const String & disk_path)
{
    const String prefix = part_root.ends_with('/') ? part_root : part_root + "/";
    if (!disk_path.starts_with(prefix))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot map disk path {} below materialized-index part root {}", disk_path, part_root);
    return disk_path.substr(prefix.size());
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
        VersionMetadata::TXN_VERSION_METADATA_FILE_NAME,
    };

    return files_without_checksums.contains(file_name);
}

std::vector<String> listChecksumFiles(const IDataPartStorage & part_storage)
{
    const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&part_storage);
    if (!disk_storage)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Cannot recursively calculate ANNIndex checksums for non-disk part storage {}", part_storage.getRelativePath());

    std::vector<String> result;
    const auto disk = disk_storage->getDisk();
    const String part_root = part_storage.getRelativePath();
    std::vector<String> dirs{part_root};

    while (!dirs.empty())
    {
        String current = std::move(dirs.back());
        dirs.pop_back();

        for (auto it = disk->iterateDirectory(current); it->isValid(); it->next())
        {
            const String entry_path = it->path();
            const String rel_path = relativeToPartRoot(part_root, entry_path);
            if (disk->existsFile(entry_path))
            {
                if (rel_path.find('/') == String::npos && isFileWithoutChecksum(rel_path))
                    continue;
                result.push_back(rel_path);
                continue;
            }

            if (disk->existsDirectory(entry_path))
                dirs.push_back(entry_path);
        }
    }

    return result;
}

MergeTreeDataPartChecksums calculateChecksums(
    const IDataPartStorage & part_storage,
    const ReadSettings & read_settings)
{
    MergeTreeDataPartChecksums checksums;
    for (const auto & file_name : listChecksumFiles(part_storage))
    {
        checksums.addFile(file_name, checksumFile(part_storage, file_name, read_settings));
    }
    return checksums;
}

}

void writeANNIndexPartUUIDFile(
    IDataPartStorage & part_storage,
    const UUID & uuid,
    const WriteSettings & write_settings)
{
    part_storage.removeFileIfExists(IMergeTreeDataPart::UUID_FILE_NAME);
    if (uuid == UUIDHelpers::Nil)
        return;

    auto out = part_storage.writeFile(IMergeTreeDataPart::UUID_FILE_NAME, 4096, write_settings);
    writeUUIDText(uuid, *out);
    out->finalize();
}

MergeTreeDataPartChecksums finalizeANNIndexPartChecksums(
    IDataPartStorage & part_storage,
    const MergeTreeData * inner_storage)
{
    auto write_settings = getWriteSettings(inner_storage);
    auto read_settings = getReadSettings(inner_storage);

    /// Drop the checksum set written by `finalizePart` (columns only) so the
    /// full-directory recompute below is authoritative.
    part_storage.removeFileIfExists("checksums.txt");

    auto checksums = calculateChecksums(part_storage, read_settings);
    writePlainFile(part_storage, "checksums.txt", write_settings, [&](WriteBuffer & out)
    {
        checksums.write(out);
    });

    return checksums;
}

}
