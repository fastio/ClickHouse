#include <Common/ProfileEvents.h>
#include <Access/Common/AccessFlags.h>
#include <Access/EnabledRowPolicies.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeTuple.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>

namespace ProfileEvents
{
    extern const Event MapMetadataManifestReads;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCORRECT_DATA;
    extern const int ACCESS_DENIED;
    extern const int TIMEOUT_EXCEEDED;
}

static ASTPtr getCompressionCodecDeltaLZ4()
{
    return makeASTFunction("CODEC",
        make_intrusive<ASTIdentifier>("Delta"),
        make_intrusive<ASTIdentifier>("LZ4"));
}

const String RowExistsColumn::name = "_row_exists";
const DataTypePtr RowExistsColumn::type = std::make_shared<DataTypeUInt8>();

bool isLightweightDeleteAssignment(const ASTAssignment & assignment)
{
    if (assignment.column_name != RowExistsColumn::name)
        return false;
    /// `DELETE FROM` rewrites to `_row_exists = 0`; only that exact literal is a delete. Any other
    /// expression (e.g. `_row_exists = 1` to resurrect rows) is a real update of the deletion mask.
    const auto * literal = assignment.expression()->as<ASTLiteral>();
    return literal && literal->value == Field(static_cast<UInt64>(0));
}

const String BlockNumberColumn::name = "_block_number";
const DataTypePtr BlockNumberColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockNumberColumn::codec = getCompressionCodecDeltaLZ4();

const String BlockOffsetColumn::name = "_block_offset";
const DataTypePtr BlockOffsetColumn::type = std::make_shared<DataTypeUInt64>();
const ASTPtr BlockOffsetColumn::codec = getCompressionCodecDeltaLZ4();

const String PartDataVersionColumn::name = "_part_data_version";
const DataTypePtr PartDataVersionColumn::type = std::make_shared<DataTypeUInt64>();

const String PartitionIdColumn::name = "_partition_id";
const DataTypePtr PartitionIdColumn::type = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());

const String PartitionValueColumn::name = "_partition_value";
DataTypePtr PartitionValueColumn::type(const KeyDescription * partition_key)
{
    auto partition_types = partition_key->sample_block.getDataTypes();
    return std::make_shared<DataTypeTuple>(std::move(partition_types));
}

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection)
{
    const auto & part = part_or_projection.isProjectionPart() ? *part_or_projection.getParentPart() : part_or_projection;

    if (column_name == RowExistsColumn::name)
        return 1ULL;

    if (column_name == BlockNumberColumn::name)
        return part.info.min_block;

    if (column_name == "_part")
        return part.name;

    if (column_name == "_part_uuid")
        return part.uuid;

    if (column_name == "_partition_id")
        return part.info.getPartitionId();

    if (column_name == PartDataVersionColumn::name)
        return part.info.getDataVersion();

    if (column_name == "_partition_value")
        return Tuple(part.partition.value.begin(), part.partition.value.end());

    if (column_name == "_disk_name")
        return part.getDataPartStorage().getDiskName();

    throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "Unexpected const virtual column: {}", column_name);
}

std::vector<Field> readMapKeyColumnsManifest(const IMergeTreeDataPart & part, const NameAndTypePair & column, ContextPtr context)
{
    if (!part.getColumns().contains(column.name))
        return {};
    const auto serialization = part.getSerialization(column.name);
    const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(serialization.get());
    if (!per_key)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Column {} in part {} does not have with_key_columns serialization", column.name, part.name);
    ISerialization::SubstreamPath path;
    path.push_back(ISerialization::Substream::MapKeysInfo);
    auto stream = IMergeTreeDataPart::getStreamNameForColumn(
        column, path, IMergeTreeDataPart::DATA_FILE_EXTENSION, part.getDataPartStorage(), part.storage.getSettings());
    if (!stream)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Missing keys_info stream for column {} in part {}", column.name, part.name);
    auto file = part.getDataPartStorage().readFile(*stream + IMergeTreeDataPart::DATA_FILE_EXTENSION, context->getReadSettings(), std::nullopt);
    CompressedReadBufferFromFile in(std::move(file), true);
    auto keys = per_key->readManifest(in);
    ProfileEvents::increment(ProfileEvents::MapMetadataManifestReads);
    return keys;
}

void checkMapMetadataAccess(const StorageID & id, const Names & columns, ContextPtr context)
{
    context->checkAccess(AccessType::SELECT, id, columns);
    if (auto policy = context->getRowPolicyFilter(id.database_name, id.table_name, RowPolicyFilterType::SELECT_FILTER);
        policy && !policy->isAlwaysTrue())
        throw Exception(ErrorCodes::ACCESS_DENIED, "Map metadata cannot be read from a table with a restrictive row policy");
}

void collectMapKeyColumnsColumnKeys(
    const IMergeTreeDataPart & part, const NamesAndTypesList & columns, MapColumnKeys & keys, ContextPtr context)
{
    for (const auto & column : columns)
    {
        if (!part.getColumns().contains(column.name))
            continue;
        const auto serialization = part.getSerialization(column.name);
        const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(serialization.get());
        if (!per_key)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Column {} in part {} does not have with_key_columns serialization", column.name, part.name);
        auto key_column = per_key->getKeyType()->createColumn();
        for (const auto & key : readMapKeyColumnsManifest(part, column, context))
        {
            if (auto status = context->getProcessListElement(); status && !status->checkTimeLimit())
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Time limit exceeded while enumerating Map keys");
            key_column->insert(key);
            WriteBufferFromOwnString out;
            per_key->getKeySerialization()->serializeText(*key_column, key_column->size() - 1, out, FormatSettings{});
            keys.emplace(column.name, out.str());
        }
    }
}

ColumnPtr makeMapColumnKeysColumn(const MapColumnKeys & keys)
{
    auto names = ColumnString::create();
    auto values = ColumnString::create();
    for (const auto & [name, key] : keys)
    {
        names->insertData(name.data(), name.size());
        values->insertData(key.data(), key.size());
    }
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->getData().push_back(keys.size());
    return ColumnArray::create(ColumnTuple::create(Columns{std::move(names), std::move(values)}), std::move(offsets));
}

ColumnPtr getMapKeyColumnsFiles(const IMergeTreeDataPart & part, const NamesAndTypesList & columns, ContextPtr context)
{
    std::set<String> files;
    for (const auto & column : columns)
    {
        if (!isMap(column.type) || !part.getColumns().contains(column.name))
            continue;
        const auto serialization = part.getSerialization(column.name);
        const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(serialization.get());
        if (!per_key)
            continue;
        ISerialization::EnumerateStreamsSettings settings;
        for (const auto & key : readMapKeyColumnsManifest(part, column, context))
        {
            if (auto status = context->getProcessListElement(); status && !status->checkTimeLimit())
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Time limit exceeded while enumerating Map files");
            per_key->enumerateKeyStreams(settings, [&](const ISerialization::SubstreamPath & path)
            {
                for (const auto & extension : {String(IMergeTreeDataPart::DATA_FILE_EXTENSION), part.getMarksFileExtension()})
                {
                    auto stream = IMergeTreeDataPart::getStreamNameForColumn(
                        column, path, extension, part.getDataPartStorage(), part.storage.getSettings());
                    if (stream && part.checksums.files.contains(*stream + extension))
                        files.insert(*stream + extension);
                }
            }, ISerialization::SubstreamData(serialization).withType(column.type), key);
        }
    }
    auto result = ColumnArray::create(ColumnString::create());
    auto & data = assert_cast<ColumnString &>(result->getData());
    for (const auto & file : files)
        data.insertData(file.data(), file.size());
    result->getOffsets().push_back(files.size());
    return result;
}

}
