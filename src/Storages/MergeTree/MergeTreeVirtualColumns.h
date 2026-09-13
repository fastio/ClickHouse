#pragma once

#include <DataTypes/IDataType.h>

#include <Parsers/IAST_fwd.h>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/MergeTreePartition.h>

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/Context_fwd.h>
#include <set>

namespace DB
{

class IMergeTreeDataPart;
class ASTAssignment;
struct StorageID;
using MapColumnKeys = std::set<std::pair<String, String>>;
std::vector<Field> readMapKeyColumnsManifest(const IMergeTreeDataPart & part, const NameAndTypePair & column, ContextPtr context);
void checkMapMetadataAccess(const StorageID & id, const Names & columns, ContextPtr context);
void collectMapKeyColumnsColumnKeys(const IMergeTreeDataPart & part, const NamesAndTypesList & columns, MapColumnKeys & keys, ContextPtr context);
ColumnPtr makeMapColumnKeysColumn(const MapColumnKeys & keys);
ColumnPtr getMapKeyColumnsFiles(const IMergeTreeDataPart & part, const NamesAndTypesList & columns, ContextPtr context);

struct RowExistsColumn
{
    static const String name;
    static const DataTypePtr type;
};

/// True only for the lightweight-delete marker assignment `_row_exists = 0` (what `DELETE FROM`
/// rewrites to). An arbitrary `_row_exists = <expr>` modifies the deletion mask and is a real update,
/// so it returns false. Used to govern `_row_exists = 0` by ALTER DELETE while keeping ALTER UPDATE
/// for any other assignment to the column.
bool isLightweightDeleteAssignment(const ASTAssignment & assignment);

struct BlockNumberColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct BlockOffsetColumn
{
    static const String name;
    static const DataTypePtr type;
    static const ASTPtr codec;
};

struct PartDataVersionColumn
{
    static const String name;
    static const DataTypePtr type;
};

struct PartitionIdColumn
{
    static const String name;
    static const DataTypePtr type;
};

struct PartitionValueColumn
{
    static const String name;
    static DataTypePtr type(const KeyDescription * partition_key);
};

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection);

}
