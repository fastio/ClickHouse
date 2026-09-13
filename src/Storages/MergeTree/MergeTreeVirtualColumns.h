#pragma once
#include <Core/Types.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <set>

namespace DB
{

class IMergeTreeDataPart;
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

Field getFieldForConstVirtualColumn(const String & column_name, const IMergeTreeDataPart & part_or_projection);

}
