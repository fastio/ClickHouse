#include <Analyzer/Resolve/TableExpressionData.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

std::optional<AnalysisTableExpressionData::SubcolumnInfo>
AnalysisTableExpressionData::tryGetSubcolumnInfo(std::string_view full_identifier_name) const
{
    ensureColumnMembershipSetsArePopulated();
    for (auto [column_name, subcolumn_name] : Nested::getAllColumnAndSubcolumnPairs(full_identifier_name))
    {
        if (!column_names.contains(column_name))
            continue;
        const auto & node_map = getColumnNodeMap();
        auto it = node_map.find(column_name);
        if (it == node_map.end())
            continue;

        if (supports_subcolumns && storage_snapshot && !it->second->hasExpression()
            && storage_snapshot->metadata->getColumns().hasPhysical(String(column_name)))
        {
            /// Physical subcolumn types can depend on storage serialization, as with `with_key_columns` `Map` values.
            /// Use the same type as the reader instead of the data type's default serialization.
            auto column = storage_snapshot->tryGetColumn(
                GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns(), String(full_identifier_name));
            if (column && column->isSubcolumn() && column->getNameInStorage() == column_name
                && column->getSubcolumnName() == subcolumn_name)
                return SubcolumnInfo{it->second, subcolumn_name, column->type};
            continue;
        }

        if (auto subcolumn_type = it->second->getResultType()->tryGetSubcolumnType(subcolumn_name))
            return SubcolumnInfo{it->second, subcolumn_name, subcolumn_type};
    }

    return std::nullopt;
}

void AnalysisTableExpressionData::ensureColumnMembershipSetsArePopulated() const
{
    if (column_membership_sets_populated)
        return;
    column_names.reserve(column_names_and_types.size());
    column_identifier_first_parts.reserve(column_names_and_types.size());
    for (const auto & column_name_and_type : column_names_and_types)
    {
        column_names.insert(column_name_and_type.name);
        Identifier column_name_identifier(column_name_and_type.name);
        column_identifier_first_parts.insert(column_name_identifier.at(0));
    }
    column_membership_sets_populated = true;
}

const ColumnNameToColumnNodeMap & AnalysisTableExpressionData::getColumnNodeMap() const
{
    if (column_name_to_column_node.has_value())
        return *column_name_to_column_node;
    /// Emplace the (initially empty) map before invoking the populator. The populator
    /// first inserts every regular column (and ALIAS placeholders) into the map, then
    /// resolves ALIAS expressions; that resolution can recursively trigger identifier
    /// lookups that call this method again. Emplacing up front breaks the recursion:
    /// re-entrants find the map present and see the placeholders the populator has
    /// already inserted.
    auto & node_map = column_name_to_column_node.emplace();
    ensureColumnMembershipSetsArePopulated();
    if (populate_column_node_map)
        populate_column_node_map(node_map);
    return node_map;
}

void AnalysisTableExpressionData::setColumnNodeMapPopulator(std::function<void(ColumnNameToColumnNodeMap &)> populator)
{
    populate_column_node_map = std::move(populator);
}

ColumnNameToColumnNodeMap & AnalysisTableExpressionData::emplaceColumnNodeMap() const
{
    return column_name_to_column_node.emplace();
}

}
