#include <Storages/System/StorageSystemReflectionAnnIndexes.h>

#include <Access/ContextAccess.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Reflection/ANNIndex/ReflectionANNIndex.h>

#include <string>

namespace DB
{

namespace
{

Map makeStringMap(const std::map<String, String> & values)
{
    Map result;
    result.reserve(values.size());
    for (const auto & [key, value] : values)
        result.push_back(Tuple{key, value});
    return result;
}

}

StorageSystemReflectionAnnIndexes::StorageSystemReflectionAnnIndexes(const StorageID & storage_id_, ColumnsDescription columns_description_)
    : IStorageSystemOneBlock(storage_id_, std::move(columns_description_))
{
}

ColumnsDescription StorageSystemReflectionAnnIndexes::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"database", std::make_shared<DataTypeString>(), "Database of the ANN reflection."},
        {"reflection_name", std::make_shared<DataTypeString>(), "Name of the ANN reflection."},
        {"family", std::make_shared<DataTypeString>(), "Reflection engine family."},
        {"impl", std::make_shared<DataTypeString>(), "ANN algorithm implementation."},
        {"metric", std::make_shared<DataTypeString>(), "ANN distance metric."},
        {"dimension", std::make_shared<DataTypeUInt64>(), "ANN vector dimension."},
        {"algorithm_params", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()), "Algorithm-specific observability parameters."},
        {"coverage_ratio", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "Fraction of source rows covered by ready ANNIndex parts."},
        {"uncovered_rows", std::make_shared<DataTypeUInt64>(), "Rows waiting for ANNIndex build coverage."},
        {"pending_task_count", std::make_shared<DataTypeUInt64>(), "Number of reserved or running ANNIndex tasks."},
    };
}

void StorageSystemReflectionAnnIndexes::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_TABLES);

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_remote_databases = false});
    for (const auto & [database_name, database] : databases)
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE || database->isExternal())
            continue;

        const bool check_access_for_tables = check_access_for_databases && !access->isGranted(AccessType::SHOW_TABLES, database_name);
        for (auto tables_it = database->getTablesIterator(context); tables_it->isValid(); tables_it->next())
        {
            const auto table_name = tables_it->name();
            if (check_access_for_tables && !access->isGranted(AccessType::SHOW_TABLES, database_name, table_name))
                continue;

            const auto table = tables_it->table();
            const auto * ann = dynamic_cast<const ReflectionANNIndex *>(table.get());
            if (!ann || ann->getFamily() != "ann")
                continue;

            const auto params = ann->getAlgorithmObservabilityFields();
            UInt64 dimension = 0;
            if (auto it = params.find("dimension"); it != params.end())
                dimension = std::stoull(it->second);

            const auto observability = ann->getObservabilitySnapshot();
            size_t col = 0;
            res_columns[col++]->insert(database_name);
            res_columns[col++]->insert(table_name);
            res_columns[col++]->insert(ann->getFamily());
            res_columns[col++]->insert(ann->getImpl());
            if (auto it = params.find("metric"); it != params.end())
                res_columns[col++]->insert(it->second);
            else
                res_columns[col++]->insert(String{});
            res_columns[col++]->insert(dimension);
            res_columns[col++]->insert(makeStringMap(params));
            res_columns[col++]->insertDefault();
            res_columns[col++]->insert(observability.backlog_rows);
            res_columns[col++]->insert(observability.pending_task_count);
        }
    }
}

}
