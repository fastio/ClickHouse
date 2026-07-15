#include "config.h"

#if USE_LANCE

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/Lance/StorageLance.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_lance;
    extern const SettingsUInt64 lance_version;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/* lance(uri [, access_key_id, secret_access_key [, session_token]]) - reads an external Lance
 * dataset as a table. Also accepts a named collection holding `url` plus storage options. */
class TableFunctionLance : public ITableFunction
{
public:
    static constexpr auto name = "lance";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "Lance"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String uri;
    LanceStorageOptions storage_options;
};

void TableFunctionLance::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    if (!context->getSettingsRef()[Setting::allow_experimental_lance])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "Set `allow_experimental_lance` setting to enable the `lance` table function");

    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    parseLanceArguments(args_func.at(0)->children, context, uri, storage_options);
}

ColumnsDescription TableFunctionLance::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    const UInt64 version = context->getSettingsRef()[Setting::lance_version];
    return StorageLance::getTableStructureFromData(uri, storage_options, version, context);
}

StoragePtr TableFunctionLance::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageLance>(StorageID(getDatabaseName(), table_name), columns, uri, storage_options);
    res->startup();
    return res;
}

}

void registerTableFunctionLance(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionLance>({}, {.allow_readonly = true});
}

}

#endif
