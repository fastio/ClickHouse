#include "config.h"

#if USE_LANCE

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/Lance/StorageLance.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/* lance(uri) - reads an external Lance dataset as a table. */
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
};

void TableFunctionLance::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;
    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    if (args.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires one argument: the Lance dataset path or URI.", getName());

    args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
    uri = checkAndGetLiteralArgument<String>(args[0], "uri");
}

ColumnsDescription TableFunctionLance::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return StorageLance::getTableStructureFromData(uri, context);
}

StoragePtr TableFunctionLance::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageLance>(StorageID(getDatabaseName(), table_name), columns, uri);
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
