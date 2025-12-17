#include <DataTypes/DataTypePostingList.h>
#include <DataTypes/Serializations/SerializationPostingList.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Parsers/IAST.h>
#include <Parsers/NullsAction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static std::pair<DataTypePtr, DataTypeCustomDescPtr> create(const ASTPtr & arguments)
{
    if (arguments && !arguments->children.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PostingList type can not have any arguments");

    Array params;
    DataTypes types = {std::make_shared<DataTypeUInt32>()};
    AggregateFunctionProperties properties;
    auto function = AggregateFunctionFactory::instance().get("postingList", NullsAction::EMPTY, types, params, properties);
    DataTypePtr posting_list_type = std::make_shared<DataTypeAggregateFunction>(function, types, params);
    DataTypeCustomNamePtr custom_type = std::make_unique<DataTypePostingList>();
    SerializationPtr custom_serialization = std::make_unique<SerializationPostingList>(function);

    return std::make_pair(std::move(posting_list_type), std::make_unique<DataTypeCustomDesc>(std::move(custom_type), std::move(custom_serialization)));
}

void registerDataTypePostingList(DataTypeFactory & factory)
{
    factory.registerDataTypeCustom("PostingList", create);
}

}
