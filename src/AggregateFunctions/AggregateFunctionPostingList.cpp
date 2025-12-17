#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/AggregateFunctionPostingListData.h>
#include <DataTypes/DataTypesNumber.h>
#include <algorithm>
#include <utility>
#include <Common/RadixSort.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/IColumn.h>

#pragma clang optimize off
namespace DB
{

struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class AggregateFunctionPostingList final : public IAggregateFunctionDataHelper<PostingListData, AggregateFunctionPostingList>
{
    using Data = PostingListData;
    static constexpr auto NAME = "postingList";
public:
    explicit AggregateFunctionPostingList(const DataTypePtr & data_type_)
        : IAggregateFunctionDataHelper({data_type_}, {}, createResultType())
    {
    }

    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt32>()); }

    String getName() const override { return NAME; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        (void) place;
        (void) columns;
        (void) row_num;
        (void) arena;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        (void) place;
        (void) rhs;
        (void) arena;
    }

    void serialize(ConstAggregateDataPtr __restrict /* place */, WriteBuffer & /* buf */, std::optional<size_t> /* version */) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

    void deserialize(AggregateDataPtr __restrict /* place */, ReadBuffer & /* buf */, std::optional<size_t> /* version */, Arena * /* arena */) const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        (void) place;
        (void) to;
        (void) arena;
    }

    bool allocatesMemoryInArena() const override { return true; }
};

AggregateFunctionPtr create(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!WhichDataType(argument_types[0]).isUInt32())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad argument for aggregate function {}, Only support UInt32", name);

    return std::make_shared<AggregateFunctionPostingList>(argument_types[0]);
}

void registerAggregateFunctionPostingList(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    factory.registerFunction("postingList", {create, properties});
}
}
