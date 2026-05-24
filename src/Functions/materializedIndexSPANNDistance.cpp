#include "config.h"

#if USE_SPTAG

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Common/FunctionDocumentation.h>
#include <Common/assert_cast.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Storages/AuxiliaryIndex/SPANNFacade.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

SPANNFacade::Metric toSPANNMetric(UInt64 metric_id, const String & function_name)
{
    if (metric_id == SPANNFacade::metricId(SPANNFacade::Metric::L2))
        return SPANNFacade::Metric::L2;
    if (metric_id == SPANNFacade::metricId(SPANNFacade::Metric::Cosine))
        return SPANNFacade::Metric::Cosine;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported SPANN metric id {} for function {}", metric_id, function_name);
}

UInt64 getConstUInt64(const ColumnWithTypeAndName & argument, std::string_view argument_name, const String & function_name)
{
    const auto * column = checkAndGetColumnConst<ColumnUInt64>(argument.column.get());
    if (!column)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument '{}' of function {} must be constant UInt64", argument_name, function_name);
    return column->getValue<UInt64>();
}

void validateArrayFloat32Type(const DataTypePtr & type, std::string_view argument_name, const String & function_name)
{
    const auto * array_type = typeid_cast<const DataTypeArray *>(type.get());
    if (!array_type || !typeid_cast<const DataTypeFloat32 *>(array_type->getNestedType().get()))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument '{}' of function {} must be Array(Float32)", argument_name, function_name);
}

class FunctionAuxiliaryIndexSPANNDistance final : public IFunction
{
public:
    static constexpr auto name = "__materializedIndexSPANNDistance";

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionAuxiliaryIndexSPANNDistance>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2, 3}; }
    bool isSuitableForConstantFolding() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    bool useDefaultImplementationForConstants() const override { return false; }
    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 4)
            throw Exception(
                ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                "Number of arguments for function {} can't be {}, should be 4",
                getName(),
                arguments.size());

        validateArrayFloat32Type(arguments[0], "embedding", getName());
        validateArrayFloat32Type(arguments[1], "query_vector", getName());

        if (!WhichDataType(arguments[2]).isUInt64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 'metric_id' of function {} must be UInt64", getName());
        if (!WhichDataType(arguments[3]).isUInt64())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 'dim' of function {} must be UInt64", getName());

        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnFloat32::create();

        const UInt64 metric_id = getConstUInt64(arguments[2], "metric_id", getName());
        const auto metric = toSPANNMetric(metric_id, getName());

        const UInt64 dim64 = getConstUInt64(arguments[3], "dim", getName());
        if (dim64 == 0 || dim64 > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument 'dim' of function {} is out of UInt32 range", getName());
        const auto dim = static_cast<UInt32>(dim64);

        const auto * query_const = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
        if (!query_const)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 'query_vector' of function {} must be constant Array(Float32)", getName());

        const auto & query_array = assert_cast<const ColumnArray &>(query_const->getDataColumn());
        const auto & query_offsets = query_array.getOffsets();
        if (query_offsets.size() != 1 || query_offsets.front() != dim)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Query vector dimension for function {} does not match index dim {}", getName(), dim);

        const auto & query_data = assert_cast<const ColumnFloat32 &>(query_array.getData()).getData();

        auto candidates_holder = arguments[0].column->convertToFullColumnIfConst();
        const auto * candidates_array = typeid_cast<const ColumnArray *>(candidates_holder.get());
        if (!candidates_array)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument 'embedding' of function {} was not ColumnArray", getName());

        const auto & candidate_data = assert_cast<const ColumnFloat32 &>(candidates_array->getData()).getData();
        const auto & candidate_offsets = candidates_array->getOffsets();
        if (candidate_offsets.size() != input_rows_count)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected row count in argument 'embedding' of function {}", getName());

        UInt64 prev_offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const UInt64 current_offset = candidate_offsets[row];
            if (current_offset - prev_offset != dim)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Embedding row {} dimension for function {} is {} but index dim is {}",
                    row,
                    getName(),
                    current_offset - prev_offset,
                    dim);
            prev_offset = current_offset;
        }

        auto result = ColumnFloat32::create(input_rows_count);
        SPANNFacade::computeDistances(
            metric,
            dim,
            query_data.data(),
            candidate_data.data(),
            input_rows_count,
            result->getData().data());
        return result;
    }
};

}

REGISTER_FUNCTION(AuxiliaryIndexSPANNDistance)
{
    factory.registerFunction<FunctionAuxiliaryIndexSPANNDistance>(
        FunctionDocumentation::INTERNAL_FUNCTION_DOCS,
        FunctionFactory::Case::Sensitive);
}

}

#endif
