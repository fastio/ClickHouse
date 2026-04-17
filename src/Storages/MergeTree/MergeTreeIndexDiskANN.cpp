#include <Storages/MergeTree/MergeTreeIndexDiskANN.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <ranges>
#include <optional>
#include <unordered_map>
#include <unordered_set>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

/// Argument names (mirror the fields of `ANNIndexParams`).
constexpr auto ARG_METRIC   = "metric";
constexpr auto ARG_DIM      = "dim";
constexpr auto ARG_R        = "R";
constexpr auto ARG_L        = "L";
constexpr auto ARG_ALPHA    = "alpha";
constexpr auto ARG_PQ_BYTES = "pq_bytes";

/// ---------------------------------------------------------------------------
/// Named-argument parsing helpers.
///
/// Copied from `MergeTreeIndexText.cpp` per decision D-01 (research.md). That
/// file defines these in its own anonymous namespace and does not expose them,
/// so duplicating ~50 lines here is cheaper than extracting a shared header in
/// this task. If a third index type needs the same helpers, extract them then.
/// ---------------------------------------------------------------------------

template <typename Type>
Type castAs(const Field & field, std::string_view argument_name)
{
    auto expected_type = Field::TypeToEnum<Type>::value;
    if (expected_type != field.getType())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index argument '{}' expected to be {}, but got {}",
            argument_name, fieldTypeToString(Field::TypeToEnum<Type>::value), field.getTypeName());
    }
    return field.safeGet<Type>();
}

template <typename Type>
std::optional<Type> extractFieldOption(std::unordered_map<String, ASTPtr> & options, const String & option)
{
    auto it = options.find(option);
    if (it == options.end())
        return {};

    Field value = getFieldFromIndexArgumentAST(it->second);
    value = castAs<Type>(value, option);

    options.erase(it);
    return value.safeGet<Type>();
}

std::pair<String, ASTPtr> parseNamedArgument(const ASTFunction * ast_equal_function)
{
    if (!ast_equal_function
        || ast_equal_function->name != "equals"
        || ast_equal_function->arguments->children.size() != 2)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index arguments must be passed as key=value pairs");
    }

    const auto & arguments = ast_equal_function->arguments;
    const auto * key_identifier = arguments->children[0]->as<ASTIdentifier>();

    if (!key_identifier)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index argument key must be an identifier, got {}",
            ast_equal_function->formatForErrorMessage());
    }

    return {key_identifier->name(), arguments->children[1]};
}

std::unordered_map<String, ASTPtr> convertArgumentsToOptionsMap(const ASTPtr & arguments)
{
    std::unordered_map<String, ASTPtr> options;
    if (!arguments)
        return options;

    for (const auto & child : arguments->children)
    {
        const auto * ast_equal_function = child->as<ASTFunction>();
        auto [key, ast] = parseNamedArgument(ast_equal_function);

        if (!options.emplace(key, ast).second)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DiskANN index argument '{}' is specified more than once",
                key);
    }
    return options;
}

/// ---------------------------------------------------------------------------
/// DiskANN-specific value sets.
/// ---------------------------------------------------------------------------

const std::unordered_set<String> & validMetrics()
{
    static const std::unordered_set<String> kValid{"l2", "mips", "cosine"};
    return kValid;
}

const std::unordered_set<UInt64> & validPqBytes()
{
    static const std::unordered_set<UInt64> kValid{0, 8, 16, 32, 64};
    return kValid;
}

/// Core parser shared by `diskANNIndexValidator` and `ANNIndexParams::fromDescription`.
ANNIndexParams parseParams(const IndexDescription & index)
{
    ANNIndexParams params;
    auto options = convertArgumentsToOptionsMap(index.arguments);

    /// metric (required)
    auto metric_opt = extractFieldOption<String>(options, ARG_METRIC);
    if (!metric_opt)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN index argument 'metric' is required");
    params.metric = *metric_opt;
    if (!validMetrics().contains(params.metric))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index 'metric' must be one of: 'l2', 'mips', 'cosine' (got: '{}')",
            params.metric);
    }

    /// dim (required)
    auto dim_opt = extractFieldOption<UInt64>(options, ARG_DIM);
    if (!dim_opt)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN index argument 'dim' is required");
    params.dim = *dim_opt;
    if (params.dim == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "DiskANN index 'dim' must be > 0");

    /// R (optional, default 64, [16, 128])
    params.graph_degree = extractFieldOption<UInt64>(options, ARG_R).value_or(64);
    if (params.graph_degree < 16 || params.graph_degree > 128)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index 'R' must be in [16, 128] (got: {})",
            params.graph_degree);
    }

    /// L (optional, default 100, [R, 512])
    params.search_list_size = extractFieldOption<UInt64>(options, ARG_L).value_or(100);
    if (params.search_list_size < params.graph_degree || params.search_list_size > 512)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index 'L' must be in [R={}, 512] (got: {})",
            params.graph_degree, params.search_list_size);
    }

    /// alpha (optional, default 1.2, [1.0, 2.0])
    params.alpha = extractFieldOption<Float64>(options, ARG_ALPHA).value_or(1.2);
    if (params.alpha < 1.0 || params.alpha > 2.0)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index 'alpha' must be in [1.0, 2.0] (got: {})",
            params.alpha);
    }

    /// pq_bytes (optional, default 0, in {0, 8, 16, 32, 64})
    params.pq_bytes = extractFieldOption<UInt64>(options, ARG_PQ_BYTES).value_or(0);
    if (!validPqBytes().contains(params.pq_bytes))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index 'pq_bytes' must be one of: 0, 8, 16, 32, 64 (got: {})",
            params.pq_bytes);
    }

    if (!options.empty())
    {
        auto keys_view = std::views::keys(options);
        std::vector<String> unknown_keys(keys_view.begin(), keys_view.end());
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown DiskANN index arguments: {}",
            fmt::join(unknown_keys, ", "));
    }

    return params;
}

}

ANNIndexParams ANNIndexParams::fromDescription(const IndexDescription & index)
{
    return parseParams(index);
}

void diskANNIndexValidator(const IndexDescription & index, bool /* attach */)
{
    /// 1. Parameter parsing with full range / required-field validation.
    (void)parseParams(index);

    /// 2. Column-shape validation (mirrors `vectorSimilarityIndexValidator`).
    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "DiskANN index must be created on a single column");

    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!data_type_array)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "DiskANN index can only be created on columns of type Array(Float32|Float64|BFloat16)");

    WhichDataType nested_which(data_type_array->getNestedType()->getTypeId());
    if (!nested_which.isNativeFloat() && !nested_which.isBFloat16())
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "DiskANN index can only be created on columns of type Array(Float32|Float64|BFloat16)");
}

}
