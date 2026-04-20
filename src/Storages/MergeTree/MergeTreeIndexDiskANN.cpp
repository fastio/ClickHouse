#include <Storages/MergeTree/MergeTreeIndexDiskANN.h>

#include <DataTypes/DataTypeArray.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Storages/IndicesDescription.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>

#include <unordered_map>
#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int INCORRECT_NUMBER_OF_COLUMNS;
    extern const int INCORRECT_QUERY;
}

namespace
{

/// Positional-only schema fallback: (metric, dim [, pq_bytes]).
constexpr size_t POSITIONAL_METRIC = 0;
constexpr size_t POSITIONAL_DIM = 1;
constexpr size_t POSITIONAL_PQ_BYTES = 2;

constexpr std::string_view KEY_METRIC = "metric";
constexpr std::string_view KEY_DIM = "dim";
constexpr std::string_view KEY_PQ_BYTES = "pq_bytes";
constexpr std::string_view ANN_INDEX_PREFIX = "ann_index_";

const std::unordered_set<std::string_view> SUPPORTED_METRICS = {"l2", "cosine"};

/// Helper: is the argument a named-argument form `name = value`?
/// Returns the equals ASTFunction on success.
const ASTFunction * asNamedArg(const IAST * arg)
{
    const auto * func = arg->as<ASTFunction>();
    if (!func || func->name != "equals")
        return nullptr;
    const auto * expr_list = func->arguments ? func->arguments->as<ASTExpressionList>() : nullptr;
    if (!expr_list || expr_list->children.size() != 2)
        return nullptr;
    return func;
}

/// Parse a scalar argument (ASTLiteral or ASTIdentifier) into a Field.
Field parseScalar(const IAST * ast, const String & arg_hint)
{
    if (const auto * literal = ast->as<ASTLiteral>())
        return literal->value;
    if (const auto * identifier = ast->as<ASTIdentifier>())
        return Field(identifier->name());
    throw Exception(
        ErrorCodes::INCORRECT_QUERY,
        "DiskANN index argument {} must be a literal or identifier",
        arg_hint);
}

String fieldToString(const Field & f, const String & key)
{
    if (f.getType() == Field::Types::String)
        return f.safeGet<String>();
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "DiskANN index parameter `{}` must be a string (e.g. 'l2')", key);
}

UInt64 fieldToUInt64(const Field & f, const String & key)
{
    if (f.getType() == Field::Types::UInt64)
        return f.safeGet<UInt64>();
    if (f.getType() == Field::Types::Int64)
    {
        auto v = f.safeGet<Int64>();
        if (v < 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "DiskANN index parameter `{}` must be a non-negative integer",
                key);
        return static_cast<UInt64>(v);
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "DiskANN index parameter `{}` must be an unsigned integer",
        key);
}

struct ParsedArgs
{
    String metric;
    UInt64 dim = 0;
    bool has_metric = false;
    bool has_dim = false;
};

ParsedArgs parseDiskannArguments(const IndexDescription & index)
{
    ParsedArgs result;

    const ASTPtr & arguments = index.arguments;
    if (!arguments || arguments->children.empty())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "DiskANN index requires parameters, at least `metric` and `dim`");

    const auto & children = arguments->children;

    /// Detect form: all named or all positional. Mixing is not supported.
    size_t named_count = 0;
    for (const auto & child : children)
        if (asNamedArg(child.get()))
            ++named_count;

    if (named_count != 0 && named_count != children.size())
        throw Exception(
            ErrorCodes::INCORRECT_QUERY,
            "DiskANN index arguments must be either all named (e.g. metric='l2', dim=128) "
            "or all positional, not mixed");

    if (named_count == children.size())
    {
        /// Named-argument form.
        std::unordered_map<String, Field> seen;
        for (const auto & child : children)
        {
            const auto * equals = asNamedArg(child.get());
            const auto & equal_args = equals->arguments->as<ASTExpressionList &>().children;

            const auto * key_ast = equal_args[0]->as<ASTIdentifier>();
            if (!key_ast)
                throw Exception(
                    ErrorCodes::INCORRECT_QUERY,
                    "DiskANN index named argument must have an identifier on the left-hand side");
            const String & key = key_ast->name();

            Field value = parseScalar(equal_args[1].get(), "`" + key + "`");

            if (!seen.emplace(key, value).second)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "DiskANN index parameter `{}` specified more than once",
                    key);
        }

        for (const auto & [key, value] : seen)
        {
            if (key == KEY_METRIC)
            {
                result.metric = fieldToString(value, key);
                result.has_metric = true;
            }
            else if (key == KEY_DIM)
            {
                result.dim = fieldToUInt64(value, key);
                result.has_dim = true;
            }
            else if (key == KEY_PQ_BYTES)
            {
                (void)fieldToUInt64(value, key);
            }
            else if (key.starts_with(ANN_INDEX_PREFIX))
            {
                /// Opaque ann_index_* parameters are passed through to later phases.
                /// We only require them to be scalar (parseScalar already enforced that).
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown DiskANN index parameter `{}`. "
                    "Supported: metric, dim, pq_bytes, ann_index_*",
                    key);
            }
        }
    }
    else
    {
        /// Positional form: (metric, dim [, pq_bytes]).
        if (children.size() < 2 || children.size() > 3)
            throw Exception(
                ErrorCodes::INCORRECT_QUERY,
                "DiskANN index positional arguments must be (metric, dim [, pq_bytes])");

        Field metric_field = parseScalar(children[POSITIONAL_METRIC].get(), "metric");
        Field dim_field = parseScalar(children[POSITIONAL_DIM].get(), "dim");

        result.metric = fieldToString(metric_field, "metric");
        result.has_metric = true;
        result.dim = fieldToUInt64(dim_field, "dim");
        result.has_dim = true;

        if (children.size() == 3)
        {
            Field pq_field = parseScalar(children[POSITIONAL_PQ_BYTES].get(), "pq_bytes");
            (void)fieldToUInt64(pq_field, "pq_bytes");
        }
    }

    if (!result.has_metric)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index requires parameter `metric` (supported: 'l2', 'cosine')");
    if (!result.has_dim)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index requires parameter `dim` (must be > 0)");

    return result;
}

}

void diskannIndexValidator(const IndexDescription & index, bool attach)
{
    /// Still validate even on attach: we reject only truly invalid values.
    /// Unknown-but-forward-compatible things (ann_index_*) remain accepted.

    ParsedArgs parsed = parseDiskannArguments(index);

    /// Only `l2` and `cosine` are supported; `mips` is rejected explicitly so users get
    /// a clear message rather than the generic "unknown metric" branch below.
    if (parsed.metric == "mips")
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN metric 'mips' is not yet supported. Supported metrics: 'l2', 'cosine'");

    if (!SUPPORTED_METRICS.contains(parsed.metric))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN metric '{}' is not supported. Supported metrics: 'l2', 'cosine'",
            parsed.metric);

    if (parsed.dim == 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "DiskANN index parameter `dim` must be > 0");

    /// DDL surface of the index: a single Array(Float32) column.
    if (attach)
    {
        /// On ATTACH (re-loading persisted metadata) we skip column-shape checks
        /// for forward compatibility: old tables on disk must always reload.
        return;
    }

    if (index.column_names.size() != 1 || index.data_types.size() != 1)
        throw Exception(
            ErrorCodes::INCORRECT_NUMBER_OF_COLUMNS,
            "DiskANN index must be created on a single column");

    DataTypePtr data_type = index.sample_block.getDataTypes()[0];
    const auto * data_type_array = typeid_cast<const DataTypeArray *>(data_type.get());
    if (!data_type_array)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "DiskANN index can only be created on columns of type Array(Float32)");

    WhichDataType which(data_type_array->getNestedType()->getTypeId());
    if (!which.isFloat32())
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "DiskANN index can only be created on columns of type Array(Float32)");
}

}
