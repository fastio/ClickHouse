#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessType.h>
#include <Access/EnabledRowPolicies.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Common/HashTable/HashSet.h>
#include <Common/OptimizedRegularExpression.h>
#include <Common/Stopwatch.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <DataTypes/getLeastSupertype.h>
#include <Formats/FormatSettings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/castColumn.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <set>


namespace DB
{
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsMergeTreeMapSerializationVersion map_serialization_version;
}

namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsBool allow_lossy_numeric_supertype;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int INCORRECT_DATA;
    extern const int TIMEOUT_EXCEEDED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

// map(x, y, ...) is a function that allows you to make key-value pair
class FunctionMap final : public IFunction
{
public:
    static constexpr auto name = "map";

    explicit FunctionMap(ContextPtr context)
        : use_variant_as_common_type(context->getSettingsRef()[Setting::use_variant_as_common_type])
        , allow_lossy_numeric_supertype(context->getSettingsRef()[Setting::allow_lossy_numeric_supertype])
        , function_array(FunctionFactory::instance().get("array", context))
        , function_map_from_arrays(FunctionFactory::instance().get("mapFromArrays", context))
    {
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionMap>(context);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isInjective(const ColumnsWithTypeAndName &) const override
    {
        return true;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }
    /// map(..., Nothing) -> Map(..., Nothing)
    bool useDefaultImplementationForNothing() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() % 2 != 0)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires even number of arguments, but {} given", getName(), arguments.size());

        DataTypes keys;
        DataTypes values;
        for (size_t i = 0; i < arguments.size(); i += 2)
        {
            keys.emplace_back(arguments[i]);
            values.emplace_back(arguments[i + 1]);
        }

        DataTypes tmp;
        if (use_variant_as_common_type)
        {
            tmp.emplace_back(getLeastSupertypeOrVariant(keys, allow_lossy_numeric_supertype));
            tmp.emplace_back(getLeastSupertypeOrVariant(values, allow_lossy_numeric_supertype));
        }
        else
        {
            tmp.emplace_back(getLeastSupertype(keys, allow_lossy_numeric_supertype));
            tmp.emplace_back(getLeastSupertype(values, allow_lossy_numeric_supertype));
        }
        return std::make_shared<DataTypeMap>(tmp);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        size_t num_elements = arguments.size();
        if (num_elements == 0)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        ColumnsWithTypeAndName key_args;
        ColumnsWithTypeAndName value_args;
        for (size_t i = 0; i < num_elements; i += 2)
        {
            key_args.emplace_back(arguments[i]);
            value_args.emplace_back(arguments[i+1]);
        }

        const auto & result_type_map = static_cast<const DataTypeMap &>(*result_type);
        const DataTypePtr & key_type = result_type_map.getKeyType();
        const DataTypePtr & value_type = result_type_map.getValueType();
        const DataTypePtr & key_array_type = std::make_shared<DataTypeArray>(key_type);
        const DataTypePtr & value_array_type = std::make_shared<DataTypeArray>(value_type);

        /// key_array = array(args[0], args[2]...)
        ColumnPtr key_array = function_array->build(key_args)->execute(key_args, key_array_type, input_rows_count, /* dry_run = */ false);
        /// value_array = array(args[1], args[3]...)
        ColumnPtr value_array = function_array->build(value_args)->execute(value_args, value_array_type, input_rows_count, /* dry_run = */ false);

        /// result = mapFromArrays(key_array, value_array)
        ColumnsWithTypeAndName map_args{{key_array, key_array_type, ""}, {value_array, value_array_type, ""}};
        return function_map_from_arrays->build(map_args)->execute(map_args, result_type, input_rows_count, /* dry_run = */ false);
    }

private:
    bool use_variant_as_common_type = false;
    bool allow_lossy_numeric_supertype = false;
    FunctionOverloadResolverPtr function_array;
    FunctionOverloadResolverPtr function_map_from_arrays;
};

/// mapFromArrays(keys, values) is a function that allows you to make key-value pair from a pair of arrays or maps
class FunctionMapFromArrays final : public IFunction
{
public:
    static constexpr auto name = "mapFromArrays";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapFromArrays>(); }
    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires 2 arguments, but {} given",
                getName(),
                arguments.size());

        auto get_nested_type = [&](const DataTypePtr & type)
        {
            DataTypePtr nested;
            if (const auto * type_as_array = checkAndGetDataType<DataTypeArray>(type.get()))
                nested = type_as_array->getNestedType();
            else if (const auto * type_as_map = checkAndGetDataType<DataTypeMap>(type.get()))
                nested = std::make_shared<DataTypeTuple>(type_as_map->getKeyValueTypes());
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} must be Array or Map, but {} is given",
                    getName(),
                    type->getName());

            return nested;
        };

        auto key_type = get_nested_type(arguments[0]);
        auto value_type = get_nested_type(arguments[1]);

        /// We accept Array(Nullable(T)) or Array(LowCardinality(Nullable(T))) as key types as long as the actual array doesn't contain NULL value(this is checked in executeImpl).
        key_type = removeNullableOrLowCardinalityNullable(key_type);

        DataTypes key_value_types{key_type, value_type};
        return std::make_shared<DataTypeMap>(key_value_types);
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t /* input_rows_count */) const override
    {
        auto get_array_column = [&](const ColumnPtr & column) -> std::pair<const ColumnArray *, ColumnPtr>
        {
            bool is_const = isColumnConst(*column);
            ColumnPtr holder = is_const ? column->convertToFullColumnIfConst() : column;

            const ColumnArray * col_res = nullptr;
            if (const auto * col_array = checkAndGetColumn<ColumnArray>(holder.get()))
                col_res = col_array;
            else if (const auto * col_map = checkAndGetColumn<ColumnMap>(holder.get()))
                col_res = &col_map->getNestedColumn();
            else
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN,
                    "Argument columns of function {} must be Array or Map, but {} is given",
                    getName(),
                    holder->getName());

            return {col_res, holder};
        };

        auto [col_keys, key_holder] = get_array_column(arguments[0].column);
        auto [col_values, values_holder] = get_array_column(arguments[1].column);

        /// Nullable(T) or LowCardinality(Nullable(T)) are okay as nested key types but actual NULL values are not okay.
        ColumnPtr data_keys = col_keys->getDataPtr();
        if (isColumnNullableOrLowCardinalityNullable(*data_keys))
        {
            if (const auto * nullable = checkAndGetColumn<ColumnNullable>(data_keys.get()))
            {
                const auto * null_map = &nullable->getNullMapData();
                if (null_map && !memoryIsZero(null_map->data(), 0, null_map->size()))
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "The nested column of first argument in function {} must not contain NULLs", getName());

                data_keys = nullable->getNestedColumnPtr();
            }
            else if (const auto * low_cardinality = checkAndGetColumn<ColumnLowCardinality>(data_keys.get()))
            {
                if (low_cardinality->containsNull())
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "The nested column of first argument in function {} must not contain NULLs", getName());

                data_keys = low_cardinality->cloneWithDefaultOnNull();
            }
        }

        if (!col_keys->hasEqualOffsets(*col_values))
            throw Exception(ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Two arguments of function {} must have equal sizes", getName());

        const auto & data_values = col_values->getDataPtr();
        const auto & offsets = col_keys->getOffsetsPtr();
        auto nested_column = ColumnArray::create(ColumnTuple::create(Columns{std::move(data_keys), data_values}), offsets);
        return ColumnMap::create(nested_column);
    }
};

class FunctionMapUpdate final : public IFunction
{
public:
    static constexpr auto name = "mapUpdate";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionMapUpdate>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(),
                arguments.size());

        const auto * left = checkAndGetDataType<DataTypeMap>(arguments[0].type.get());
        const auto * right = checkAndGetDataType<DataTypeMap>(arguments[1].type.get());

        if (!left || !right)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The two arguments for function {} must be both Map type", getName());

        if (!left->getKeyType()->equals(*right->getKeyType()) || !left->getValueType()->equals(*right->getValueType()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The Key And Value type of Map for function {} must be the same", getName());

        return std::make_shared<DataTypeMap>(left->getKeyType(), left->getValueType());
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        bool is_left_const = isColumnConst(*arguments[0].column);
        bool is_right_const = isColumnConst(*arguments[1].column);

        const auto * map_column_left = is_left_const
            ? checkAndGetColumnConstData<ColumnMap>(arguments[0].column.get())
            : checkAndGetColumn<ColumnMap>(arguments[0].column.get());

        const auto * map_column_right = is_right_const
            ? checkAndGetColumnConstData<ColumnMap>(arguments[1].column.get())
            : checkAndGetColumn<ColumnMap>(arguments[1].column.get());

        if (!map_column_left || !map_column_right)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Arguments for function {} must be maps, got {} and {} instead",
                getName(), arguments[0].column->getName(), arguments[1].column->getName());

        const auto & nested_column_left = map_column_left->getNestedColumn();
        const auto & keys_data_left = map_column_left->getNestedData().getColumn(0);
        const auto & values_data_left = map_column_left->getNestedData().getColumn(1);
        const auto & offsets_left = nested_column_left.getOffsets();

        const auto & nested_column_right = map_column_right->getNestedColumn();
        const auto & keys_data_right = map_column_right->getNestedData().getColumn(0);
        const auto & values_data_right = map_column_right->getNestedData().getColumn(1);
        const auto & offsets_right = nested_column_right.getOffsets();

        auto result_keys = keys_data_left.cloneEmpty();
        auto result_values = values_data_left.cloneEmpty();

        size_t size_to_reserve = keys_data_right.size() + (keys_data_left.size() - keys_data_right.size());

        result_keys->reserve(size_to_reserve);
        result_values->reserve(size_to_reserve);

        auto result_offsets = ColumnVector<IColumn::Offset>::create(input_rows_count);
        auto & result_offsets_data = result_offsets->getData();

        using Set = HashSetWithStackMemory<std::string_view, StringViewHash, 4>;

        Set right_keys_const;
        if (is_right_const)
        {
            for (size_t i = 0; i < keys_data_right.size(); ++i)
                right_keys_const.insert(keys_data_right.getDataAt(i));
        }

        IColumn::Offset current_offset = 0;
        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            size_t left_from = is_left_const ? 0 : offsets_left[row_idx - 1];
            size_t left_to = is_left_const ? offsets_left[0] : offsets_left[row_idx];

            size_t right_from = is_right_const ? 0 : offsets_right[row_idx - 1];
            size_t right_to = is_right_const ? offsets_right[0] : offsets_right[row_idx];

            auto execute_row = [&](const auto & set)
            {
                for (size_t i = left_from; i < left_to; ++i)
                {
                    if (!set.find(keys_data_left.getDataAt(i)))
                    {
                        result_keys->insertFrom(keys_data_left, i);
                        result_values->insertFrom(values_data_left, i);
                        ++current_offset;
                    }
                }
            };

            if (is_right_const)
            {
                execute_row(right_keys_const);
            }
            else
            {
                Set right_keys;
                for (size_t i = right_from; i < right_to; ++i)
                    right_keys.insert(keys_data_right.getDataAt(i));

                execute_row(right_keys);
            }

            size_t right_map_size = right_to - right_from;
            result_keys->insertRangeFrom(keys_data_right, right_from, right_map_size);
            result_values->insertRangeFrom(values_data_right, right_from, right_map_size);

            current_offset += right_map_size;
            result_offsets_data[row_idx] = current_offset;
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(result_keys), std::move(result_values)}),
            std::move(result_offsets));

        return ColumnMap::create(nested_column);
    }
};
}

namespace
{
/// Adapted from ByConity `src/Functions/map.cpp` and `src/DataTypes/MapHelpers.cpp`.
/// Copyright (2022) Bytedance Ltd. and/or its affiliates. Licensed under Apache-2.0.
class FunctionExtractMapColumn final : public IFunction
{
public:
    static constexpr auto name = "extractMapColumn";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionExtractMapColumn>();
    }
    String getName() const override
    {
        return name;
    }
    size_t getNumberOfArguments() const override
    {
        return 1;
    }
    bool useDefaultImplementationForConstants() const override
    {
        return true;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return true;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of {} must be String", name);
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        auto result = ColumnString::create();
        for (size_t row = 0; row < rows; ++row)
        {
            std::string_view file = arguments[0].column->getDataAt(row);
            /// ByConity accepts any suffix after the second separator, with `__M__1.bin` as the minimum length.
            const auto end = file.find("__", 2);
            if (file.size() >= 10 && file.starts_with("__") && end != std::string_view::npos)
                result->insertData(file.data() + 2, end - 2);
            else
                result->insertDefault();
        }
        return result;
    }
};

class FunctionGetMapKeys final : public IFunction, WithContext
{
public:
    static constexpr auto name = "getMapKeys";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionGetMapKeys>(context_);
    }
    explicit FunctionGetMapKeys(ContextPtr context_) : WithContext(context_)
    {
    }
    String getName() const override
    {
        return name;
    }
    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }
    bool isDeterministic() const override
    {
        return false;
    }
    bool isSuitableForConstantFolding() const override
    {
        return false;
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override
    {
        return false;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3 || arguments.size() > 5)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} requires database, table, column, [partition regexp], [timeout seconds]", name);
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (!arguments[i].column || !isColumnConst(*arguments[i].column)
                || (i < 4 ? !isString(arguments[i].type) : !(WhichDataType(arguments[i].type).isUInt8() || WhichDataType(arguments[i].type).isUInt16()
                    || WhichDataType(arguments[i].type).isUInt32() || WhichDataType(arguments[i].type).isUInt64())))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument {} of {} must be a constant {}", i + 1, name, i < 4 ? "String" : "unsigned integer");
        }
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t rows) const override
    {
        return result_type->createColumnConst(rows, Array{});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows) const override
    {
        const auto database = String(arguments[0].column->getDataAt(0));
        const auto table_name = String(arguments[1].column->getDataAt(0));
        const auto column_name = String(arguments[2].column->getDataAt(0));
        if (database.empty() || table_name.empty() || column_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database, table and column for {} must not be empty", name);
        const auto query_context = getContext();
        query_context->checkAccess(AccessType::SELECT, database, table_name, column_name);
        if (auto policy = query_context->getRowPolicyFilter(database, table_name, RowPolicyFilterType::SELECT_FILTER);
            policy && !policy->isAlwaysTrue())
            throw Exception(ErrorCodes::ACCESS_DENIED, "{} cannot expose part keys for a table with a row policy", name);
        auto storage = DatabaseCatalog::instance().getTable({database, table_name}, query_context);
        auto lock = storage->lockForShare(query_context->getCurrentQueryId(), query_context->getSettingsRef()[Setting::lock_acquire_timeout]);
        const auto * table = dynamic_cast<const MergeTreeData *>(storage.get());
        if (!table || (*table->getSettings())[MergeTreeSetting::map_serialization_version] != MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} requires a MergeTree table with with_key_columns Map serialization", name);
        auto metadata = storage->getInMemoryMetadataPtr(query_context, false);
        const auto & column = metadata->getColumns().getPhysical(column_name);
        if (!isMap(column.type))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} must be Map for {}", column_name, name);
        const String pattern = arguments.size() >= 4 ? String(arguments[3].column->getDataAt(0)) : "";
        OptimizedRegularExpression regexp(pattern);
        const UInt64 timeout = arguments.size() == 5 ? arguments[4].column->getUInt(0) : 0;
        Stopwatch watch;
        const auto query_status = query_context->getProcessListElement();
        auto check_limits = [&]
        {
            if (query_status)
                query_status->checkTimeLimit();
            if (timeout && watch.elapsed() / 1000000000ULL >= timeout)
                throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "{} exceeded its timeout", name);
        };
        std::set<String> keys;
        for (const auto & part : table->getVisibleDataPartsVector(query_context))
        {
            check_limits();
            if (!regexp.match(part->info.getPartitionId()))
                continue;
            /// A column added after this part was written has no stored keys in that part.
            if (!part->getColumns().contains(column_name))
                continue;
            const auto serialization_ptr = part->getSerialization(column_name);
            const auto * serialization = typeid_cast<const SerializationMapKeyColumns *>(serialization_ptr.get());
            if (!serialization)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} in part {} is not a with_key_columns Map", column_name, part->name);
            const auto fields = readMapKeyColumnsManifest(*part, {column_name, column.type}, query_context);
            auto key_column = serialization->getKeyType()->createColumn();
            for (const auto & field : fields)
            {
                check_limits();
                key_column->insert(field);
                WriteBufferFromOwnString out;
                serialization->getKeySerialization()->serializeText(*key_column, key_column->size() - 1, out, FormatSettings{});
                keys.insert(out.str());
            }
        }
        auto result = ColumnArray::create(ColumnString::create());
        auto & data = assert_cast<ColumnString &>(result->getData());
        for (const auto & key : keys)
            data.insertData(key.data(), key.size());
        result->getOffsets().push_back(keys.size());
        return ColumnConst::create(std::move(result), rows);
    }
};

}

REGISTER_FUNCTION(Map)
{
    factory.registerFunction<FunctionGetMapKeys>(FunctionDocumentation{
        .description = R"(Returns the distinct keys recorded in the visible data parts of a `with_key_columns` `Map` column, sorted as strings.
An optional regular expression selects partition IDs. This reads key manifests, so keys may remain after their rows are deleted until the parts are rewritten.
Requires `SELECT` on the column. Tables with restrictive row policies are not supported.)",
        .syntax = "getMapKeys(database, table, column[, partition_regexp[, timeout_seconds]])",
        .arguments = {{"database", "Database name.", {"const String"}}, {"table", "Table name.", {"const String"}},
            {"column", "Map column name.", {"const String"}}, {"partition_regexp", "Optional partition ID regular expression.", {"const String"}},
            {"timeout_seconds", "Optional timeout in seconds; zero disables this limit.", {"const UInt8", "const UInt16", "const UInt32", "const UInt64"}}},
        .returned_value = {"Returns sorted distinct keys formatted as strings.", {"Array(String)"}},
        .examples = {{"List keys", "SELECT getMapKeys('default', 'events', 'attributes')", "['a','b']"}},
        .introduced_in = {26, 9},
        .category = FunctionDocumentation::Category::Map});
    factory.registerFunction<FunctionExtractMapColumn>(FunctionDocumentation{
        .description = "Extracts the escaped map column name from a ByConity file name with the `__column__key` prefix. Returns an empty string for unrecognized names. This does not parse ClickHouse `with_key_columns` file names.",
        .syntax = "extractMapColumn(filename)",
        .arguments = {{"filename", "ByConity map file name.", {"String"}}},
        .returned_value = {"Returns the escaped column name or an empty string.", {"String"}},
        .examples = {{"Extract a column", "SELECT extractMapColumn('__m__1.bin')", "m"}},
        .introduced_in = {26, 9},
        .category = FunctionDocumentation::Category::Map});
    /// map function documentation
    FunctionDocumentation::Description description_map = R"(
Creates a value of type `Map(key, value)` from key-value pairs.
)";
    FunctionDocumentation::Syntax syntax_map = "map(key1, value1[, key2, value2, ...])";
    FunctionDocumentation::Arguments arguments_map = {
        {"key_n", "The keys of the map entries.", {"Any"}},
        {"value_n", "The values of the map entries.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_map = {"Returns a map containing key:value pairs.", {"Map(Any, Any)"}};
    FunctionDocumentation::Examples examples_map = {
        {"Usage example", "SELECT map('key1', number, 'key2', number * 2) FROM numbers(3)", "{'key1':0,'key2':0}\n{'key1':1,'key2':2}\n{'key1':2,'key2':4}"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_map = {21, 1};
    FunctionDocumentation::Category category_map = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_map = {description_map, syntax_map, arguments_map, {}, returned_value_map, examples_map, introduced_in_map, category_map};
    factory.registerFunction<FunctionMap>(documentation_map);

    /// mapFromArrays function documentation
    FunctionDocumentation::Description description_mapFromArrays = R"(
Creates a map from an array or map of keys and an array or map of values.
The function is a convenient alternative to syntax `CAST([...], 'Map(key_type, value_type)')`.
)";
    FunctionDocumentation::Syntax syntax_mapFromArrays = "mapFromArrays(keys, values)";
    FunctionDocumentation::Arguments arguments_mapFromArrays = {
        {"keys", "Array or map of keys to create the map from.", {"Array", "Map"}},
        {"values", "Array or map of values to create the map from.", {"Array", "Map"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapFromArrays = {"Returns a map with keys and values constructed from the key array and value array/map.", {"Map"}};
    FunctionDocumentation::Examples examples_mapFromArrays = {
        {"Basic usage", "SELECT mapFromArrays(['a', 'b', 'c'], [1, 2, 3])", "{'a':1,'b':2,'c':3}"},
        {"With map inputs", "SELECT mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))", "{1:('a',1),2:('b',2),3:('c',3)}"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapFromArrays = {23, 3};
    FunctionDocumentation::Category category_mapFromArrays = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapFromArrays = {description_mapFromArrays, syntax_mapFromArrays, arguments_mapFromArrays, {}, returned_value_mapFromArrays, examples_mapFromArrays, introduced_in_mapFromArrays, category_mapFromArrays};
    factory.registerFunction<FunctionMapFromArrays>(documentation_mapFromArrays);
    factory.registerAlias("MAP_FROM_ARRAYS", "mapFromArrays");

    /// mapUpdate function documentation
    FunctionDocumentation::Description description_mapUpdate = R"(
For two maps, returns the first map with values updated on the values for the corresponding keys in the second map.
)";
    FunctionDocumentation::Syntax syntax_mapUpdate = "mapUpdate(map1, map2)";
    FunctionDocumentation::Arguments arguments_mapUpdate = {
        {"map1", "The map to update.", {"Map(K, V)"}},
        {"map2", "The map to use for updating.", {"Map(K, V)"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_mapUpdate = {"Returns `map1` with values updated from values for the corresponding keys in `map2`.", {"Map(K, V)"}};
    FunctionDocumentation::Examples examples_mapUpdate = {
        {"Basic usage", "SELECT mapUpdate(map('key1', 0, 'key3', 0), map('key1', 10, 'key2', 10))", "{'key3':0,'key1':10,'key2':10}"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_mapUpdate = {22, 3};
    FunctionDocumentation::Category category_mapUpdate = FunctionDocumentation::Category::Map;
    FunctionDocumentation documentation_mapUpdate = {description_mapUpdate, syntax_mapUpdate, arguments_mapUpdate, {}, returned_value_mapUpdate, examples_mapUpdate, introduced_in_mapUpdate, category_mapUpdate};
    factory.registerFunction<FunctionMapUpdate>(documentation_mapUpdate);
}

}
