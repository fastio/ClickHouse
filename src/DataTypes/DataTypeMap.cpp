#include <Common/StringUtils.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMapHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/SipHash.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMapKeyColumns.h>
#include <DataTypes/Serializations/SerializationMap.h>
#include <DataTypes/Serializations/SerializationMapKeyValue.h>
#include <DataTypes/Serializations/SerializationMapKeyColumns.h>
#include <DataTypes/Serializations/SerializationNamed.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <DataTypes/Serializations/SerializationWrapper.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Parsers/IAST.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
}

DataTypeMap::DataTypeMap(const DataTypePtr & nested_)
    : nested(nested_)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!type_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->hasExplicitNames())
    {
        const auto & names = type_tuple->getElementNames();
        if (names[0] != "keys" || names[1] != "values")
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Expected Tuple(key, value) with explicit names 'keys', 'values', got explicit names '{}', '{}'", names[0], names[1]);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Expected Tuple(key, value) with explicit names 'keys', 'values', got without explicit names");

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    assertKeyType();
}

DataTypeMap::DataTypeMap(const DataTypes & elems_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    assertKeyType();

    nested = std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMap::DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_), value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(
        std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"})))
{
    assertKeyType();
}

void DataTypeMap::assertKeyType() const
{
    if (!isValidKeyType(key_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Map cannot have a key of type {}", key_type->getName());
}


std::string DataTypeMap::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getName() << ", " << value_type->getName() << ")";

    return s.str();
}

std::string DataTypeMap::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "Map(" << key_type->getPrettyName(indent) << ", " << value_type->getPrettyName(indent) << ')';
    return s.str();
}

MutableColumnPtr DataTypeMap::createColumn() const
{
    return ColumnMap::create(nested->createColumn());
}

Field DataTypeMap::getDefault() const
{
    return Map();
}

SerializationPtr DataTypeMap::doGetSerialization(const SerializationInfoSettings & settings) const
{
    SerializationPtr key_serialization;
    SerializationPtr value_serialization;
    if (settings.propagate_types_serialization_versions_to_nested_types)
    {
        key_serialization = key_type->getSerialization(settings);
        value_serialization = value_type->getSerialization(settings);
    }
    else
    {
        key_serialization = key_type->getDefaultSerialization();
        value_serialization = value_type->getDefaultSerialization();
    }

    /// Don't use nested->getSerialization() to avoid creating exponentially growing number of serializations for deep nested maps.
    /// Instead, reuse already created serializations for keys and values.
    auto key_serialization_named = std::static_pointer_cast<const SerializationNamed>(SerializationNamed::create(key_serialization, "keys", SubstreamType::TupleElement));
    auto value_serialization_named = std::static_pointer_cast<const SerializationNamed>(SerializationNamed::create(value_serialization, "values", SubstreamType::TupleElement));
    auto nested_serialization = SerializationArray::create(SerializationTuple::create(SerializationTuple::ElementSerializations{key_serialization_named, value_serialization_named}, true));
    auto text_serialization = SerializationMap::create(key_serialization, value_serialization, nested_serialization, MergeTreeMapSerializationVersion::BASIC);

    if (settings.map_serialization_version == MergeTreeMapSerializationVersion::WITH_KEY_COLUMNS)
    {
        auto physical_value_type = getValueTypeForMapKeyColumn(value_type);
        auto physical_serialization = physical_value_type->getSerialization(settings);
        return SerializationMapKeyColumns::create(
            key_type, value_type, physical_value_type, key_serialization, text_serialization, physical_serialization);
    }

    return SerializationMap::create(key_serialization, value_serialization, nested_serialization, settings.map_serialization_version);
}

bool DataTypeMap::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMap & rhs_map = static_cast<const DataTypeMap &>(rhs);
    return nested->equals(*rhs_map.nested);
}

bool DataTypeMap::isValidKeyType(DataTypePtr key_type)
{
    return !isNullableOrLowCardinalityNullable(key_type);
}

DataTypePtr DataTypeMap::getNestedTypeWithUnnamedTuple() const
{
    const auto & from_array = assert_cast<const DataTypeArray &>(*nested);
    const auto & from_tuple = assert_cast<const DataTypeTuple &>(*from_array.getNestedType());
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(from_tuple.getElements()));
}

DataTypePtr DataTypeMap::getNestedDataType() const
{
    return assert_cast<const DataTypeArray &>(*nested).getNestedType();
}

void DataTypeMap::updateHashImpl(SipHash & hash) const
{
    key_type->updateHash(hash);
    value_type->updateHash(hash);
}

void DataTypeMap::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    callback(*key_type);
    callback(*value_type);
    key_type->forEachChild(callback);
    value_type->forEachChild(callback);
}

/// Resolves a dynamic subcolumn like `map['key']` by parsing the key from the subcolumn name,
/// creating a `SerializationMapKeyValue` that knows how to read only the relevant bucket,
/// and optionally pre-extracting the values from an existing column.
/// The subcolumn name must start with "key_" followed by the text-serialized key value.
std::unique_ptr<IDataType::SubstreamData> DataTypeMap::getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t /*initial_array_level*/, bool throw_if_null) const
{
    SerializationPtr serialization = removeNamedSerialization(data.serialization);
    /// `SerializationMapKeyColumns` is itself a `SerializationWrapper`. Check it before unwrapping,
    /// then peel Sparse / Replicated / other wrappers that may sit outside the Map serialization.
    for (size_t i = 0; i < 8; ++i)
    {
        if (typeid_cast<const SerializationMapKeyColumns *>(serialization.get())
            || typeid_cast<const SerializationMap *>(serialization.get()))
            break;

        if (const auto * wrapper = typeid_cast<const SerializationWrapper *>(serialization.get()))
        {
            serialization = wrapper->getNested();
            continue;
        }
        break;
    }

    if (subcolumn_name == "keys" && typeid_cast<const SerializationMapKeyColumns *>(serialization.get()))
    {
        auto result = std::make_unique<SubstreamData>(std::make_shared<SerializationMapKeyPresence>(serialization, std::nullopt));
        result->type = std::make_shared<DataTypeArray>(key_type);
        if (data.column)
        {
            const auto & map = assert_cast<const ColumnMap &>(*data.column);
            result->column = ColumnArray::create(map.getNestedData().getColumnPtr(0), map.getNestedColumn().getOffsetsPtr());
        }
        return result;
    }
    const bool existence = subcolumn_name.starts_with(EXISTS_SUBCOLUMN_PREFIX);
    if (existence && !canBeMapKeyColumnsValueType(value_type))
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Existence subcolumns require a with_key_columns-compatible Map value type");
        return nullptr;
    }

    /// Existence uses a separate prefix so a key ending in `.null` stays unambiguous.
    if (!existence && !subcolumn_name.starts_with(KEY_SUBCOLUMN_PREFIX))
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} doesn't have subcolumn {}", getName(), subcolumn_name);
        return nullptr;
    }

    /// Parse the key value from the subcolumn name.
    std::string_view key_string = subcolumn_name.substr(existence ? EXISTS_SUBCOLUMN_PREFIX.size() : KEY_SUBCOLUMN_PREFIX.size());
    auto key_column = key_type->createColumn();
    auto key_serialization = key_type->getDefaultSerialization();
    ReadBufferFromString buf(key_string);
    try
    {
        key_serialization->deserializeWholeText(*key_column, buf, FormatSettings{});
    }
    catch (...)
    {
        if (throw_if_null)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Type {} doesn't have subcolumn {}", getName(), subcolumn_name);
        return nullptr;
    }

    if (existence)
    {
        Field requested;
        key_column->get(0, requested);
        auto result = std::make_unique<SubstreamData>(std::make_shared<SerializationMapKeyPresence>(serialization, requested));
        result->type = std::make_shared<DataTypeUInt8>();
        if (data.column)
        {
            const auto & map = assert_cast<const ColumnMap &>(*data.column);
            auto values = getValueTypeForMapKeyColumn(value_type)->createColumn();
            extractKeyValueFromMap(map.getNestedColumn(), *key_column, *values, 0, map.size());
            auto exists = ColumnUInt8::create();
            for (size_t row = 0; row < values->size(); ++row)
                exists->getData().push_back(!values->isNullAt(row));
            result->column = std::move(exists);
        }
        return result;
    }

    if (const auto * per_key = typeid_cast<const SerializationMapKeyColumns *>(serialization.get()))
    {
        auto key_subcolumn_name = String(KEY_SUBCOLUMN_PREFIX) + String(key_string);
        auto key_value_serialization = SerializationMapKeyColumn::create(
            per_key->getPhysicalSerialization(),
            per_key->getPtr(),
            key_column->getPtr(),
            key_subcolumn_name);
        std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(key_value_serialization);
        res->type = per_key->getPhysicalValueType();

        if (data.column)
        {
            const auto & column_map = assert_cast<const ColumnMap &>(*data.column);
            auto value_column = per_key->getPhysicalValueType()->createColumn();
            extractKeyValueFromMap(*column_map.getNestedColumnPtr(), *key_column->getPtr(), *value_column, 0, data.column->size());
            res->column = std::move(value_column);
        }

        return res;
    }

    /// Create a serialization that reads only the bucket containing the requested key.
    const auto & map_serialization = assert_cast<const SerializationMap &>(*serialization);
    auto key_value_serialization = SerializationMapKeyValue::create(
        map_serialization.getValueSerialization(),
        map_serialization.getNestedSerialization(),
        map_serialization.getMapSerializationVersion(),
        key_column->getPtr(),
        nested);
    std::unique_ptr<SubstreamData> res = std::make_unique<SubstreamData>(key_value_serialization);
    res->type = value_type;

    /// If a column is available, pre-extract the values for the requested key.
    if (data.column)
    {
        const auto & column_map = assert_cast<const ColumnMap &>(*data.column);
        auto value_column = value_type->createColumn();
        extractKeyValueFromMap(*column_map.getNestedColumnPtr(), *key_column->getPtr(), *value_column, 0, data.column->size());
        res->column = std::move(value_column);
    }

    return res;
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Map data type family must have two arguments: key and value types");

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMap>(nested_types);
}


void registerDataTypeMap(DataTypeFactory & factory)
{
    factory.registerDataType("Map", create);
}

}
