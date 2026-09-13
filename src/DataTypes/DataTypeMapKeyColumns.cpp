#include <DataTypes/DataTypeMapKeyColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

bool isForbiddenMapKeyColumnsNestedType(const IDataType & type)
{
    const WhichDataType which(type);
    return which.isNullable()
        || which.isTuple()
        || which.isMap()
        || which.isObject()
        || which.isDynamic()
        || which.isVariant()
        || which.isAggregateFunction();
}

bool canBeMapKeyColumnsScalarType(const DataTypePtr & type)
{
    return type->canBeInsideNullable() && !isForbiddenMapKeyColumnsNestedType(*type);
}

}

bool canBeMapKeyColumnsValueType(const DataTypePtr & type)
{
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(type.get()))
    {
        const auto & dictionary_type = low_cardinality->getDictionaryType();
        const auto * nullable = typeid_cast<const DataTypeNullable *>(dictionary_type.get());
        if (!nullable)
            return false;
        return canBeMapKeyColumnsScalarType(nullable->getNestedType());
    }

    if (const auto * array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        /// An array element's `NULL` is independent of the outer key-presence bitmap.
        if (const auto * nullable = typeid_cast<const DataTypeNullable *>(array->getNestedType().get()))
            return canBeMapKeyColumnsScalarType(nullable->getNestedType());
        return canBeMapKeyColumnsValueType(array->getNestedType());
    }

    return canBeMapKeyColumnsScalarType(type);
}

DataTypePtr getValueTypeForMapKeyColumn(const DataTypePtr & value_type)
{
    if (!canBeMapKeyColumnsValueType(value_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Type {} cannot be used as the value type of a Map column with map_serialization_version = 'with_key_columns'",
            value_type->getName());
    }

    if (value_type->lowCardinality())
        return value_type;

    if (value_type->canBeInsideNullable())
        return makeNullable(value_type);

    return DataTypeNullable::createForInternalUse(value_type);
}

bool isMapKeyColumnsKeyColumn(const NameAndTypePair & column)
{
    return column.isSubcolumn()
        && typeid_cast<const DataTypeMap *>(column.getTypeInStorage().get())
        && column.getSubcolumnName().starts_with(DataTypeMap::KEY_SUBCOLUMN_PREFIX);
}

NameAndTypePair adjustMapKeyColumnIfNeeded(NameAndTypePair column, bool uses_key_columns)
{
    if (!uses_key_columns || !isMapKeyColumnsKeyColumn(column))
        return column;

    const auto & map_type = assert_cast<const DataTypeMap &>(*column.getTypeInStorage());

    return NameAndTypePair(
        column.getNameInStorage(),
        String(column.getSubcolumnName()),
        column.getTypeInStorage(),
        getValueTypeForMapKeyColumn(map_type.getValueType()));
}

}
