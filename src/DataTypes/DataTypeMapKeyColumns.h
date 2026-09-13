#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/IDataType.h>

namespace DB
{

/// Value types allowed for `Map(K, V)` when `map_serialization_version = 'with_key_columns'`.
/// User-declared `Nullable(V)`, `Tuple`, nested `Map`, `Dynamic`, `Variant`, `JSON` / `Object`,
/// and `AggregateFunction` are rejected at the value level. `Array(T)` is allowed when `T`
/// is itself allowed, or is `Nullable(S)` for an allowed scalar `S`.
/// `LowCardinality` is allowed only as `LowCardinality(Nullable(T))` for an allowed scalar `T`.
bool canBeMapKeyColumnsValueType(const DataTypePtr & type);

/// Physical subcolumn / `m['k']` type for a `with_key_columns` `Map` value type.
/// Scalars and `Array` are wrapped in `Nullable`. `LowCardinality(Nullable(T))` is unchanged.
DataTypePtr getValueTypeForMapKeyColumn(const DataTypePtr & value_type);

/// True when `column` is a `key_*` subcolumn of a `Map`, as built for the gathering stage of a
/// `with_key_columns` merge. Unlike the name-based `tryParseMapSubcolumnName`, this cannot mistake a
/// physical column that merely contains `.key_` - such as the flattened `Nested` column
/// `n.key_id` - for a Map key subcolumn.
bool isMapKeyColumnsKeyColumn(const NameAndTypePair & column);

/// If `column` is a `key_*` subcolumn of a `Map` and the table uses `with_key_columns`,
/// replace the value type `V` with the physical type from `getValueTypeForMapKeyColumn`.
NameAndTypePair adjustMapKeyColumnIfNeeded(NameAndTypePair column, bool uses_key_columns);

}
