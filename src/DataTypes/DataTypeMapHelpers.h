#pragma once

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <base/types.h>

#include <optional>
#include <utility>
#include <vector>

namespace DB
{

/// Optimized extraction of values for a given constant key from a Map column
/// stored as Array(Tuple(K, V)).
///
/// For each row in [start, end), finds the key-value pair matching `key`
/// and inserts the corresponding value into `result`. If the key is not found,
/// inserts a default value (or null for Nullable value types).
void extractKeyValueFromMap(
    const IColumn & nested_column,
    const IColumn & key,
    IColumn & result,
    size_t start,
    size_t end);

/// Extract values for every registered key in one pass over the Map rows.
/// `results[i]` receives the physical values for `keys[i]` on rows `[start, end)`.
/// Missing keys insert the result column's default (SQL `NULL` for `Nullable`).
/// Duplicate keys in a row keep the first occurrence.
void extractRegisteredKeyValuesFromMap(
    const IColumn & nested_column,
    const std::vector<Field> & keys,
    const std::vector<IColumn *> & results,
    size_t start,
    size_t end);

/// Try to parse a Map subcolumn reference like `map.key_<serialized_key>`.
/// Returns {map_column_name, serialized_key} if the column name has the expected format.
std::optional<std::pair<String, String>> tryParseMapSubcolumnName(const String & column_name);

}
