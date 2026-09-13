#include <DataTypes/DataTypeMapHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/Exception.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/assert_cast.h>
#include <base/StringViewHash.h>
#include <base/memcmpSmall.h>

#include <map>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// A sentinel value meaning "key not found in this row".
constexpr size_t KEY_NOT_FOUND = std::numeric_limits<size_t>::max();

/// ---------------------------------------------------------------------------
/// Phase 1: Find the position of the requested key in each row.
///
/// Builds `matched_positions[i]` = flat index into the keys/values column
/// of the first key matching `key` in row (start + i), or KEY_NOT_FOUND.
/// ---------------------------------------------------------------------------

/// Generic key matcher that uses virtual compareAt. Used as a fallback
/// when the key column type is not one of the specialized types.
struct KeyMatcherGeneric
{
    const IColumn & keys_column;
    const IColumn & key;

    bool match(size_t keys_row) const
    {
        return keys_column.compareAt(keys_row, 0, key, 0) == 0;
    }
};

/// Specialized key matcher for ColumnVector<T>. Compares values directly
/// without virtual dispatch.
template <typename T>
struct KeyMatcherVector
{
    const typename ColumnVector<T>::Container & data;
    T key_value;

    bool match(size_t keys_row) const
    {
        return data[keys_row] == key_value;
    }
};

/// Specialized key matcher for ColumnString. Compares string data directly
/// using size check + memcmp, avoiding virtual dispatch and assert_cast.
struct KeyMatcherString
{
    const ColumnString::Chars & chars;
    const ColumnString::Offsets & string_offsets;
    const char * key_data;
    size_t key_size;

    bool match(size_t keys_row) const
    {
        size_t offset = string_offsets[ssize_t(keys_row) - 1];
        size_t size = string_offsets[keys_row] - offset;
        if (size != key_size)
            return false;
        return memcmp(&chars[offset], key_data, key_size) == 0;
    }
};

/// Specialized key matcher for ColumnFixedString. Compares fixed-size data
/// directly using memcmpSmallAllowOverflow15.
struct KeyMatcherFixedString
{
    const ColumnFixedString::Chars & chars;
    size_t n;
    const UInt8 * key_data;

    bool match(size_t keys_row) const
    {
        return memcmpSmallAllowOverflow15(&chars[keys_row * n], key_data, n) == 0;
    }
};

/// The core position-finding loop, parametrized by Matcher type.
/// For each row in [start, end), finds the flat index of the matching key.
///
/// `m[key]` returns the value of the FIRST occurrence of the key in the row, so
/// each row is scanned left to right and the first match is taken. Duplicate keys
/// in a Map are a legal (if degenerate) state, so a position-prediction shortcut
/// across rows is not used: it could accept a later duplicate at the predicted
/// offset while an earlier occurrence exists, yielding a value that depends on the
/// preceding rows in the block (see issue #111203). Ruling out an earlier duplicate
/// still requires scanning from the start, so there is no correct constant-time
/// shortcut to prefer over the scan.
template <typename Matcher>
void findKeyPositions(
    const ColumnArray::Offsets & offsets,
    const Matcher & matcher,
    size_t start,
    size_t end,
    PaddedPODArray<size_t> & matched_positions)
{
    size_t num_rows = end - start;
    matched_positions.resize(num_rows);

    for (size_t i = start; i < end; ++i)
    {
        size_t positions_row_idx = i - start;
        size_t offset_start = offsets[ssize_t(i) - 1];
        size_t offset_end = offsets[i];

        matched_positions[positions_row_idx] = KEY_NOT_FOUND;
        for (size_t j = offset_start; j < offset_end; ++j)
        {
            if (matcher.match(j))
            {
                matched_positions[positions_row_idx] = j;
                break;
            }
        }
    }
}

/// Dispatches to the appropriate specialized matcher based on the key column type,
/// then calls findKeyPositions with that matcher.
void findKeyPositionsDispatch(
    const IColumn & keys_column,
    const ColumnArray::Offsets & offsets,
    const IColumn & key,
    size_t start,
    size_t end,
    PaddedPODArray<size_t> & matched_positions)
{
    TypeIndex type_id = keys_column.getDataType();

    /// Try ColumnVector<T> specializations.
    switch (type_id)
    {
#define DISPATCH_VECTOR(T) \
        case TypeIndex::T: \
        { \
            using ColType = ColumnVector<T>; \
            const auto & typed_col = assert_cast<const ColType &>(keys_column); \
            const auto & key_col = assert_cast<const ColType &>(key); \
            KeyMatcherVector<T> matcher{typed_col.getData(), key_col.getData()[0]}; \
            findKeyPositions(offsets, matcher, start, end, matched_positions); \
            return; \
        }

        DISPATCH_VECTOR(UInt8)
        DISPATCH_VECTOR(UInt16)
        DISPATCH_VECTOR(UInt32)
        DISPATCH_VECTOR(UInt64)
        DISPATCH_VECTOR(Int8)
        DISPATCH_VECTOR(Int16)
        DISPATCH_VECTOR(Int32)
        DISPATCH_VECTOR(Int64)
        DISPATCH_VECTOR(Float32)
        DISPATCH_VECTOR(Float64)
#undef DISPATCH_VECTOR

        case TypeIndex::String:
        {
            const auto & typed_col = assert_cast<const ColumnString &>(keys_column);
            const auto & key_col = assert_cast<const ColumnString &>(key);
            auto key_ref = key_col.getDataAt(0);
            KeyMatcherString matcher{typed_col.getChars(), typed_col.getOffsets(), key_ref.data(), key_ref.size()};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
            return;
        }
        case TypeIndex::FixedString:
        {
            const auto & typed_col = assert_cast<const ColumnFixedString &>(keys_column);
            const auto & key_col = assert_cast<const ColumnFixedString &>(key);
            KeyMatcherFixedString matcher{typed_col.getChars(), typed_col.getN(), key_col.getChars().data()};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
            return;
        }
        default:
        {
            /// Fallback: generic matcher using virtual compareAt.
            KeyMatcherGeneric matcher{keys_column, key};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
        }
    }
}

/// ---------------------------------------------------------------------------
/// Phase 2: Extract values at the matched positions.
///
/// For each row, if matched_positions[i] != KEY_NOT_FOUND, copy the value from
/// values_column at that flat index into result. Otherwise, insert a default.
/// ---------------------------------------------------------------------------

/// Generic value extractor using virtual insertFrom / insertDefault.
void extractValuesGeneric(
    const IColumn & values_column,
    IColumn & result,
    const PaddedPODArray<size_t> & matched_positions)
{
    auto * nullable_result = typeid_cast<ColumnNullable *>(&result);
    const auto * nullable_values = typeid_cast<const ColumnNullable *>(&values_column);

    result.reserve(result.size() + matched_positions.size());
    for (size_t pos : matched_positions)
    {
        if (pos != KEY_NOT_FOUND)
        {
            if (nullable_result && !nullable_values)
                nullable_result->insertFromNotNullable(values_column, pos);
            else
                result.insertFrom(values_column, pos);
        }
        else
            result.insertDefault();
    }
}

/// Specialized value extractor for ColumnVector<T>, with optional Nullable support.
/// If src_null_map / dst_null_map are non-null, propagates null flags.
/// For missing keys, inserts a default value and sets the null flag to 1.
template <typename T>
void extractValuesVector(
    const ColumnVector<T> & values_column,
    ColumnVector<T> & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    const auto & src_data = values_column.getData();
    auto & dst_data = result.getData();
    size_t old_size = dst_data.size();
    size_t num_rows = matched_positions.size();
    dst_data.resize(old_size + num_rows);
    if (dst_null_map)
        dst_null_map->resize(old_size + num_rows);

    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            dst_data[old_size + i] = src_data[pos];
            if (dst_null_map)
                (*dst_null_map)[old_size + i] = src_null_map ? (*src_null_map)[pos] : 0;
        }
        else
        {
            dst_data[old_size + i] = T{};
            if (dst_null_map)
                (*dst_null_map)[old_size + i] = static_cast<UInt8>(1);
        }
    }
}

/// Specialized value extractor for ColumnString, with optional Nullable support.
/// Two-pass approach: first pass computes offsets, total chars size, and fills null map;
/// second pass re-iterates matched_positions to copy string data.
void extractValuesString(
    const ColumnString & values_column,
    ColumnString & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    const auto & src_chars = values_column.getChars();
    const auto & src_offsets = values_column.getOffsets();
    auto & dst_chars = result.getChars();
    auto & dst_offsets = result.getOffsets();

    size_t old_offsets_size = dst_offsets.size();
    size_t num_rows = matched_positions.size();

    dst_offsets.resize(old_offsets_size + num_rows);
    if (dst_null_map)
        dst_null_map->resize(dst_null_map->size() + num_rows);

    /// First pass: compute result offsets, total chars size, and fill null map.
    /// total_chars_size must start from the existing chars size because
    /// ColumnString offsets are absolute positions into the chars array.
    size_t old_chars_size = dst_chars.size();
    size_t total_chars_size = old_chars_size;
    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            total_chars_size += src_offsets[pos] - src_offsets[ssize_t(pos) - 1];
            if (dst_null_map)
                (*dst_null_map)[old_offsets_size + i] = src_null_map ? (*src_null_map)[pos] : 0;
        }
        else
        {
            if (dst_null_map)
                (*dst_null_map)[old_offsets_size + i] = static_cast<UInt8>(1);
        }
        dst_offsets[old_offsets_size + i] = total_chars_size;
    }

    /// Second pass: resize chars once and copy string data.
    dst_chars.resize(total_chars_size);
    size_t current_offset = old_chars_size;
    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            size_t src_offset = src_offsets[ssize_t(pos) - 1];
            size_t src_size = src_offsets[pos] - src_offset;
            memcpy(&dst_chars[current_offset], &src_chars[src_offset], src_size);
            current_offset += src_size;
        }
    }
}

/// Specialized value extractor for ColumnFixedString, with optional Nullable support.
void extractValuesFixedString(
    const ColumnFixedString & values_column,
    ColumnFixedString & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    size_t n = values_column.getN();
    const auto & src_chars = values_column.getChars();
    auto & dst_chars = result.getChars();
    size_t num_rows = matched_positions.size();

    size_t old_chars_size = dst_chars.size();
    size_t old_num_rows = old_chars_size / n;
    dst_chars.resize(old_chars_size + num_rows * n);
    if (dst_null_map)
        dst_null_map->resize(old_num_rows + num_rows);

    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            memcpy(&dst_chars[old_chars_size + i * n], &src_chars[pos * n], n);
            if (dst_null_map)
                (*dst_null_map)[old_num_rows + i] = src_null_map ? (*src_null_map)[pos] : 0;
        }
        else
        {
            memset(&dst_chars[old_chars_size + i * n], 0, n);
            if (dst_null_map)
                (*dst_null_map)[old_num_rows + i] = static_cast<UInt8>(1);
        }
    }
}

/// Dispatches to the appropriate specialized value extractor based on the value column type.
/// For Nullable columns, unwraps to the nested column and passes null map pointers
/// to the same extractors used for non-nullable columns.
void extractValuesDispatch(
    const IColumn & values_column,
    IColumn & result,
    const PaddedPODArray<size_t> & matched_positions)
{
    /// Unwrap Nullable on the source and/or the result. A `with_key_columns` Map stores
    /// `Nullable(V)` while the in-memory Map values are still `V`.
    const IColumn * data_column = &values_column;
    IColumn * result_data_column = &result;
    const NullMap * src_null_map = nullptr;
    NullMap * dst_null_map = nullptr;

    if (auto * nullable_result = typeid_cast<ColumnNullable *>(&result))
    {
        result_data_column = &nullable_result->getNestedColumn();
        dst_null_map = &nullable_result->getNullMapData();
    }

    if (const auto * nullable_values = typeid_cast<const ColumnNullable *>(&values_column))
    {
        data_column = &nullable_values->getNestedColumn();
        src_null_map = &nullable_values->getNullMapData();
    }

    /// Keep the outer `Nullable` for types without a specialized extractor so
    /// `insertDefault` writes a SQL `NULL` rather than a nested type default.
    if (dst_null_map)
    {
        TypeIndex type_id = data_column->getDataType();
        switch (type_id)
        {
#define DISPATCH_VECTOR_NULLABLE(T) \
            case TypeIndex::T: \
            { \
                using ColType = ColumnVector<T>; \
                extractValuesVector<T>( \
                    assert_cast<const ColType &>(*data_column), \
                    assert_cast<ColType &>(*result_data_column), \
                    matched_positions, src_null_map, dst_null_map); \
                return; \
            }

            DISPATCH_VECTOR_NULLABLE(UInt8)
            DISPATCH_VECTOR_NULLABLE(UInt16)
            DISPATCH_VECTOR_NULLABLE(UInt32)
            DISPATCH_VECTOR_NULLABLE(UInt64)
            DISPATCH_VECTOR_NULLABLE(Int8)
            DISPATCH_VECTOR_NULLABLE(Int16)
            DISPATCH_VECTOR_NULLABLE(Int32)
            DISPATCH_VECTOR_NULLABLE(Int64)
            DISPATCH_VECTOR_NULLABLE(Float32)
            DISPATCH_VECTOR_NULLABLE(Float64)
#undef DISPATCH_VECTOR_NULLABLE

            case TypeIndex::String:
            {
                extractValuesString(
                    assert_cast<const ColumnString &>(*data_column),
                    assert_cast<ColumnString &>(*result_data_column),
                    matched_positions, src_null_map, dst_null_map);
                return;
            }
            case TypeIndex::FixedString:
            {
                extractValuesFixedString(
                    assert_cast<const ColumnFixedString &>(*data_column),
                    assert_cast<ColumnFixedString &>(*result_data_column),
                    matched_positions, src_null_map, dst_null_map);
                return;
            }
            default:
            {
                extractValuesGeneric(values_column, result, matched_positions);
                return;
            }
        }
    }

    TypeIndex type_id = data_column->getDataType();

    switch (type_id)
    {
#define DISPATCH_VECTOR(T) \
        case TypeIndex::T: \
        { \
            using ColType = ColumnVector<T>; \
            extractValuesVector<T>( \
                assert_cast<const ColType &>(*data_column), \
                assert_cast<ColType &>(*result_data_column), \
                matched_positions, src_null_map, dst_null_map); \
            return; \
        }

        DISPATCH_VECTOR(UInt8)
        DISPATCH_VECTOR(UInt16)
        DISPATCH_VECTOR(UInt32)
        DISPATCH_VECTOR(UInt64)
        DISPATCH_VECTOR(Int8)
        DISPATCH_VECTOR(Int16)
        DISPATCH_VECTOR(Int32)
        DISPATCH_VECTOR(Int64)
        DISPATCH_VECTOR(Float32)
        DISPATCH_VECTOR(Float64)
#undef DISPATCH_VECTOR

        case TypeIndex::String:
        {
            extractValuesString(
                assert_cast<const ColumnString &>(*data_column),
                assert_cast<ColumnString &>(*result_data_column),
                matched_positions, src_null_map, dst_null_map);
            return;
        }
        case TypeIndex::FixedString:
        {
            extractValuesFixedString(
                assert_cast<const ColumnFixedString &>(*data_column),
                assert_cast<ColumnFixedString &>(*result_data_column),
                matched_positions, src_null_map, dst_null_map);
            return;
        }
        default:
        {
            /// Fallback for all other column types (handles both Nullable and non-Nullable).
            extractValuesGeneric(values_column, result, matched_positions);
        }
    }
}

void initRegisteredKeyPositions(
    std::vector<PaddedPODArray<size_t>> & matched_positions,
    size_t num_keys,
    size_t num_rows)
{
    matched_positions.resize(num_keys);
    for (auto & positions : matched_positions)
        positions.resize_fill(num_rows, KEY_NOT_FOUND);
}

template <typename Lookup, typename GetKey>
void fillRegisteredKeyPositions(
    const ColumnArray::Offsets & offsets,
    size_t start,
    size_t end,
    const Lookup & lookup,
    const GetKey & get_key,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    for (size_t row = start; row < end; ++row)
    {
        const size_t row_idx = row - start;
        const size_t kv_start = offsets[ssize_t(row) - 1];
        const size_t kv_end = offsets[row];
        for (size_t j = kv_start; j < kv_end; ++j)
        {
            const auto it = lookup.find(get_key(j));
            if (it == lookup.end())
                continue;
            size_t & position = matched_positions[it->second][row_idx];
            if (position == KEY_NOT_FOUND)
                position = j;
        }
    }
}

template <typename T>
void findRegisteredKeyPositionsVector(
    const ColumnVector<T> & keys_column,
    const ColumnArray::Offsets & offsets,
    const std::vector<Field> & keys,
    size_t start,
    size_t end,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    std::unordered_map<T, size_t> lookup;
    lookup.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        lookup.emplace(applyVisitor(FieldVisitorConvertToNumber<T>(), keys[i]), i);

    const auto & data = keys_column.getData();
    fillRegisteredKeyPositions(
        offsets,
        start,
        end,
        lookup,
        [&](size_t j) { return data[j]; },
        matched_positions);
}

void findRegisteredKeyPositionsString(
    const ColumnString & keys_column,
    const ColumnArray::Offsets & offsets,
    const std::vector<Field> & keys,
    size_t start,
    size_t end,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    std::unordered_map<std::string_view, size_t, StringViewHash> lookup;
    lookup.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        lookup.emplace(keys[i].safeGet<String>(), i);

    const auto & chars = keys_column.getChars();
    const auto & string_offsets = keys_column.getOffsets();
    fillRegisteredKeyPositions(
        offsets,
        start,
        end,
        lookup,
        [&](size_t j) -> std::string_view
        {
            const size_t offset = string_offsets[ssize_t(j) - 1];
            const size_t size = string_offsets[j] - offset;
            return {reinterpret_cast<const char *>(&chars[offset]), size};
        },
        matched_positions);
}

void findRegisteredKeyPositionsFixedString(
    const ColumnFixedString & keys_column,
    const ColumnArray::Offsets & offsets,
    const std::vector<Field> & keys,
    size_t start,
    size_t end,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    std::unordered_map<std::string_view, size_t, StringViewHash> lookup;
    lookup.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        lookup.emplace(keys[i].safeGet<String>(), i);

    const size_t n = keys_column.getN();
    const auto & chars = keys_column.getChars();
    fillRegisteredKeyPositions(
        offsets,
        start,
        end,
        lookup,
        [&](size_t j) -> std::string_view
        {
            return {reinterpret_cast<const char *>(&chars[j * n]), n};
        },
        matched_positions);
}

void findRegisteredKeyPositionsGeneric(
    const IColumn & keys_column,
    const ColumnArray::Offsets & offsets,
    const std::vector<Field> & keys,
    size_t start,
    size_t end,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    std::map<Field, size_t> lookup;
    for (size_t i = 0; i < keys.size(); ++i)
        lookup.emplace(keys[i], i);

    fillRegisteredKeyPositions(
        offsets,
        start,
        end,
        lookup,
        [&](size_t j)
        {
            Field key;
            keys_column.get(j, key);
            return key;
        },
        matched_positions);
}

void findRegisteredKeyPositionsDispatch(
    const IColumn & keys_column,
    const ColumnArray::Offsets & offsets,
    const std::vector<Field> & keys,
    size_t start,
    size_t end,
    std::vector<PaddedPODArray<size_t>> & matched_positions)
{
    initRegisteredKeyPositions(matched_positions, keys.size(), end - start);

    switch (keys_column.getDataType())
    {
#define DISPATCH_VECTOR(T) \
        case TypeIndex::T: \
        { \
            findRegisteredKeyPositionsVector<T>( \
                assert_cast<const ColumnVector<T> &>(keys_column), \
                offsets, \
                keys, \
                start, \
                end, \
                matched_positions); \
            return; \
        }

        DISPATCH_VECTOR(UInt8)
        DISPATCH_VECTOR(UInt16)
        DISPATCH_VECTOR(UInt32)
        DISPATCH_VECTOR(UInt64)
        DISPATCH_VECTOR(Int8)
        DISPATCH_VECTOR(Int16)
        DISPATCH_VECTOR(Int32)
        DISPATCH_VECTOR(Int64)
#undef DISPATCH_VECTOR

        case TypeIndex::String:
        {
            findRegisteredKeyPositionsString(
                assert_cast<const ColumnString &>(keys_column), offsets, keys, start, end, matched_positions);
            return;
        }
        case TypeIndex::FixedString:
        {
            findRegisteredKeyPositionsFixedString(
                assert_cast<const ColumnFixedString &>(keys_column), offsets, keys, start, end, matched_positions);
            return;
        }
        default:
        {
            findRegisteredKeyPositionsGeneric(keys_column, offsets, keys, start, end, matched_positions);
        }
    }
}

}

void extractKeyValueFromMap(
    const IColumn & nested_column,
    const IColumn & key,
    IColumn & result,
    size_t start,
    size_t end)
{
    const auto & array_column = assert_cast<const ColumnArray &>(nested_column);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(array_column.getData());
    const auto & offsets = array_column.getOffsets();
    const auto & keys_column = tuple_column.getColumn(0);
    const auto & values_column = tuple_column.getColumn(1);

    /// Phase 1: find the position of the requested key in each row.
    PaddedPODArray<size_t> matched_positions;
    findKeyPositionsDispatch(keys_column, offsets, key, start, end, matched_positions);

    /// Phase 2: extract values at the matched positions.
    extractValuesDispatch(values_column, result, matched_positions);
}

void extractRegisteredKeyValuesFromMap(
    const IColumn & nested_column,
    const std::vector<Field> & keys,
    const std::vector<IColumn *> & results,
    size_t start,
    size_t end)
{
    if (keys.size() != results.size())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of registered Map keys {} does not match result columns {}",
            keys.size(),
            results.size());
    }

    if (keys.empty() || start >= end)
        return;

    const auto & array_column = assert_cast<const ColumnArray &>(nested_column);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(array_column.getData());
    const auto & keys_column = tuple_column.getColumn(0);
    const auto & values_column = tuple_column.getColumn(1);

    if (keys.size() == 1)
    {
        auto key_column = keys_column.cloneEmpty();
        key_column->insert(keys[0]);
        extractKeyValueFromMap(nested_column, *key_column, *results[0], start, end);
        return;
    }

    std::vector<PaddedPODArray<size_t>> matched_positions;
    findRegisteredKeyPositionsDispatch(keys_column, array_column.getOffsets(), keys, start, end, matched_positions);
    for (size_t i = 0; i < keys.size(); ++i)
        extractValuesDispatch(values_column, *results[i], matched_positions[i]);
}

std::optional<std::pair<String, String>> tryParseMapSubcolumnName(const String & column_name)
{
    static constexpr std::string_view key_marker = ".key_";

    auto pos = column_name.find(key_marker);
    if (pos == String::npos)
        return std::nullopt;

    auto map_column_name = column_name.substr(0, pos);
    auto serialized_key = column_name.substr(pos + key_marker.size());
    return std::pair{std::move(map_column_name), std::move(serialized_key)};
}

}
