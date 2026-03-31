#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/URL/protocol.h>

#include <string>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
}

namespace
{

struct URLComponents
{
    std::string_view scheme;     /// e.g. "http" (without ':')
    std::string_view authority;  /// e.g. "example.com:8080" (without "//")
    std::string_view path;       /// e.g. "/a/b/c"
    std::string_view query;      /// e.g. "key=val" (without '?')
    std::string_view fragment;   /// e.g. "frag" (without '#')

    bool has_scheme = false;
    bool has_authority = false;
    bool has_query = false;
    bool has_fragment = false;
};

/// Parse a URL into its components according to RFC 3986 Section 3.
/// Zero-copy: all returned string_views point into the input data.
/// Best-effort on malformed input, never throws.
URLComponents parseURLComponents(std::string_view url)
{
    URLComponents result;

    if (url.empty())
        return result;

    const char * pos = url.data();
    const char * end = url.data() + url.size();

    /// Step 1: Detect scheme using getURLScheme (D-04).
    /// getURLScheme returns the scheme without ':', e.g. "http" from "http://..."
    std::string_view scheme = getURLScheme(pos, end - pos);
    if (!scheme.empty())
    {
        const char * after_scheme = pos + scheme.size();
        if (after_scheme < end && *after_scheme == ':')
        {
            result.scheme = scheme;
            result.has_scheme = true;
            pos = after_scheme + 1; /// skip past ':'
        }
        /// else: looks like scheme chars but no ':', treat as relative reference
    }

    /// Step 2: Detect authority (starts with "//")
    if (pos + 1 < end && pos[0] == '/' && pos[1] == '/')
    {
        pos += 2; /// skip "//"
        result.has_authority = true;
        const char * auth_start = pos;
        /// Authority ends at '/', '?', '#', or end
        while (pos < end && *pos != '/' && *pos != '?' && *pos != '#')
            ++pos;
        result.authority = std::string_view(auth_start, pos - auth_start);
    }

    /// Step 3: Extract path (up to '?' or '#' or end)
    {
        const char * path_start = pos;
        while (pos < end && *pos != '?' && *pos != '#')
            ++pos;
        result.path = std::string_view(path_start, pos - path_start);
    }

    /// Step 4: Extract query (after '?' up to '#' or end)
    if (pos < end && *pos == '?')
    {
        ++pos; /// skip '?'
        result.has_query = true;
        const char * query_start = pos;
        while (pos < end && *pos != '#')
            ++pos;
        result.query = std::string_view(query_start, pos - query_start);
    }

    /// Step 5: Extract fragment (after '#' to end)
    if (pos < end && *pos == '#')
    {
        ++pos; /// skip '#'
        result.has_fragment = true;
        result.fragment = std::string_view(pos, end - pos);
    }

    return result;
}

/// RFC 3986 Section 5.2.4: remove_dot_segments algorithm.
/// Modifies path in-place. Output is always <= input length.
void removeDotSegments(std::string & path)
{
    if (path.empty())
        return;

    const char * input = path.data();
    const char * input_end = path.data() + path.size();
    char * output = path.data();
    char * output_start = output;

    while (input < input_end)
    {
        /// A: If the input buffer begins with a prefix of "../" or "./"
        if (input_end - input >= 3 && input[0] == '.' && input[1] == '.' && input[2] == '/')
        {
            input += 3;
            continue;
        }
        if (input_end - input >= 2 && input[0] == '.' && input[1] == '/')
        {
            input += 2;
            continue;
        }

        /// B: If the input buffer begins with a prefix of "/./" or "/."(end)
        if (input_end - input >= 3 && input[0] == '/' && input[1] == '.' && input[2] == '/')
        {
            input += 2; /// replace "/./" with "/", effectively skip "./"
            continue;
        }
        if (input_end - input == 2 && input[0] == '/' && input[1] == '.')
        {
            /// Replace "/." with "/"
            *output++ = '/';
            input += 2;
            continue;
        }

        /// C: If the input buffer begins with a prefix of "/../" or "/.."(end)
        if (input_end - input >= 4 && input[0] == '/' && input[1] == '.' && input[2] == '.' && input[3] == '/')
        {
            input += 3; /// replace "/../" with "/", effectively skip "../"
            /// Remove last segment from output
            if (output > output_start)
            {
                --output;
                while (output > output_start && *output != '/')
                    --output;
            }
            continue;
        }
        if (input_end - input == 3 && input[0] == '/' && input[1] == '.' && input[2] == '.')
        {
            /// Replace "/.." with "/"
            input += 3;
            /// Remove last segment from output
            if (output > output_start)
            {
                --output;
                while (output > output_start && *output != '/')
                    --output;
            }
            *output++ = '/';
            continue;
        }

        /// D: If the input buffer consists only of "." or ".."
        if ((input_end - input == 1 && input[0] == '.')
            || (input_end - input == 2 && input[0] == '.' && input[1] == '.'))
        {
            break;
        }

        /// E: Move the first path segment (including initial "/" if any) to output
        if (*input == '/')
        {
            *output++ = *input++;
        }
        while (input < input_end && *input != '/')
        {
            *output++ = *input++;
        }
    }

    path.resize(static_cast<size_t>(output - output_start));
}

/// RFC 3986 Section 5.2.3: merge base path with relative path.
std::string mergePaths(const URLComponents & base, std::string_view relative_path)
{
    if (base.has_authority && base.path.empty())
    {
        /// If the base URI has authority and an empty path,
        /// prepend "/" to the relative path
        std::string merged;
        merged.reserve(1 + relative_path.size());
        merged += '/';
        merged.append(relative_path);
        return merged;
    }

    /// Find the last '/' in base path
    std::string merged;
    auto last_slash = base.path.rfind('/');
    if (last_slash != std::string_view::npos)
    {
        merged.reserve(last_slash + 1 + relative_path.size());
        merged.append(base.path.data(), last_slash + 1);
    }
    merged.append(relative_path);
    return merged;
}

/// Recompose URL components into a string (RFC 3986 Section 5.3).
std::string recomposeURL(
    bool has_scheme, std::string_view scheme,
    bool has_authority, std::string_view authority,
    std::string_view path,
    bool has_query, std::string_view query,
    bool has_fragment, std::string_view fragment)
{
    std::string result;
    /// Estimate capacity
    size_t estimated = path.size() + 5; /// separators: ':' + "//" + '?' + '#'
    if (has_scheme)
        estimated += scheme.size() + 1;
    if (has_authority)
        estimated += authority.size() + 2;
    if (has_query)
        estimated += query.size() + 1;
    if (has_fragment)
        estimated += fragment.size() + 1;
    result.reserve(estimated);

    if (has_scheme)
    {
        result.append(scheme);
        result += ':';
    }
    if (has_authority)
    {
        result += '/';
        result += '/';
        result.append(authority);
    }
    result.append(path);
    if (has_query)
    {
        result += '?';
        result.append(query);
    }
    if (has_fragment)
    {
        result += '#';
        result.append(fragment);
    }
    return result;
}

/// RFC 3986 Section 5.2.2: reference resolution algorithm.
/// Resolves a relative URL reference against a base URL.
std::string resolveReference(std::string_view relative, std::string_view base)
{
    /// Edge cases
    if (relative.empty() && base.empty())
        return {};

    if (base.empty())
        return std::string(relative);

    URLComponents R = parseURLComponents(relative);
    URLComponents B = parseURLComponents(base);

    bool t_has_scheme = false;
    std::string_view t_scheme;
    bool t_has_authority = false;
    std::string_view t_authority;
    std::string t_path;
    bool t_has_query = false;
    std::string_view t_query;
    bool t_has_fragment = R.has_fragment;
    std::string_view t_fragment = R.fragment;

    if (R.has_scheme)
    {
        t_has_scheme = true;
        t_scheme = R.scheme;
        t_has_authority = R.has_authority;
        t_authority = R.authority;
        t_path = std::string(R.path);
        removeDotSegments(t_path);
        t_has_query = R.has_query;
        t_query = R.query;
    }
    else
    {
        if (R.has_authority)
        {
            t_has_authority = true;
            t_authority = R.authority;
            t_path = std::string(R.path);
            removeDotSegments(t_path);
            t_has_query = R.has_query;
            t_query = R.query;
        }
        else
        {
            if (R.path.empty())
            {
                t_path = std::string(B.path);
                if (R.has_query)
                {
                    t_has_query = true;
                    t_query = R.query;
                }
                else
                {
                    t_has_query = B.has_query;
                    t_query = B.query;
                }
            }
            else
            {
                if (R.path[0] == '/')
                {
                    t_path = std::string(R.path);
                    removeDotSegments(t_path);
                }
                else
                {
                    t_path = mergePaths(B, R.path);
                    removeDotSegments(t_path);
                }
                t_has_query = R.has_query;
                t_query = R.query;
            }
            t_has_authority = B.has_authority;
            t_authority = B.authority;
        }
        t_has_scheme = B.has_scheme;
        t_scheme = B.scheme;
    }

    return recomposeURL(
        t_has_scheme, t_scheme,
        t_has_authority, t_authority,
        t_path,
        t_has_query, t_query,
        t_has_fragment, t_fragment);
}

} /// anonymous namespace

class FunctionResolveRelativeURL : public IFunction
{
public:
    static constexpr auto name = "resolveRelativeURL";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionResolveRelativeURL>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected String",
                arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected String",
                arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_relative = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        const auto * col_base = checkAndGetColumn<ColumnString>(arguments[1].column.get());

        if (!col_relative)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), getName());

        if (!col_base)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}",
                arguments[1].column->getName(), getName());

        auto col_res = ColumnString::create();
        auto & res_chars = col_res->getChars();
        auto & res_offsets = col_res->getOffsets();

        const auto & rel_chars = col_relative->getChars();
        const auto & rel_offsets = col_relative->getOffsets();
        const auto & base_chars = col_base->getChars();
        const auto & base_offsets = col_base->getOffsets();

        res_chars.reserve(rel_chars.size() + base_chars.size());
        res_offsets.resize(input_rows_count);

        size_t res_offset = 0;
        size_t rel_prev_offset = 0;
        size_t base_prev_offset = 0;

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// ColumnString stores each string with a trailing null byte.
            /// The actual string length is offset[i] - offset[i-1] - 1.
            size_t rel_size = rel_offsets[i] - rel_prev_offset;
            size_t base_size = base_offsets[i] - base_prev_offset;

            std::string_view rel_sv(reinterpret_cast<const char *>(&rel_chars[rel_prev_offset]), rel_size > 0 ? rel_size - 1 : 0);
            std::string_view base_sv(reinterpret_cast<const char *>(&base_chars[base_prev_offset]), base_size > 0 ? base_size - 1 : 0);

            std::string result = resolveReference(rel_sv, base_sv);

            size_t new_size = res_offset + result.size() + 1;
            res_chars.resize(new_size);
            if (!result.empty())
                memcpy(&res_chars[res_offset], result.data(), result.size());
            res_chars[res_offset + result.size()] = '\0';
            res_offset = new_size;
            res_offsets[i] = res_offset;

            rel_prev_offset = rel_offsets[i];
            base_prev_offset = base_offsets[i];
        }

        return col_res;
    }
};

REGISTER_FUNCTION(ResolveRelativeURL)
{
    FunctionDocumentation::Description description = R"(
Resolves a relative URL against a base URL according to [RFC 3986](https://datatracker.ietf.org/doc/html/rfc3986#section-5).
The function implements the full reference resolution algorithm including `remove_dot_segments` path normalization.
    )";
    FunctionDocumentation::Syntax syntax = "resolveRelativeURL(relative_url, base_url)";
    FunctionDocumentation::Arguments function_arguments = {
        {"relative_url", "The relative URL to resolve. Can be a relative path, absolute path, or full URL with scheme.", {"String"}},
        {"base_url", "The base URL to resolve against.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Resolved absolute URL.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Relative path",
            R"(SELECT resolveRelativeURL('../g', 'http://a/b/c/d');)",
            R"(http://a/b/g)"
        },
        {
            "Query override",
            R"(SELECT resolveRelativeURL('?y', 'http://a/b/c/d;p?q');)",
            R"(http://a/b/c/d;p?y)"
        },
        {
            "New authority",
            R"(SELECT resolveRelativeURL('//other/path', 'http://a/b');)",
            R"(http://other/path)"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::URL;
    FunctionDocumentation documentation = {description, syntax, function_arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionResolveRelativeURL>(documentation);
}

} /// namespace DB
