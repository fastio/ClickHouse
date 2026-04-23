#include <Storages/MergeTree/ANNIndex/ANNIndexManifest.h>

#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Common/Exception.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>

#include <base/hex.h>

#include <filesystem>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

namespace fs = std::filesystem;

namespace
{
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 4096;
    constexpr std::string_view TMP_SUFFIX = ".tmp";

    String formatHexU64(UInt64 v)
    {
        /// Match the project-wide "0x" + lowercase fixed-width hex convention.
        return "0x" + getHexUIntLowercase(v);
    }

    UInt64 parseHexU64(const String & s, const char * field_name)
    {
        std::string_view sv(s);
        if (sv.substr(0, 2) == "0x" || sv.substr(0, 2) == "0X")
            sv.remove_prefix(2);
        if (sv.empty() || sv.size() > 16)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`manifest.json`: field `{}` is not a valid hex uint64: `{}`", field_name, s);

        UInt64 result = 0;
        for (char c : sv)
        {
            UInt8 d = unhex(c);
            if (d == 0xFF && !(c == '0'))
            {
                /// `unhex` returns 0 for '0' and also 0 for garbage in some branches; to be safe,
                /// check by parsing manually below.
            }
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "`manifest.json`: field `{}` contains non-hex character: `{}`", field_name, s);
            result = (result << 4) | d;
        }
        return result;
    }

    DiskPtr getDisk(VolumePtr volume)
    {
        if (!volume)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndexManifest: null volume");
        return volume->getDisk(0);
    }
}

ANNIndexManifest ANNIndexManifest::loadOrEmpty(VolumePtr volume, const std::string & relative_root_path)
{
    auto disk = getDisk(volume);
    const auto full_path = fs::path(relative_root_path) / FILE_NAME;

    if (!disk->existsFile(full_path))
        return {};

    auto in = disk->readFile(full_path, ReadSettings{}, std::nullopt);
    String contents;
    {
        const size_t file_size = disk->getFileSize(full_path);
        contents.resize(file_size);
        if (file_size > 0)
            in->readStrict(contents.data(), file_size);
    }

    Poco::JSON::Object::Ptr root;
    try
    {
        Poco::JSON::Parser parser;
        auto parsed = parser.parse(contents);
        root = parsed.extract<Poco::JSON::Object::Ptr>();
        if (!root)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`manifest.json`: root is not a JSON object");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`manifest.json`: failed to parse JSON: {}", e.displayText());
    }

    ANNIndexManifest manifest;
    try
    {
        manifest.version = root->getValue<UInt32>("version");

        auto shape_ptr = root->getObject("shape");
        if (!shape_ptr)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "`manifest.json`: missing `shape`");
        manifest.shape.dim = shape_ptr->getValue<UInt32>("dim");
        manifest.shape.metric = static_cast<UInt8>(shape_ptr->getValue<UInt32>("metric"));
        manifest.shape.algorithm = shape_ptr->getValue<std::string>("algorithm");
        manifest.shape.params_hash = parseHexU64(
            shape_ptr->getValue<std::string>("params_hash"), "shape.params_hash");

        manifest.hash_algo = root->getValue<std::string>("hash_algo");
        manifest.hash_seed = parseHexU64(
            root->getValue<std::string>("hash_seed"), "hash_seed");

        if (auto active_arr = root->getArray("active_groups"))
        {
            manifest.active_groups.reserve(active_arr->size());
            for (unsigned int i = 0; i < active_arr->size(); ++i)
                manifest.active_groups.push_back(active_arr->getElement<std::string>(i));
        }
        if (auto retired_arr = root->getArray("retired_groups"))
        {
            manifest.retired_groups.reserve(retired_arr->size());
            for (unsigned int i = 0; i < retired_arr->size(); ++i)
                manifest.retired_groups.push_back(retired_arr->getElement<std::string>(i));
        }
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`manifest.json`: malformed structure: {}", e.displayText());
    }

    return manifest;
}

void ANNIndexManifest::writeTo(
    VolumePtr volume,
    const std::string & relative_root_path,
    DiskTransactionPtr txn) const
{
    Poco::JSON::Object root;
    root.set("version", version);

    Poco::JSON::Object shape_obj;
    shape_obj.set("dim", shape.dim);
    shape_obj.set("metric", static_cast<UInt32>(shape.metric));
    shape_obj.set("algorithm", shape.algorithm);
    shape_obj.set("params_hash", formatHexU64(shape.params_hash));
    root.set("shape", shape_obj);

    root.set("hash_algo", hash_algo);
    root.set("hash_seed", formatHexU64(hash_seed));

    Poco::JSON::Array active_arr;
    for (const auto & g : active_groups)
        active_arr.add(g);
    root.set("active_groups", active_arr);

    Poco::JSON::Array retired_arr;
    for (const auto & g : retired_groups)
        retired_arr.add(g);
    root.set("retired_groups", retired_arr);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(root, oss, 2, -1, Poco::JSON_WRAP_STRINGS | Poco::JSON_ESCAPE_UNICODE);
    const String payload = oss.str();

    auto disk = getDisk(volume);
    const auto target_path = fs::path(relative_root_path) / FILE_NAME;

    if (txn)
    {
        /// Transactional write: commit is the caller's responsibility; they will use the same
        /// transaction for the corresponding group directory rename so that the manifest and
        /// filesystem state become visible atomically.
        auto out = txn->writeFile(target_path, DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
        return;
    }

    /// Non-transactional path: write to `manifest.json.tmp` then atomically rename over the
    /// existing `manifest.json`. This survives a crash between the two operations: the
    /// previous `manifest.json` remains intact until the rename completes.
    const auto tmp_path = fs::path(relative_root_path) / (String(FILE_NAME) + String(TMP_SUFFIX));
    {
        auto out = disk->writeFile(tmp_path, DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});
        out->write(payload.data(), payload.size());
        out->finalize();
    }
    disk->replaceFile(tmp_path, target_path);
}

}
