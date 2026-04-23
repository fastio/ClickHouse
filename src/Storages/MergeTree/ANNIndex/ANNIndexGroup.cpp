#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromFileBase.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <base/hex.h>

#include <filesystem>
#include <string>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{
    /// Minimal hex parser matching the one in `ANNIndexManifest.cpp`. Kept local because it is
    /// only needed by the meta.json reader and cross-file sharing is not worth the header
    /// churn.
    UInt64 parseHexU64(const std::string & s, const char * field)
    {
        std::string_view sv(s);
        if (sv.substr(0, 2) == "0x" || sv.substr(0, 2) == "0X")
            sv.remove_prefix(2);
        if (sv.empty() || sv.size() > 16)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`meta.json`: field `{}` is not a valid hex uint64: `{}`", field, s);

        UInt64 result = 0;
        for (char c : sv)
        {
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')))
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "`meta.json`: field `{}` contains non-hex character: `{}`", field, s);
            result = (result << 4) | unhex(c);
        }
        return result;
    }

    struct MetaJson
    {
        ANNIndexShapeFingerprint shape;
        UInt64 hash_seed = 0;
        UInt64 num_points = 0;
    };

    MetaJson readMetaJson(IANNGroupStorage & storage)
    {
        const std::string name(ANNIndexGroup::META_FILE_NAME);
        if (!storage.existsFile(name))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ANNIndexGroup: `meta.json` is missing from group `{}`", storage.getGroupDir());

        const size_t file_size = storage.getFileSize(name);
        auto in = storage.readFile(name, ReadSettings{}, file_size);
        std::string contents;
        contents.resize(file_size);
        if (file_size > 0)
            in->readStrict(contents.data(), file_size);

        Poco::JSON::Object::Ptr root;
        try
        {
            Poco::JSON::Parser parser;
            auto parsed = parser.parse(contents);
            root = parsed.extract<Poco::JSON::Object::Ptr>();
            if (!root)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "`meta.json`: root is not a JSON object");
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`meta.json`: failed to parse: {}", e.displayText());
        }

        MetaJson m;
        try
        {
            auto shape = root->getObject("shape");
            if (!shape)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "`meta.json`: missing `shape`");
            m.shape.dim = shape->getValue<UInt32>("dim");
            m.shape.metric = static_cast<UInt8>(shape->getValue<UInt32>("metric"));
            m.shape.algorithm = shape->getValue<std::string>("algorithm");
            m.shape.params_hash = parseHexU64(shape->getValue<std::string>("params_hash"), "shape.params_hash");

            m.hash_seed = parseHexU64(root->getValue<std::string>("hash_seed"), "hash_seed");
            m.num_points = root->getValue<UInt64>("num_points");
        }
        catch (const Exception &)
        {
            throw;
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`meta.json`: malformed structure: {}", e.displayText());
        }
        return m;
    }
}

ANNIndexGroup::ANNIndexGroup(
    ANNGroupStoragePtr storage_,
    ANNIndexShapeFingerprint shape_,
    UInt64 hash_seed_,
    DiskANNDiskIndexSearcherPtr searcher_,
    PartRowIdMapReader id_map_,
    ANNGroupCoverage coverage_)
    : storage(std::move(storage_))
    , shape(std::move(shape_))
    , hash_seed(hash_seed_)
    , searcher(std::move(searcher_))
    , id_map(std::move(id_map_))
    , coverage(std::move(coverage_))
{
}

ANNIndexGroup::ANNIndexGroup(
    test_only_t,
    ANNIndexShapeFingerprint shape_,
    UInt64 hash_seed_,
    ANNGroupCoverage coverage_)
    : storage(nullptr)
    , shape(std::move(shape_))
    , hash_seed(hash_seed_)
    , searcher(nullptr)
    , id_map()
    , coverage(std::move(coverage_))
{
}

std::shared_ptr<ANNIndexGroup> ANNIndexGroup::load(
    ANNGroupStoragePtr storage,
    const DiskANNSearchOptions & search_options)
{
    const MetaJson meta = readMetaJson(*storage);

    /// Build the index prefix understood by the FFI: `<group_full_path>/idx`.
    namespace fs = std::filesystem;
    const auto index_prefix = (fs::path(storage->getFullPath()) / DiskANNArtifactNames::INDEX_PREFIX_BASENAME).string();

    auto searcher = std::make_shared<DiskANNDiskIndexSearcher>(
        meta.shape.dim,
        static_cast<DiskANNMetric>(meta.shape.metric),
        index_prefix,
        search_options);

    PartRowIdMapReader id_map;
    id_map.loadFrom(*storage, ReadSettings{});

    ANNGroupCoverage coverage;
    coverage.readFrom(*storage);

    if (id_map.size() != meta.num_points)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "ANNIndexGroup: id_map row count ({}) does not match `meta.json` `num_points` ({})",
            id_map.size(), meta.num_points);

    return std::make_shared<ANNIndexGroup>(
        std::move(storage),
        std::move(meta.shape),
        meta.hash_seed,
        std::move(searcher),
        std::move(id_map),
        std::move(coverage));
}

std::vector<ANNIndexGroup::SearchHit> ANNIndexGroup::search(
    const float * query,
    size_t query_dim,
    size_t k,
    size_t search_list_size,
    size_t beam_width) const
{
    if (k == 0)
        return {};

    std::vector<uint64_t> ids(k);
    std::vector<float> distances(k);

    const size_t found = searcher->search(
        query, query_dim, k, ids.data(), distances.data(), search_list_size, beam_width);

    std::vector<SearchHit> hits;
    hits.reserve(found);
    for (size_t i = 0; i < found; ++i)
    {
        const uint64_t raw = ids[i];
        if (raw > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "ANNIndexGroup::search: FFI returned internal_id {} which exceeds UInt32", raw);
        hits.push_back(SearchHit{static_cast<UInt32>(raw), distances[i]});
    }
    return hits;
}

}
#endif
