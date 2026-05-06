#include <Storages/MaterializedIndex/MaterializedIndexAlgorithmFactory.h>
#include <Storages/MaterializedIndex/MaterializedIndexContext.h>
#include <Storages/MaterializedIndex/MockAnnAlgorithm.h>

#include <Common/Exception.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


MaterializedIndexAlgorithmFactory & MaterializedIndexAlgorithmFactory::instance()
{
    static MaterializedIndexAlgorithmFactory factory;
    static const bool initialized = []
    {
        // The "ann" family is the only registered algorithm at this point.
        // Real backends (DiskANN, HNSW, ...) plug in through additional
        // registerFamily calls from their own translation units.
        factory.registerFamily(
            "ann",
            [](const String & impl, const ASTPtr & /*build_params*/, const MaterializedIndexContext & /*ctx*/) -> MaterializedIndexAlgorithmPtr
            {
                if (impl == "MockAnn")
                    return std::make_unique<MockAnnAlgorithm>();
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Family 'ann' does not support impl '{}'", impl);
            },
            {"MockAnn"});
        return true;
    }();
    (void)initialized;
    return factory;
}

void MaterializedIndexAlgorithmFactory::registerFamily(const String & family_name, AlgorithmCreator creator, Strings supported_impls)
{
    std::unique_lock lock(mutex);
    auto [it, inserted] = families.try_emplace(family_name, FamilyEntry{std::move(creator), std::move(supported_impls)});
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MaterializedIndex family '{}' is already registered", family_name);
}

MaterializedIndexAlgorithmPtr MaterializedIndexAlgorithmFactory::get(
    const String & family,
    const String & impl,
    const ASTPtr & build_params,
    const MaterializedIndexContext & ctx) const
{
    std::shared_lock lock(mutex);
    auto it = families.find(family);
    if (it == families.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown MaterializedIndex family: '{}'", family);

    const auto & entry = it->second;
    if (std::find(entry.impls.begin(), entry.impls.end(), impl) == entry.impls.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Family '{}' does not support impl '{}'", family, impl);

    return entry.creator(impl, build_params, ctx);
}

bool MaterializedIndexAlgorithmFactory::hasFamily(const String & family) const
{
    std::shared_lock lock(mutex);
    return families.contains(family);
}

bool MaterializedIndexAlgorithmFactory::familySupportsImpl(const String & family, const String & impl) const
{
    std::shared_lock lock(mutex);
    auto it = families.find(family);
    if (it == families.end())
        return false;
    const auto & impls = it->second.impls;
    return std::find(impls.begin(), impls.end(), impl) != impls.end();
}

}
