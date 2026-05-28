#include <Storages/Reflection/ANNIndex/ANNAlgorithmFactory.h>
#include <Storages/Reflection/ANNIndex/ANNIndexContext.h>

#include "config.h"

#if USE_DISKANN
#include <Storages/Reflection/ANNIndex/DiskANNAlgorithm.h>
#endif
#if USE_SPTAG
#include <Storages/Reflection/ANNIndex/SPANNAlgorithm.h>
#endif

#include <Common/Exception.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


ANNAlgorithmFactory & ANNAlgorithmFactory::instance()
{
    static ANNAlgorithmFactory factory;
    static const bool initialized = []
    {
        Strings supported_impls;
#if USE_DISKANN
        supported_impls.emplace_back("diskann");
#endif
#if USE_SPTAG
        supported_impls.emplace_back("spann");
#endif

        factory.registerFamily(
            "ann",
            [](const String & impl, const ASTPtr & build_params, const ANNIndexContext & /*ctx*/) -> ANNIndexAlgorithmPtr
            {
#if USE_DISKANN
                if (impl == "diskann")
                {
                    auto algo = std::make_unique<DiskANNAlgorithm>();
                    if (build_params)
                        algo->setBuildParameters(build_params, nullptr);
                    return algo;
                }
#else
                (void)build_params;
#endif
#if USE_SPTAG
                if (impl == "spann")
                {
                    auto algo = std::make_unique<SPANNAlgorithm>();
                    if (build_params)
                        algo->setBuildParameters(build_params, nullptr);
                    return algo;
                }
#endif
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Family 'ann' does not support impl '{}'", impl);
            },
            supported_impls);
        return true;
    }();
    (void)initialized;
    return factory;
}

void ANNAlgorithmFactory::registerFamily(const String & family_name, AlgorithmCreator creator, Strings supported_impls)
{
    std::unique_lock lock(mutex);
    auto [it, inserted] = families.try_emplace(family_name, FamilyEntry{std::move(creator), std::move(supported_impls)});
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ANNIndex family '{}' is already registered", family_name);
}

ANNIndexAlgorithmPtr ANNAlgorithmFactory::get(
    const String & family,
    const String & impl,
    const ASTPtr & build_params,
    const ANNIndexContext & ctx) const
{
    std::shared_lock lock(mutex);
    auto it = families.find(family);
    if (it == families.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown ANNIndex family: '{}'", family);

    const auto & entry = it->second;
    if (std::find(entry.impls.begin(), entry.impls.end(), impl) == entry.impls.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Family '{}' does not support impl '{}'", family, impl);

    return entry.creator(impl, build_params, ctx);
}

bool ANNAlgorithmFactory::hasFamily(const String & family) const
{
    std::shared_lock lock(mutex);
    return families.contains(family);
}

bool ANNAlgorithmFactory::familySupportsImpl(const String & family, const String & impl) const
{
    std::shared_lock lock(mutex);
    auto it = families.find(family);
    if (it == families.end())
        return false;
    const auto & impls = it->second.impls;
    return std::find(impls.begin(), impls.end(), impl) != impls.end();
}

}
