#pragma once

#include <Storages/Reflection/ANNIndex/IANNIndexAlgorithm.h>

#include <Core/Names.h>
#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <memory>
#include <shared_mutex>
#include <unordered_map>


namespace DB
{

struct ANNIndexContext;

/// Singleton registry for ANNIndex algorithm families.
///
/// A family (e.g. "ann") picks a C++ class; an impl (e.g. "diskann")
/// selects behaviour inside that class. Registration is expected
/// to happen once at process startup during static initialisation; lookup is
/// lock-free on the happy path.
class ANNAlgorithmFactory : private boost::noncopyable
{
public:
    using AlgorithmCreator = std::function<ANNIndexAlgorithmPtr(
        const String & impl,
        const ASTPtr & build_params,
        const ANNIndexContext & ctx)>;

    static ANNAlgorithmFactory & instance();

    /// Register the creator for a family along with the list of impl names
    /// that the creator recognises. `supported_impls` is a closed set
    /// enumerated at registration time; `get` rejects anything outside it.
    void registerFamily(const String & family_name, AlgorithmCreator creator, Strings supported_impls);

    ANNIndexAlgorithmPtr get(
        const String & family,
        const String & impl,
        const ASTPtr & build_params,
        const ANNIndexContext & ctx) const;

    bool hasFamily(const String & family) const;
    bool familySupportsImpl(const String & family, const String & impl) const;

private:
    ANNAlgorithmFactory() = default;

    struct FamilyEntry
    {
        AlgorithmCreator creator;
        Strings impls;
    };

    mutable std::shared_mutex mutex;
    std::unordered_map<String, FamilyEntry> families;
};

}
