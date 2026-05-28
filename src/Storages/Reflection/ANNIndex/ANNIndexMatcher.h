#pragma once

#include <Storages/Reflection/IReflectionMatcher.h>

#include <optional>


namespace DB
{

class ReflectionANNIndex;

/// ANN-specific implementation of `IReflectionMatcher`. Held as a member of
/// `ReflectionANNIndex` and exposed via `getReflectionMatcher()` so the optimizer
/// can dispatch to it through the generic `IReflection` interface — the
/// storage no longer inherits a matcher interface itself.
class ANNIndexMatcher final : public IReflectionMatcher
{
public:
    explicit ANNIndexMatcher(ReflectionANNIndex & storage_);
    ~ANNIndexMatcher() override = default;

    ReflectionMatchKind matchKind() const override { return ReflectionMatchKind::ReadHint; }

    std::optional<ReflectionReadHintOffer> matchReadHint(
        const ReflectionPlanShape & shape, ContextPtr query_context) override;

    ReflectionReadHintRealization realizeReadHint(
        const ReflectionPlanShape & shape,
        const ReflectionReadHintOffer & offer,
        ContextPtr query_context) override;

private:
    ReflectionANNIndex & storage;
};

}
