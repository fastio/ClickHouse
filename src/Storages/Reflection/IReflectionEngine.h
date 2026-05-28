#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/Reflection/IStorageReflection.h>

#include <memory>

namespace DB
{

class Context;
using ContextPtr = std::shared_ptr<const Context>;

struct ReflectionPlanContext
{
    ContextPtr query_context;
};

struct ReflectionMatchResult
{
    ReflectionMatchKind kind = ReflectionMatchKind::ReadHint;
    String engine_name;
    String reason;
};

/// Minimal engine-side contract. The framework owns catalog/lifecycle plumbing;
/// each engine owns its matching rules, cost model details, and private tasks.
class IReflectionEngine
{
public:
    virtual ~IReflectionEngine() = default;

    virtual String getName() const = 0;
    virtual ReflectionMatchKind getDefaultMatchKind() const = 0;
    virtual void validateCreate(const ASTPtr & create_query, ContextPtr context) const = 0;
};

}
