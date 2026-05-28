#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>

#include <chrono>

namespace DB
{

enum class ReflectionMatchKind : uint8_t
{
    PlanRewrite,
    ReadHint,
    CostHint,
};

struct ReflectionObservabilitySnapshot
{
    UInt64 backlog_rows = 0;
    UInt64 backlog_bytes = 0;
    UInt64 backlog_parts = 0;
    UInt64 pending_task_count = 0;
    UInt64 ready_part_count = 0;
    UInt64 obsolete_ready_source_count = 0;
    UInt64 repeated_failure_count = 0;
    UInt64 tombstone_rows = 0;
    double tombstone_ratio = 0.0;
    UInt64 retry_count = 0;
    std::chrono::system_clock::time_point next_retry_time{};
    String last_error;
};

/// Common catalog-facing surface for source-derived objects. Engine-specific
/// storage classes keep their build policy and private metadata, while system
/// tables and DDL plumbing can enumerate them uniformly through this interface.
class IStorageReflection
{
public:
    virtual ~IStorageReflection() = default;

    virtual const StorageID & getReflectionSourceTableID() const = 0;
    virtual const Names & getReflectionIndexedColumns() const = 0;
    virtual const String & getReflectionFamily() const = 0;
    virtual const String & getReflectionImpl() const = 0;
    virtual String getReflectionEngineName() const = 0;
    virtual StoragePtr getReflectionInnerTable() const = 0;
    virtual ReflectionObservabilitySnapshot getReflectionObservabilitySnapshot() const = 0;
};

}
