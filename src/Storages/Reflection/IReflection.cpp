#include <Storages/Reflection/IReflection.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{
[[noreturn]] void rejectAsReflection(std::string_view operation)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "{} is not supported on a Reflection. A Reflection is a derived object recomputed from its source table; "
        "operate on the source table instead.",
        operation);
}
}

void IReflection::read(
    QueryPlan &,
    const Names &,
    const StorageSnapshotPtr &,
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t,
    size_t)
{
    rejectAsReflection("Direct SELECT");
}

SinkToStoragePtr IReflection::write(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr,
    bool)
{
    rejectAsReflection("Direct INSERT");
}

void IReflection::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr,
    TableExclusiveLockHolder &)
{
    rejectAsReflection("TRUNCATE");
}

void IReflection::alter(const AlterCommands &, ContextPtr, AlterLockHolder &)
{
    rejectAsReflection("ALTER");
}

void IReflection::checkAlterIsPossible(const AlterCommands &, ContextPtr) const
{
    rejectAsReflection("ALTER");
}

void IReflection::checkMutationIsPossible(const MutationCommands &, const Settings &) const
{
    rejectAsReflection("Mutations");
}

void IReflection::mutate(const MutationCommands &, ContextPtr)
{
    rejectAsReflection("Mutations");
}

void IReflection::backupData(
    BackupEntriesCollector &,
    const String &,
    const std::optional<ASTs> &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "BACKUP of a Reflection is not supported. Back up the source table; the Reflection will be rebuilt on demand.");
}

void IReflection::restoreDataFromBackup(
    RestorerFromBackup &,
    const String &,
    const std::optional<ASTs> &)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "RESTORE of a Reflection is not supported. Restore the source table and the Reflection will be rebuilt on demand.");
}

void IReflection::drop()
{
    /// Catalog removal of the Reflection cascades to its inner storage. The
    /// inner table is not user-visible and has no independent lifetime: it
    /// only exists to back this Reflection's data. We hand off to
    /// `dropInnerTableIfAny`, which the catalog also calls explicitly during
    /// `DROP TABLE ... SYNC`; the override there releases extra ENGINE state
    /// (e.g. replicated leader leases) before dropping the inner storage.
}

void IReflection::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    auto inner = getReflectionInnerTable();
    if (!inner)
        return;

    auto inner_id = inner->getStorageID();
    if (!DatabaseCatalog::instance().tryGetTable(inner_id, local_context))
        return;

    InterpreterDropQuery::executeDropQuery(
        ASTDropQuery::Kind::Drop,
        local_context,
        local_context,
        inner_id,
        sync,
        /*ignore_sync_setting=*/true,
        /*need_ddl_guard=*/false);
}

std::vector<StorageID> IReflection::getInnerStorageIDs() const
{
    if (auto inner = getReflectionInnerTable())
        return {inner->getStorageID()};
    return {};
}

StoragePolicyPtr IReflection::getStoragePolicy() const
{
    if (auto inner = getReflectionInnerTable())
        return inner->getStoragePolicy();
    return {};
}

Strings IReflection::getDataPaths() const
{
    if (auto inner = getReflectionInnerTable())
        return inner->getDataPaths();
    return {};
}

bool IReflection::storesDataOnDisk() const
{
    if (auto inner = getReflectionInnerTable())
        return inner->storesDataOnDisk();
    return false;
}

IStorage::ColumnSizeByName IReflection::getColumnSizes() const
{
    if (auto inner = getReflectionInnerTable())
        return inner->getColumnSizes();
    return {};
}

IStorage::IndexSizeByName IReflection::getSecondaryIndexSizes() const
{
    if (auto inner = getReflectionInnerTable())
        return inner->getSecondaryIndexSizes();
    return {};
}

std::optional<UInt64> IReflection::totalRows(ContextPtr query_context) const
{
    if (auto inner = getReflectionInnerTable())
        return inner->totalRows(query_context);
    return {};
}

std::optional<UInt64> IReflection::totalBytes(ContextPtr query_context) const
{
    if (auto inner = getReflectionInnerTable())
        return inner->totalBytes(query_context);
    return {};
}

std::optional<UInt64> IReflection::totalBytesUncompressed(const Settings & settings) const
{
    if (auto inner = getReflectionInnerTable())
        return inner->totalBytesUncompressed(settings);
    return {};
}

ActionLock IReflection::getActionLock(StorageActionBlockType action_type)
{
    if (auto inner = getReflectionInnerTable())
        return inner->getActionLock(action_type);
    return {};
}

void IReflection::onActionLockRemove(StorageActionBlockType action_type)
{
    if (auto inner = getReflectionInnerTable())
        inner->onActionLockRemove(action_type);
}

}
