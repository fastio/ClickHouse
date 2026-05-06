#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MaterializedIndex/StorageReplicatedMaterializedIndex.h>

#include <Storages/StorageFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// The MaterializedIndex / ReplicatedMaterializedIndex factory entries are
/// addressed by engine key only; construction always goes through
/// InterpreterCreateQuery::doCreateTable, which owns the AST unpacking needed
/// for the real ctor call. Reaching the lambda therefore means something
/// handed the factory a CREATE statement it was not meant to see.
static StoragePtr rejectFactoryInvocation(const char * engine_name, const StorageFactory::Arguments & /*args*/)
{
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Storage {} must be constructed via InterpreterCreateQuery for MaterializedIndex tables, not through the factory creator",
        engine_name);
}


void registerStorageMaterializedIndex(StorageFactory & factory)
{
    factory.registerStorage(
        "MaterializedIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            return rejectFactoryInvocation("MaterializedIndex", args);
        });
}

void registerStorageReplicatedMaterializedIndex(StorageFactory & factory)
{
    factory.registerStorage(
        "ReplicatedMaterializedIndex",
        [](const StorageFactory::Arguments & args) -> StoragePtr
        {
            return rejectFactoryInvocation("ReplicatedMaterializedIndex", args);
        });
}

}
