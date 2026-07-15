#pragma once

#include "config.h"

#if USE_LANCE

#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>

#include <map>

namespace DB
{

/// Object-store storage options forwarded verbatim to the Lance Rust SDK (object_store config keys,
/// e.g. access_key_id / secret_access_key / region / endpoint / allow_http). Empty for local paths.
using LanceStorageOptions = std::map<String, String>;

/// Parse the arguments shared by the `lance()` table function and the `Lance` table engine into a
/// dataset URI plus object-store storage options. Accepts either
///   lance(uri [, access_key_id, secret_access_key [, session_token]])
/// or a named collection (its `url` key is the URI; every other key is forwarded as a storage option).
void parseLanceArguments(ASTs & args, const ContextPtr & context, String & uri, LanceStorageOptions & storage_options);

/// Reads an external Lance dataset as a table. The actual columnar decoding happens in the Rust
/// `lance` crate; data crosses the FFI boundary as Arrow record batches (Arrow C Data Interface)
/// and is converted to ClickHouse Chunks via ArrowColumnToCHColumn.
class StorageLance final : public IStorage
{
public:
    StorageLance(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        String uri_,
        LanceStorageOptions storage_options_);

    std::string getName() const override { return "Lance"; }

    /// Infer the ClickHouse table structure from a Lance dataset's Arrow schema. Lives here (in the
    /// `dbms` target, which links the lance FFI) so the table function need not touch the FFI itself.
    /// `version == 0` reads the latest version.
    static ColumnsDescription getTableStructureFromData(
        const String & uri, const LanceStorageOptions & storage_options, UInt64 version, const ContextPtr & context);

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        ContextPtr context,
        bool async_insert) override;

private:
    String uri;
    LanceStorageOptions storage_options;
};

}

#endif
