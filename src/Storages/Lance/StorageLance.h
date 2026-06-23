#pragma once

#include "config.h"

#if USE_LANCE

#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>

namespace DB
{

/// Reads an external Lance dataset as a table. The actual columnar decoding happens in the Rust
/// `lance` crate; data crosses the FFI boundary as Arrow record batches (Arrow C Data Interface)
/// and is converted to ClickHouse Chunks via ArrowColumnToCHColumn.
class StorageLance final : public IStorage
{
public:
    StorageLance(const StorageID & table_id_, const ColumnsDescription & columns_, String uri_);

    std::string getName() const override { return "Lance"; }

    /// Infer the ClickHouse table structure from a Lance dataset's Arrow schema. Lives here (in the
    /// `dbms` target, which links the lance FFI) so the table function need not touch the FFI itself.
    static ColumnsDescription getTableStructureFromData(const String & uri, const ContextPtr & context);

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
};

}

#endif
