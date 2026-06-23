#include <Storages/Lance/StorageLance.h>

#if USE_LANCE

#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Common/Exception.h>

#include <arrow/c/bridge.h>
#include <arrow/table.h>

#include <lance.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// A single-stream source pulling Arrow record batches from a Lance scan via FFI.
class LanceSource final : public ISource
{
public:
    LanceSource(String uri_, Names column_names_, Block header_, const ContextPtr & context_, size_t batch_size_)
        : ISource(std::make_shared<const Block>(header_))
        , uri(std::move(uri_))
        , column_names(std::move(column_names_))
        , header(std::move(header_))
        , format_settings(getFormatSettings(context_))
    {
        open(batch_size_);
    }

    ~LanceSource() override
    {
        if (stream)
            lance_stream_free(stream);
        if (scanner)
            lance_scanner_free(scanner);
        if (dataset)
            lance_dataset_free(dataset);
    }

    String getName() const override { return "Lance"; }

protected:
    Chunk generate() override
    {
        if (!stream)
            return {};

        while (true)
        {
            ArrowArray c_array = {};
            ArrowSchema c_schema = {};
            const int32_t rc = lance_stream_next(stream, &c_array, &c_schema);

            if (rc == 1)
                return {};
            if (rc < 0)
                throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Lance scan failed: {}", lance_last_error());

            auto batch_result = arrow::ImportRecordBatch(&c_array, &c_schema);
            if (!batch_result.ok())
                throw Exception(
                    ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Failed to import Lance record batch: {}", batch_result.status().ToString());

            auto table_result = arrow::Table::FromRecordBatches({*batch_result});
            if (!table_result.ok())
                throw Exception(
                    ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Failed to build Arrow table from Lance batch: {}", table_result.status().ToString());

            const auto & table = *table_result;
            const size_t num_rows = table->num_rows();
            /// An empty batch must not be returned as it would signal end-of-stream; skip to the next one.
            if (num_rows == 0)
                continue;

            ArrowColumnToCHColumn converter(
                header,
                "Lance",
                format_settings,
                /* parquet_columns_to_clickhouse */ std::nullopt,
                /* clickhouse_columns_to_parquet */ std::nullopt,
                /* allow_missing_columns */ false,
                /* null_as_default */ false,
                format_settings.date_time_overflow_behavior,
                /* allow_geoparquet_parser */ false);

            return converter.arrowTableToCHChunk(table, num_rows, nullptr, nullptr);
        }
    }

private:
    void open(size_t batch_size)
    {
        dataset = lance_dataset_open(uri.c_str());
        if (!dataset)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot open Lance dataset '{}': {}", uri, lance_last_error());

        scanner = lance_scanner_create(dataset);
        if (!scanner)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot create Lance scanner: {}", lance_last_error());

        if (!column_names.empty())
        {
            std::vector<const char *> cols;
            cols.reserve(column_names.size());
            for (const auto & name : column_names)
                cols.push_back(name.c_str());
            if (lance_scanner_project(scanner, cols.data(), cols.size()) < 0)
                throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Lance column projection failed: {}", lance_last_error());
        }

        if (batch_size > 0)
            lance_scanner_batch_size(scanner, batch_size);

        stream = lance_scanner_open_stream(scanner);
        if (!stream)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot open Lance scan stream: {}", lance_last_error());
    }

    String uri;
    Names column_names;
    Block header;
    FormatSettings format_settings;

    LanceDataset * dataset = nullptr;
    LanceScanner * scanner = nullptr;
    LanceRecordBatchStream * stream = nullptr;
};

/// Sink converting ClickHouse Blocks to Arrow record batches and feeding them to a Lance writer.
class LanceSink final : public SinkToStorage
{
public:
    LanceSink(SharedHeader header_, String uri_)
        : SinkToStorage(header_), header(*header_), uri(std::move(uri_))
    {
        /// First write creates the dataset; subsequent writes append to it.
        int32_t mode = LANCE_WRITE_CREATE;
        if (LanceDataset * existing = lance_dataset_open(uri.c_str()))
        {
            lance_dataset_free(existing);
            mode = LANCE_WRITE_APPEND;
        }

        writer = lance_writer_open(uri.c_str(), mode);
        if (!writer)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot open Lance writer for '{}': {}", uri, lance_last_error());

        CHColumnToArrowColumn::Settings settings;
        settings.output_string_as_string = true;
        converter = std::make_unique<CHColumnToArrowColumn>(header, "Lance", settings);
    }

    ~LanceSink() override
    {
        if (writer)
            lance_writer_free(writer);
    }

    String getName() const override { return "LanceSink"; }

    void consume(Chunk & chunk) override
    {
        if (chunk.getNumRows() == 0)
            return;

        std::vector<Chunk> chunks;
        chunks.emplace_back(std::move(chunk));

        std::shared_ptr<arrow::Table> table;
        converter->chChunkToArrowTable(table, chunks, header.columns());

        auto batch_result = table->CombineChunksToBatch();
        if (!batch_result.ok())
            throw Exception(
                ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Failed to combine Arrow chunks: {}", batch_result.status().ToString());

        ArrowArray c_array = {};
        ArrowSchema c_schema = {};
        const auto status = arrow::ExportRecordBatch(**batch_result, &c_array, &c_schema);
        if (!status.ok())
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Failed to export Arrow record batch: {}", status.ToString());

        /// lance_writer_write takes ownership of both c_array and c_schema.
        if (lance_writer_write(writer, &c_array, &c_schema) < 0)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Lance write failed: {}", lance_last_error());
    }

    void onFinish() override
    {
        if (writer && lance_writer_finish(writer) < 0)
            throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Lance writer finish failed: {}", lance_last_error());
    }

private:
    Block header;
    String uri;
    std::unique_ptr<CHColumnToArrowColumn> converter;
    LanceWriter * writer = nullptr;
};

}

ColumnsDescription StorageLance::getTableStructureFromData(const String & uri, const ContextPtr & context)
{
    LanceDataset * dataset = lance_dataset_open(uri.c_str());
    if (!dataset)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot open Lance dataset '{}': {}", uri, lance_last_error());

    ArrowSchema c_schema = {};
    const int32_t rc = lance_dataset_schema(dataset, &c_schema);
    lance_dataset_free(dataset);
    if (rc < 0)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot read Lance schema for '{}': {}", uri, lance_last_error());

    auto schema_result = arrow::ImportSchema(&c_schema);
    if (!schema_result.ok())
        throw Exception(
            ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot import Lance Arrow schema for '{}': {}", uri, schema_result.status().ToString());

    const auto format_settings = getFormatSettings(context);
    Block header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(**schema_result, nullptr, "Lance", format_settings);
    return ColumnsDescription(header.getNamesAndTypesList());
}

StorageLance::StorageLance(const StorageID & table_id_, const ColumnsDescription & columns_, String uri_)
    : IStorage(table_id_), uri(std::move(uri_))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

Pipe StorageLance::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & /*query_info*/,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block header = storage_snapshot->getSampleBlockForColumns(column_names);

    /// v1: a single ordered stream. Multi-fragment parallelism is a later milestone (M3).
    return Pipe(std::make_shared<LanceSource>(uri, column_names, header, context, max_block_size));
}

SinkToStoragePtr StorageLance::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr /*context*/,
    bool /*async_insert*/)
{
    return std::make_shared<LanceSink>(std::make_shared<const Block>(metadata_snapshot->getSampleBlock()), uri);
}

void registerStorageLance(StorageFactory & factory)
{
    factory.registerStorage(
        "Lance",
        [](const StorageFactory::Arguments & args)
        {
            ASTs & engine_args = args.engine_args;
            if (engine_args.size() != 1)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Storage Lance requires exactly one argument: the dataset path or URI.");

            engine_args[0] = evaluateConstantExpressionAsLiteral(engine_args[0], args.getLocalContext());
            const String uri = checkAndGetLiteralArgument<String>(engine_args[0], "uri");

            ColumnsDescription columns = args.columns;
            if (columns.empty())
                columns = StorageLance::getTableStructureFromData(uri, args.getLocalContext());

            return std::make_shared<StorageLance>(args.table_id, columns, uri);
        },
        {
            .supports_schema_inference = true,
        });
}

}

#endif
