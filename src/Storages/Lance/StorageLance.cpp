#include <Storages/Lance/StorageLance.h>

#if USE_LANCE

#include <Core/Block.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Processors/Formats/Impl/ArrowColumnToCHColumn.h>
#include <Processors/Formats/Impl/CHColumnToArrowColumn.h>
#include <Processors/ISource.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/NamedCollectionsHelpers.h>
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

namespace Setting
{
    extern const SettingsBool allow_experimental_lance;
    extern const SettingsUInt64 lance_version;
}

namespace ErrorCodes
{
    extern const int EXTERNAL_LIBRARY_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

/// Open a Lance dataset through the FFI, forwarding object-store storage options and an optional
/// version (`version == 0` => latest). The borrowed option strings only need to outlive this call.
LanceDataset * openLanceDataset(const String & uri, const LanceStorageOptions & storage_options, UInt64 version)
{
    std::vector<const char *> keys;
    std::vector<const char *> values;
    keys.reserve(storage_options.size());
    values.reserve(storage_options.size());
    for (const auto & [key, value] : storage_options)
    {
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }
    return lance_dataset_open_with_options(uri.c_str(), version, keys.data(), values.data(), keys.size());
}

bool lanceDatasetExists(const String & uri, const LanceStorageOptions & storage_options)
{
    std::vector<const char *> keys;
    std::vector<const char *> values;
    keys.reserve(storage_options.size());
    values.reserve(storage_options.size());
    for (const auto & [key, value] : storage_options)
    {
        keys.push_back(key.c_str());
        values.push_back(value.c_str());
    }

    const int32_t result = lance_dataset_exists_with_options(uri.c_str(), keys.data(), values.data(), keys.size());
    if (result < 0)
        throw Exception(ErrorCodes::EXTERNAL_LIBRARY_ERROR, "Cannot check Lance dataset '{}': {}", uri, lance_last_error());
    return result == 1;
}

/// A single-stream source pulling Arrow record batches from a Lance scan via FFI.
class LanceSource final : public ISource
{
public:
    LanceSource(
        String uri_,
        LanceStorageOptions storage_options_,
        UInt64 version_,
        Names column_names_,
        Block header_,
        const ContextPtr & context_,
        size_t batch_size_)
        : ISource(std::make_shared<const Block>(header_))
        , uri(std::move(uri_))
        , storage_options(std::move(storage_options_))
        , version(version_)
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
        dataset = openLanceDataset(uri, storage_options, version);
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
    LanceStorageOptions storage_options;
    UInt64 version;
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
    LanceSink(SharedHeader header_, String uri_, LanceStorageOptions storage_options_)
        : SinkToStorage(header_), header(*header_), uri(std::move(uri_)), storage_options(std::move(storage_options_))
    {
        /// First write creates the dataset; subsequent writes append to it.
        const int32_t mode = lanceDatasetExists(uri, storage_options) ? LANCE_WRITE_APPEND : LANCE_WRITE_CREATE;

        std::vector<const char *> keys;
        std::vector<const char *> values;
        keys.reserve(storage_options.size());
        values.reserve(storage_options.size());
        for (const auto & [key, value] : storage_options)
        {
            keys.push_back(key.c_str());
            values.push_back(value.c_str());
        }

        writer = lance_writer_open_with_options(uri.c_str(), mode, keys.data(), values.data(), keys.size());
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
    LanceStorageOptions storage_options;
    std::unique_ptr<CHColumnToArrowColumn> converter;
    LanceWriter * writer = nullptr;
};

}

void parseLanceArguments(ASTs & args, const ContextPtr & context, String & uri, LanceStorageOptions & storage_options)
{
    /// Named collection: its `url` key is the dataset URI; every other key is forwarded verbatim as
    /// an object-store storage option (region, endpoint, access_key_id, secret_access_key, ...).
    if (auto collection = tryGetNamedCollectionWithOverrides(args, context, /* throw_unknown_collection */ false))
    {
        if (!collection->has("url"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Named collection for Lance must define the 'url' key.");
        uri = collection->get<String>("url");
        for (const auto & key : collection->getKeys())
        {
            if (key != "url")
                storage_options[key] = collection->get<String>(key);
        }
        return;
    }

    if (args.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Lance requires at least one argument: the dataset path or URI.");

    if (args.size() == 2 || args.size() > 4)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Lance arguments must be: uri [, access_key_id, secret_access_key [, session_token]].");

    for (auto & arg : args)
        arg = evaluateConstantExpressionAsLiteral(arg, context);

    uri = checkAndGetLiteralArgument<String>(args[0], "uri");
    if (args.size() >= 3)
    {
        storage_options["access_key_id"] = checkAndGetLiteralArgument<String>(args[1], "access_key_id");
        storage_options["secret_access_key"] = checkAndGetLiteralArgument<String>(args[2], "secret_access_key");
    }
    if (args.size() == 4)
        storage_options["session_token"] = checkAndGetLiteralArgument<String>(args[3], "session_token");
}

ColumnsDescription StorageLance::getTableStructureFromData(
    const String & uri, const LanceStorageOptions & storage_options, UInt64 version, const ContextPtr & context)
{
    LanceDataset * dataset = openLanceDataset(uri, storage_options, version);
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

StorageLance::StorageLance(
    const StorageID & table_id_, const ColumnsDescription & columns_, String uri_, LanceStorageOptions storage_options_)
    : IStorage(table_id_), uri(std::move(uri_)), storage_options(std::move(storage_options_))
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

    /// `lance_version == 0` reads the latest version; a non-zero value selects a specific one (time travel).
    const UInt64 version = context->getSettingsRef()[Setting::lance_version];

    /// v1: a single ordered stream. Multi-fragment parallelism is a later milestone.
    return Pipe(std::make_shared<LanceSource>(uri, storage_options, version, column_names, header, context, max_block_size));
}

SinkToStoragePtr StorageLance::write(
    const ASTPtr & /*query*/,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    bool /*async_insert*/)
{
    const UInt64 version = context->getSettingsRef()[Setting::lance_version];
    if (version != 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Writing to a historical Lance dataset version is not allowed");
    return std::make_shared<LanceSink>(std::make_shared<const Block>(metadata_snapshot->getSampleBlock()), uri, storage_options);
}

void registerStorageLance(StorageFactory & factory)
{
    factory.registerStorage(
        "Lance",
        [](const StorageFactory::Arguments & args)
        {
            if (args.mode <= LoadingStrictnessLevel::CREATE
                && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_lance])
                throw Exception(
                    ErrorCodes::SUPPORT_IS_DISABLED,
                    "Set `allow_experimental_lance` setting to enable the `Lance` table engine");

            String uri;
            LanceStorageOptions storage_options;
            parseLanceArguments(args.engine_args, args.getLocalContext(), uri, storage_options);

            ColumnsDescription columns = args.columns;
            if (columns.empty())
            {
                const UInt64 version = args.getLocalContext()->getSettingsRef()[Setting::lance_version];
                columns = StorageLance::getTableStructureFromData(uri, storage_options, version, args.getLocalContext());
            }

            return std::make_shared<StorageLance>(args.table_id, columns, uri, storage_options);
        },
        {
            .supports_schema_inference = true,
        });
}

}

#endif
