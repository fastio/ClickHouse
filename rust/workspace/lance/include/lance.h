#pragma once

// C ABI for the Lance columnar format, backed by the Rust `lance` core crate.
//
// Design (mirrors the delta-kernel-rs integration philosophy):
//   * Opaque handles are owned by Rust; the C++ side frees them via the matching
//     `lance_*_free` call (wrap them in a RAII holder, cf. KernelPointerWrapper).
//   * Every function is SYNCHRONOUS. The async lance/tokio machinery is hidden behind a
//     process-wide tokio runtime + block_on inside Rust, so the caller never sees a runtime.
//   * Bulk data crosses the boundary via the Arrow C Data Interface (ArrowArray/ArrowSchema),
//     zero-copy, to be consumed by arrow::ImportRecordBatch + ArrowColumnToCHColumn.
//   * Errors are reported out-of-band: a function returns NULL / a negative code, and the
//     caller retrieves the message with lance_last_error() (thread-local, like diskann).

#include <cstdint>

// ---------------------------------------------------------------------------
// Arrow C Data Interface (stable ABI). Guarded so it does not clash with
// arrow/c/abi.h when this header is included alongside the Arrow C++ headers.
// ---------------------------------------------------------------------------
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema
{
    const char * format;
    const char * name;
    const char * metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema ** children;
    struct ArrowSchema * dictionary;
    void (*release)(struct ArrowSchema *);
    void * private_data;
};

struct ArrowArray
{
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void ** buffers;
    struct ArrowArray ** children;
    struct ArrowArray * dictionary;
    void (*release)(struct ArrowArray *);
    void * private_data;
};

#endif // ARROW_C_DATA_INTERFACE

extern "C"
{

// Opaque handles (defined in Rust; only ever used through pointers).
struct LanceDataset;
struct LanceScanner;
struct LanceRecordBatchStream;
struct LanceWriter;

// ---- Error reporting ------------------------------------------------------
// Returns the last error on the current thread, or NULL if none. The returned
// pointer is owned by Rust and valid until the next failing call on this thread.
const char * lance_last_error(void);

// ---- Dataset lifecycle & metadata -----------------------------------------
// Open the latest version of a dataset at `uri`. Returns NULL on error.
LanceDataset * lance_dataset_open(const char * uri);
// Open a specific version (time travel). Returns NULL on error.
LanceDataset * lance_dataset_open_version(const char * uri, uint64_t version);
// Open a dataset with object-store storage options (S3 credentials/endpoint/etc.) and an optional
// version. `version == 0` opens the latest; `version > 0` checks out that version. `keys`/`vals`
// are `n` parallel storage-option entries (object_store config keys, e.g. access_key_id,
// secret_access_key, region, endpoint, allow_http); pass n == 0 for none. Returns NULL on error.
LanceDataset * lance_dataset_open_with_options(
    const char * uri, uint64_t version, const char * const * keys, const char * const * vals, uint64_t n);
// Return 1 if the dataset exists, 0 if it does not exist, and -1 for any other error.
// Unlike attempting to open the dataset and treating NULL as absence, this preserves permission,
// connectivity, and corruption errors so callers cannot accidentally enter a create path.
int32_t lance_dataset_exists_with_options(
    const char * uri, const char * const * keys, const char * const * vals, uint64_t n);
void lance_dataset_free(LanceDataset * dataset);

// Export the dataset schema through the Arrow C Data Interface into `out_schema`
// (caller-allocated, ownership transferred to caller -> call out_schema.release).
// Returns 0 on success, < 0 on error.
int32_t lance_dataset_schema(const LanceDataset * dataset, struct ArrowSchema * out_schema);

uint64_t lance_dataset_version(const LanceDataset * dataset);

// Count rows, optionally filtered by an SQL predicate (`filter` may be NULL).
// Returns 0 on success and writes to *out_count; < 0 on error.
int32_t lance_dataset_count_rows(const LanceDataset * dataset, const char * filter, uint64_t * out_count);

// List all versions. On success writes a malloc'd array to *out_versions (free with
// lance_free_u64_array) and its length to *out_n. Returns 0 on success, < 0 on error.
int32_t lance_dataset_list_versions(const LanceDataset * dataset, uint64_t ** out_versions, uint64_t * out_n);
void lance_free_u64_array(uint64_t * ptr, uint64_t n);

// ---- Read (scan) ----------------------------------------------------------
LanceScanner * lance_scanner_create(const LanceDataset * dataset);
// Column projection. `columns` is an array of `n` NUL-terminated names.
int32_t lance_scanner_project(LanceScanner * scanner, const char * const * columns, uint64_t n);
// SQL filter pushed down to the scanner.
int32_t lance_scanner_filter(LanceScanner * scanner, const char * filter);
// limit < 0 means "no limit"; offset < 0 means 0.
int32_t lance_scanner_limit(LanceScanner * scanner, int64_t limit, int64_t offset);
int32_t lance_scanner_batch_size(LanceScanner * scanner, uint64_t batch_size);
// Materialize the scan into a batch stream. Returns NULL on error.
LanceRecordBatchStream * lance_scanner_open_stream(LanceScanner * scanner);
void lance_scanner_free(LanceScanner * scanner);

// Pull the next record batch. On a produced batch writes it to out_array/out_schema
// (Arrow C Data Interface, ownership to caller) and returns 0. Returns 1 at end of
// stream (out params untouched), < 0 on error.
int32_t lance_stream_next(LanceRecordBatchStream * stream, struct ArrowArray * out_array, struct ArrowSchema * out_schema);
void lance_stream_free(LanceRecordBatchStream * stream);

// ---- Write ----------------------------------------------------------------
// Write mode for lance_writer_open.
enum LanceWriteMode
{
    LANCE_WRITE_CREATE = 0,
    LANCE_WRITE_APPEND = 1,
    LANCE_WRITE_OVERWRITE = 2,
};

// Open a writer bound to `uri`. The dataset schema is taken from the first batch written.
// Returns NULL on error.
LanceWriter * lance_writer_open(const char * uri, int32_t mode);
// Same as lance_writer_open, but with object-store storage options (S3 credentials/endpoint/etc.).
// `keys`/`vals` are `n` parallel storage-option entries (pass n == 0 for none). Returns NULL on error.
LanceWriter * lance_writer_open_with_options(
    const char * uri, int32_t mode, const char * const * keys, const char * const * vals, uint64_t n);
// Feed one record batch (Arrow C Data Interface). Consumes (releases) BOTH the input array and
// schema — the caller must not release them afterwards. Returns 0 on success, < 0 on error.
int32_t lance_writer_write(LanceWriter * writer, struct ArrowArray * array, struct ArrowSchema * schema);
// Flush all buffered batches into the dataset. Returns 0 on success, < 0 on error.
int32_t lance_writer_finish(LanceWriter * writer);
void lance_writer_free(LanceWriter * writer);

} // extern "C"
