//! C FFI bridge exposing the Lance columnar format to ClickHouse.
//!
//! Mirrors the delta-kernel-rs integration philosophy:
//!   * opaque handles owned by Rust, freed via explicit `*_free` calls (RAII on the C++ side);
//!   * every FFI function is synchronous — the async lance/tokio machinery is hidden behind a
//!     process-wide tokio runtime and `block_on`, so the C++ caller never sees a runtime;
//!   * bulk data crosses the boundary through the Arrow C Data Interface (zero-copy), consumed
//!     on the C++ side by `arrow::ImportRecordBatch` + `ArrowColumnToCHColumn`;
//!   * errors are reported out-of-band via a thread-local last-error string (`lance_last_error`),
//!     matching the existing `diskann-clickhouse` first-party crate convention.
//!
//! The C ABI in `include/lance.h` is the stable contract. `*mut FFI_ArrowArray` /
//! `*mut FFI_ArrowSchema` here are ABI-identical to `struct ArrowArray *` / `struct ArrowSchema *`
//! in that header (the Arrow C Data Interface).
//!
use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::ptr;
use std::sync::{mpsc, Arc, OnceLock};
use std::thread;

use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow_schema::{ArrowError, Schema as ArrowSchema, SchemaRef};
use futures::{Stream, StreamExt, TryStreamExt};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::scanner::Scanner;
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::io::{ObjectStoreParams, StorageOptionsAccessor};

// ---- runtime --------------------------------------------------------------

fn runtime() -> &'static tokio::runtime::Runtime
{
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio runtime for lance ffi")
    })
}

// ---- error reporting ------------------------------------------------------

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_error(msg: impl Into<String>)
{
    let c = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("lance ffi error (message contained NUL)").unwrap());
    LAST_ERROR.with(|e| *e.borrow_mut() = Some(c));
}

#[no_mangle]
pub extern "C" fn lance_last_error() -> *const c_char
{
    LAST_ERROR.with(|e| match &*e.borrow()
    {
        Some(c) => c.as_ptr(),
        None => ptr::null(),
    })
}

/// Run a body that may panic across the FFI boundary, converting a panic into `default`.
fn guard<T>(default: T, body: impl FnOnce() -> T) -> T
{
    match catch_unwind(AssertUnwindSafe(body))
    {
        Ok(v) => v,
        Err(_) =>
        {
            set_error("lance ffi: caught panic");
            default
        }
    }
}

unsafe fn opt_str<'a>(p: *const c_char) -> Option<&'a str>
{
    if p.is_null()
    {
        None
    }
    else
    {
        CStr::from_ptr(p).to_str().ok()
    }
}

/// Build an object-store storage-options map from two parallel C string arrays (`keys`/`vals`,
/// each of length `n`). These map directly onto object_store's backend config keys (e.g.
/// `access_key_id`, `secret_access_key`, `region`, `endpoint`, `allow_http`). Returns `None` if
/// any entry is null or not UTF-8; an empty map (`n == 0`) is valid and means "no options".
unsafe fn build_options(
    keys: *const *const c_char,
    vals: *const *const c_char,
    n: u64,
) -> Option<HashMap<String, String>>
{
    let mut map = HashMap::with_capacity(n as usize);
    for i in 0..n as isize
    {
        let k = opt_str(*keys.offset(i))?;
        let v = opt_str(*vals.offset(i))?;
        map.insert(k.to_string(), v.to_string());
    }
    Some(map)
}

/// Build `ObjectStoreParams` carrying the given storage options, or `None` when empty (so the
/// default credential chain / local filesystem path is used unchanged).
fn store_params_from_options(options: HashMap<String, String>) -> Option<ObjectStoreParams>
{
    if options.is_empty()
    {
        return None;
    }
    let mut params = ObjectStoreParams::default();
    params.storage_options_accessor = Some(Arc::new(StorageOptionsAccessor::with_static_options(options)));
    Some(params)
}

// ---- handles --------------------------------------------------------------

pub struct LanceDataset
{
    inner: Dataset,
}

pub struct LanceScanner
{
    inner: Scanner,
}

type BatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, String>> + Send>>;

pub struct LanceRecordBatchStream
{
    inner: BatchStream,
}

pub struct LanceWriter
{
    uri: String,
    mode: WriteMode,
    storage_options: HashMap<String, String>,
    sender: Option<mpsc::SyncSender<Result<RecordBatch, ArrowError>>>,
    worker: Option<thread::JoinHandle<Result<(), String>>>,
}

struct ChannelRecordBatchReader
{
    schema: SchemaRef,
    receiver: mpsc::Receiver<Result<RecordBatch, ArrowError>>,
}

impl Iterator for ChannelRecordBatchReader
{
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item>
    {
        self.receiver.recv().ok()
    }
}

impl RecordBatchReader for ChannelRecordBatchReader
{
    fn schema(&self) -> SchemaRef
    {
        self.schema.clone()
    }
}

// ---- dataset lifecycle & metadata -----------------------------------------

#[no_mangle]
pub extern "C" fn lance_dataset_open(uri: *const c_char) -> *mut LanceDataset
{
    guard(ptr::null_mut(), || {
        let Some(uri) = (unsafe { opt_str(uri) }) else {
            set_error("lance_dataset_open: uri is null or not UTF-8");
            return ptr::null_mut();
        };
        match runtime().block_on(Dataset::open(uri))
        {
            Ok(inner) => Box::into_raw(Box::new(LanceDataset { inner })),
            Err(e) =>
            {
                set_error(format!("lance_dataset_open({uri}): {e}"));
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_open_version(uri: *const c_char, version: u64) -> *mut LanceDataset
{
    guard(ptr::null_mut(), || {
        let Some(uri) = (unsafe { opt_str(uri) }) else {
            set_error("lance_dataset_open_version: uri is null or not UTF-8");
            return ptr::null_mut();
        };
        let opened = runtime().block_on(async {
            let ds = Dataset::open(uri).await?;
            ds.checkout_version(version).await
        });
        match opened
        {
            Ok(inner) => Box::into_raw(Box::new(LanceDataset { inner })),
            Err(e) =>
            {
                set_error(format!("lance_dataset_open_version({uri}, {version}): {e}"));
                ptr::null_mut()
            }
        }
    })
}

/// Open a dataset with object-store storage options (S3 credentials/endpoint/etc.) and an optional
/// version. `version == 0` opens the latest version; `version > 0` checks out that version (time
/// travel). `keys`/`vals` are `n` parallel storage-option entries (may be empty for local paths or
/// the default credential chain). Returns NULL on error.
#[no_mangle]
pub extern "C" fn lance_dataset_open_with_options(
    uri: *const c_char,
    version: u64,
    keys: *const *const c_char,
    vals: *const *const c_char,
    n: u64,
) -> *mut LanceDataset
{
    guard(ptr::null_mut(), || {
        let Some(uri) = (unsafe { opt_str(uri) }) else {
            set_error("lance_dataset_open_with_options: uri is null or not UTF-8");
            return ptr::null_mut();
        };
        let Some(options) = (unsafe { build_options(keys, vals, n) }) else {
            set_error("lance_dataset_open_with_options: storage option key/value is null or not UTF-8");
            return ptr::null_mut();
        };
        let mut builder = DatasetBuilder::from_uri(uri);
        if !options.is_empty()
        {
            builder = builder.with_storage_options(options);
        }
        if version > 0
        {
            builder = builder.with_version(version);
        }
        match runtime().block_on(builder.load())
        {
            Ok(inner) => Box::into_raw(Box::new(LanceDataset { inner })),
            Err(e) =>
            {
                set_error(format!("lance_dataset_open_with_options({uri}, version={version}): {e}"));
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_exists_with_options(
    uri: *const c_char,
    keys: *const *const c_char,
    vals: *const *const c_char,
    n: u64,
) -> i32
{
    guard(-1, || {
        let Some(uri) = (unsafe { opt_str(uri) }) else {
            set_error("lance_dataset_exists_with_options: uri is null or not UTF-8");
            return -1;
        };
        let Some(options) = (unsafe { build_options(keys, vals, n) }) else {
            set_error("lance_dataset_exists_with_options: storage option key/value is null or not UTF-8");
            return -1;
        };
        let mut builder = DatasetBuilder::from_uri(uri);
        if !options.is_empty()
        {
            builder = builder.with_storage_options(options);
        }
        match runtime().block_on(builder.load())
        {
            Ok(_) => 1,
            Err(lance::Error::DatasetNotFound { .. } | lance::Error::NotFound { .. }) => 0,
            Err(e) =>
            {
                set_error(format!("lance_dataset_exists_with_options({uri}): {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_free(dataset: *mut LanceDataset)
{
    if !dataset.is_null()
    {
        drop(unsafe { Box::from_raw(dataset) });
    }
}

#[no_mangle]
pub extern "C" fn lance_dataset_schema(dataset: *const LanceDataset, out_schema: *mut FFI_ArrowSchema) -> i32
{
    guard(-1, || {
        let ds = unsafe { &*dataset };
        let arrow_schema: ArrowSchema = ds.inner.schema().into();
        match FFI_ArrowSchema::try_from(&arrow_schema)
        {
            Ok(ffi) =>
            {
                unsafe { ptr::write(out_schema, ffi) };
                0
            }
            Err(e) =>
            {
                set_error(format!("lance_dataset_schema: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_version(dataset: *const LanceDataset) -> u64
{
    guard(0, || {
        let ds = unsafe { &*dataset };
        ds.inner.version().version
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_count_rows(
    dataset: *const LanceDataset,
    filter: *const c_char,
    out_count: *mut u64,
) -> i32
{
    guard(-1, || {
        let ds = unsafe { &*dataset };
        let filter = unsafe { opt_str(filter) }.map(|s| s.to_string());
        match runtime().block_on(ds.inner.count_rows(filter))
        {
            Ok(n) =>
            {
                unsafe { *out_count = n as u64 };
                0
            }
            Err(e) =>
            {
                set_error(format!("lance_dataset_count_rows: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_dataset_list_versions(
    dataset: *const LanceDataset,
    out_versions: *mut *mut u64,
    out_n: *mut u64,
) -> i32
{
    guard(-1, || {
        let ds = unsafe { &*dataset };
        match runtime().block_on(ds.inner.versions())
        {
            Ok(versions) =>
            {
                let boxed: Box<[u64]> = versions.iter().map(|v| v.version).collect();
                let n = boxed.len();
                let p = Box::into_raw(boxed) as *mut u64;
                unsafe {
                    *out_versions = p;
                    *out_n = n as u64;
                }
                0
            }
            Err(e) =>
            {
                set_error(format!("lance_dataset_list_versions: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_free_u64_array(ptr_: *mut u64, n: u64)
{
    if !ptr_.is_null()
    {
        drop(unsafe { Vec::from_raw_parts(ptr_, n as usize, n as usize) });
    }
}

// ---- read (scan) ----------------------------------------------------------

#[no_mangle]
pub extern "C" fn lance_scanner_create(dataset: *const LanceDataset) -> *mut LanceScanner
{
    guard(ptr::null_mut(), || {
        let ds = unsafe { &*dataset };
        Box::into_raw(Box::new(LanceScanner { inner: ds.inner.scan() }))
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_project(
    scanner: *mut LanceScanner,
    columns: *const *const c_char,
    n: u64,
) -> i32
{
    guard(-1, || {
        let sc = unsafe { &mut *scanner };
        let mut cols: Vec<&str> = Vec::with_capacity(n as usize);
        for i in 0..n as isize
        {
            let cp = unsafe { *columns.offset(i) };
            match unsafe { opt_str(cp) }
            {
                Some(s) => cols.push(s),
                None =>
                {
                    set_error("lance_scanner_project: column name is null or not UTF-8");
                    return -1;
                }
            }
        }
        match sc.inner.project(&cols)
        {
            Ok(_) => 0,
            Err(e) =>
            {
                set_error(format!("lance_scanner_project: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_filter(scanner: *mut LanceScanner, filter: *const c_char) -> i32
{
    guard(-1, || {
        let sc = unsafe { &mut *scanner };
        let Some(filter) = (unsafe { opt_str(filter) }) else {
            set_error("lance_scanner_filter: filter is null or not UTF-8");
            return -1;
        };
        match sc.inner.filter(filter)
        {
            Ok(_) => 0,
            Err(e) =>
            {
                set_error(format!("lance_scanner_filter: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_limit(scanner: *mut LanceScanner, limit: i64, offset: i64) -> i32
{
    guard(-1, || {
        let sc = unsafe { &mut *scanner };
        let limit = if limit < 0 { None } else { Some(limit) };
        let offset = if offset <= 0 { None } else { Some(offset) };
        match sc.inner.limit(limit, offset)
        {
            Ok(_) => 0,
            Err(e) =>
            {
                set_error(format!("lance_scanner_limit: {e}"));
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_batch_size(scanner: *mut LanceScanner, batch_size: u64) -> i32
{
    guard(-1, || {
        let sc = unsafe { &mut *scanner };
        sc.inner.batch_size(batch_size as usize);
        0
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_open_stream(scanner: *mut LanceScanner) -> *mut LanceRecordBatchStream
{
    guard(ptr::null_mut(), || {
        let sc = unsafe { &*scanner };
        match runtime().block_on(sc.inner.try_into_stream())
        {
            Ok(stream) =>
            {
                let mapped: BatchStream = Box::pin(stream.map_err(|e| e.to_string()));
                Box::into_raw(Box::new(LanceRecordBatchStream { inner: mapped }))
            }
            Err(e) =>
            {
                set_error(format!("lance_scanner_open_stream: {e}"));
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_scanner_free(scanner: *mut LanceScanner)
{
    if !scanner.is_null()
    {
        drop(unsafe { Box::from_raw(scanner) });
    }
}

#[no_mangle]
pub extern "C" fn lance_stream_next(
    stream: *mut LanceRecordBatchStream,
    out_array: *mut FFI_ArrowArray,
    out_schema: *mut FFI_ArrowSchema,
) -> i32
{
    guard(-1, || {
        let st = unsafe { &mut *stream };
        match runtime().block_on(st.inner.next())
        {
            Some(Ok(batch)) =>
            {
                let struct_array = StructArray::from(batch);
                match to_ffi(&struct_array.into_data())
                {
                    Ok((ffi_array, ffi_schema)) =>
                    {
                        unsafe {
                            ptr::write(out_array, ffi_array);
                            ptr::write(out_schema, ffi_schema);
                        }
                        0
                    }
                    Err(e) =>
                    {
                        set_error(format!("lance_stream_next: arrow export: {e}"));
                        -1
                    }
                }
            }
            Some(Err(e)) =>
            {
                set_error(format!("lance_stream_next: {e}"));
                -1
            }
            None => 1,
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_stream_free(stream: *mut LanceRecordBatchStream)
{
    if !stream.is_null()
    {
        drop(unsafe { Box::from_raw(stream) });
    }
}

// ---- write ----------------------------------------------------------------

fn write_mode_from_i32(mode: i32) -> Option<WriteMode>
{
    match mode
    {
        0 => Some(WriteMode::Create),
        1 => Some(WriteMode::Append),
        2 => Some(WriteMode::Overwrite),
        _ => None,
    }
}

#[no_mangle]
pub extern "C" fn lance_writer_open(uri: *const c_char, mode: i32) -> *mut LanceWriter
{
    lance_writer_open_with_options(uri, mode, ptr::null(), ptr::null(), 0)
}

/// Open a writer with object-store storage options (S3 credentials/endpoint/etc.). `keys`/`vals`
/// are `n` parallel storage-option entries (may be empty for local paths or the default credential
/// chain). The dataset schema is taken from the first batch written. Returns NULL on error.
#[no_mangle]
pub extern "C" fn lance_writer_open_with_options(
    uri: *const c_char,
    mode: i32,
    keys: *const *const c_char,
    vals: *const *const c_char,
    n: u64,
) -> *mut LanceWriter
{
    guard(ptr::null_mut(), || {
        let Some(uri) = (unsafe { opt_str(uri) }) else {
            set_error("lance_writer_open: uri is null or not UTF-8");
            return ptr::null_mut();
        };
        let Some(mode) = write_mode_from_i32(mode) else {
            set_error(format!("lance_writer_open: invalid write mode {mode}"));
            return ptr::null_mut();
        };
        let Some(storage_options) = (unsafe { build_options(keys, vals, n) }) else {
            set_error("lance_writer_open: storage option key/value is null or not UTF-8");
            return ptr::null_mut();
        };
        Box::into_raw(Box::new(LanceWriter {
            uri: uri.to_string(),
            mode,
            storage_options,
            sender: None,
            worker: None,
        }))
    })
}

#[no_mangle]
pub extern "C" fn lance_writer_write(
    writer: *mut LanceWriter,
    array: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
) -> i32
{
    guard(-1, || {
        let w = unsafe { &mut *writer };
        // Take ownership of both the array and the schema; both are released when dropped.
        let ffi_array = unsafe { ptr::read(array) };
        let ffi_schema = unsafe { ptr::read(schema) };
        let data = match unsafe { from_ffi(ffi_array, &ffi_schema) }
        {
            Ok(d) => d,
            Err(e) =>
            {
                set_error(format!("lance_writer_write: import batch: {e}"));
                return -1;
            }
        };
        let batch = RecordBatch::from(StructArray::from(data));
        if w.sender.is_none()
        {
            let (sender, receiver) = mpsc::sync_channel(2);
            let reader = ChannelRecordBatchReader { schema: batch.schema(), receiver };
            let uri = w.uri.clone();
            let mode = w.mode;
            let store_params = store_params_from_options(w.storage_options.clone());
            w.worker = Some(thread::spawn(move || {
                let params = WriteParams { mode, store_params, ..Default::default() };
                runtime()
                    .block_on(Dataset::write(reader, &uri, Some(params)))
                    .map(|_| ())
                    .map_err(|e| e.to_string())
            }));
            w.sender = Some(sender);
        }
        match w.sender.as_ref().unwrap().send(Ok(batch))
        {
            Ok(_) => 0,
            Err(_) =>
            {
                set_error("lance_writer_write: writer task terminated before accepting the batch");
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_writer_finish(writer: *mut LanceWriter) -> i32
{
    guard(-1, || {
        let w = unsafe { &mut *writer };
        drop(w.sender.take());
        let Some(worker) = w.worker.take() else
        {
            return 0;
        };
        match worker.join()
        {
            Ok(Ok(())) => 0,
            Ok(Err(e)) =>
            {
                set_error(format!("lance_writer_finish: {e}"));
                -1
            }
            Err(_) =>
            {
                set_error("lance_writer_finish: writer task panicked");
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn lance_writer_free(writer: *mut LanceWriter)
{
    if !writer.is_null()
    {
        let mut writer = unsafe { Box::from_raw(writer) };
        if let Some(sender) = writer.sender.take()
        {
            let _ = sender.send(Err(ArrowError::ComputeError("Lance writer was aborted".to_string())));
        }
        if let Some(worker) = writer.worker.take()
        {
            let _ = worker.join();
        }
    }
}
