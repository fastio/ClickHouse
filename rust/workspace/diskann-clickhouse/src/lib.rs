//! DiskANN FFI bridge for ClickHouse.
//!
//! This crate exposes a C ABI for building and searching real disk-backed
//! `DiskANN` indexes. The ABI is intentionally path-oriented so later
//! ClickHouse layers can build TB-scale indexes without passing all vectors
//! through the FFI as one in-memory blob.

use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::panic;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use diskann::graph::config::{Builder as GraphConfigBuilder, MaxDegree};
use diskann::utils::ONE;
use diskann_disk::build::builder::build::DiskIndexBuilder;
use diskann_disk::data_model::CachingStrategy;
use diskann_disk::disk_index_build_parameter::{MemoryBudget, NumPQChunks, DISK_SECTOR_LEN};
use diskann_disk::search::provider::disk_provider::DiskIndexSearcher;
use diskann_disk::search::provider::disk_vertex_provider_factory::DiskVertexProviderFactory;
use diskann_disk::search::traits::VertexProviderFactory;
use diskann_disk::storage::disk_index_reader::DiskIndexReader;
use diskann_disk::storage::DiskIndexWriter;
use diskann_disk::utils::AlignedFileReaderFactory;
use diskann_disk::{DiskIndexBuildParameters, QuantizationType};
use diskann_providers::model::configuration::IndexConfiguration;
use diskann_providers::model::graph::traits::GraphDataType;
use diskann_providers::storage::{
    get_compressed_pq_file, get_disk_index_file, get_pq_pivot_file, StorageReadProvider,
    StorageWriteProvider,
};
use diskann_providers::utils::load_metadata_from_file;
use diskann_vector::distance::{DistanceProvider, Metric};

const ERR_NULL_PTR: i64 = -2;
const ERR_INVALID_HANDLE: i64 = -3;
const ERR_DIM_MISMATCH: i64 = -4;
const ERR_PANIC: i64 = -5;

/// DiskANN's `DiskVertexProvider` stores vectors with dimensions rounded up to
/// this alignment boundary (`dims.next_multiple_of(DISKANN_VECTOR_ALIGNMENT)`).
/// Query vectors passed to `search` must be padded to match, otherwise the SIMD
/// distance kernel panics on length mismatch.
///
/// Source: diskann-disk/src/search/provider/disk_vertex_provider.rs (field
/// `memory_aligned_dimension` and its initializer `metadata.dims.next_multiple_of(8)`).
///
/// If DiskANN ever changes its alignment constant, update this value to match.
const DISKANN_VECTOR_ALIGNMENT: usize = 8;

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);

/// Per-handle build state is wrapped in `Arc<Mutex<_>>` so the global HashMap
/// lock is held only long enough to clone the Arc out; the actual build work
/// (or, for `diskann_builder_build`, an immutable snapshot of the state taken
/// under the per-handle lock) runs with no global lock held. This is what
/// allows concurrent MaterializedIndex builds to proceed in parallel — the
/// earlier "single big mutex" design forced all builders in the process to
/// serialise behind one another, which was visible in SIFT-1M end-to-end runs
/// as 4 threads of work pinned to a single in-flight build.
static BUILDERS: std::sync::LazyLock<Mutex<HashMap<i64, Arc<Mutex<DiskIndexBuildState>>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

/// Searcher state is shared by `Arc` directly with no per-handle mutex.
/// `DiskIndexSearcher::search` takes `&self`, the underlying graph is
/// read-only after build, and `scratch_pool: Arc<ObjectPool<_>>` is the
/// crate's first-class concurrent rental mechanism, so independent search
/// calls on the same handle and across handles are free to run in parallel.
static SEARCHERS: std::sync::LazyLock<Mutex<HashMap<i64, Arc<DiskIndexSearchState>>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));

thread_local! {
    static LAST_ERROR: RefCell<String> = RefCell::new(String::new());
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiskANNMetric {
    L2 = 0,
    Cosine = 1,
    InnerProduct = 2,
    CosineNormalized = 3,
}

impl DiskANNMetric {
    fn to_metric(self) -> Metric {
        match self {
            Self::L2 => Metric::L2,
            Self::Cosine => Metric::Cosine,
            Self::InnerProduct => Metric::InnerProduct,
            Self::CosineNormalized => Metric::CosineNormalized,
        }
    }
}

struct GraphDataF32Vector;

impl GraphDataType for GraphDataF32Vector {
    type VectorIdType = u32;
    type VectorDataType = f32;
    type AssociatedDataType = ();
}

struct GraphDataF32VectorPayload8;

impl GraphDataType for GraphDataF32VectorPayload8 {
    type VectorIdType = u32;
    type VectorDataType = f32;
    type AssociatedDataType = [u8; 8];
}

struct GraphDataF32VectorPayload12;

impl GraphDataType for GraphDataF32VectorPayload12 {
    type VectorIdType = u32;
    type VectorDataType = f32;
    type AssociatedDataType = [u8; 12];
}

#[derive(Default)]
struct LocalFileStorageProvider;

impl StorageReadProvider for LocalFileStorageProvider {
    type Reader = BufReader<File>;

    fn open_reader(&self, item_identifier: &str) -> std::io::Result<Self::Reader> {
        let file = File::open(item_identifier)?;
        Ok(BufReader::new(file))
    }

    fn get_length(&self, item_identifier: &str) -> std::io::Result<u64> {
        Ok(fs::metadata(item_identifier)?.len())
    }

    fn exists(&self, item_identifier: &str) -> bool {
        fs::metadata(item_identifier).is_ok()
    }
}

impl StorageWriteProvider for LocalFileStorageProvider {
    type Writer = BufWriter<File>;

    fn open_writer(&self, item_identifier: &str) -> std::io::Result<Self::Writer> {
        let file = OpenOptions::new().write(true).open(item_identifier)?;
        Ok(BufWriter::new(file))
    }

    fn create_for_write(&self, item_identifier: &str) -> std::io::Result<Self::Writer> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(item_identifier)?;
        Ok(BufWriter::new(file))
    }

    fn delete(&self, item_identifier: &str) -> std::io::Result<()> {
        fs::remove_file(item_identifier)
    }
}

static FILE_STORAGE: LocalFileStorageProvider = LocalFileStorageProvider;

type DiskSearcherNoPayload = DiskIndexSearcher<
    GraphDataF32Vector,
    DiskVertexProviderFactory<GraphDataF32Vector, AlignedFileReaderFactory>,
>;

type DiskSearcherPayload8 = DiskIndexSearcher<
    GraphDataF32VectorPayload8,
    DiskVertexProviderFactory<GraphDataF32VectorPayload8, AlignedFileReaderFactory>,
>;

type DiskSearcherPayload12 = DiskIndexSearcher<
    GraphDataF32VectorPayload12,
    DiskVertexProviderFactory<GraphDataF32VectorPayload12, AlignedFileReaderFactory>,
>;

enum DiskSearcher {
    NoPayload(DiskSearcherNoPayload),
    Payload8(DiskSearcherPayload8),
    Payload12(DiskSearcherPayload12),
}

#[derive(Debug, Clone)]
struct DiskIndexBuildState {
    dim: u32,
    metric: DiskANNMetric,
    pruned_degree: u32,
    max_degree: u32,
    l_build: u32,
    alpha: f32,
    num_threads: usize,
    pq_chunks: usize,
    build_ram_limit_gb: f64,
    data_path: Option<String>,
    index_prefix: Option<String>,
    associated_data_path: Option<String>,
    associated_data_record_size: u32,
}

impl DiskIndexBuildState {
    fn new(
        dim: u32,
        metric: DiskANNMetric,
        pruned_degree: u32,
        max_degree: u32,
        l_build: u32,
        alpha: f32,
        num_threads: u32,
        pq_chunks: u32,
        build_ram_limit_gb: f64,
    ) -> Result<Self, String> {
        if dim == 0 {
            return Err("dimension must be > 0".to_string());
        }
        if pruned_degree == 0 {
            return Err("pruned_degree must be > 0".to_string());
        }
        if max_degree == 0 {
            return Err("max_degree must be > 0".to_string());
        }
        if l_build == 0 {
            return Err("l_build must be > 0".to_string());
        }
        if num_threads == 0 {
            return Err("num_threads must be > 0".to_string());
        }
        if pq_chunks == 0 {
            return Err("pq_chunks must be > 0".to_string());
        }
        if !build_ram_limit_gb.is_finite() || build_ram_limit_gb <= 0.0 {
            return Err("build_ram_limit_gb must be > 0".to_string());
        }

        Ok(Self {
            dim,
            metric,
            pruned_degree,
            max_degree,
            l_build,
            alpha,
            num_threads: num_threads as usize,
            pq_chunks: pq_chunks as usize,
            build_ram_limit_gb,
            data_path: None,
            index_prefix: None,
            associated_data_path: None,
            associated_data_record_size: 0,
        })
    }

    fn set_data_path(&mut self, path: String) {
        self.data_path = Some(path);
    }

    fn set_index_prefix(&mut self, prefix: String) {
        self.index_prefix = Some(prefix);
    }

    fn set_associated_data_path(&mut self, path: String, record_size: u32) -> Result<(), String> {
        if record_size != 8 && record_size != 12 {
            return Err(format!(
                "associated_data_record_size must be 8 or 12, got {record_size}"
            ));
        }
        self.associated_data_path = Some(path);
        self.associated_data_record_size = record_size;
        Ok(())
    }

    fn build(&self) -> Result<(), String> {
        let data_path = self
            .data_path
            .as_ref()
            .ok_or_else(|| "data_path is not set".to_string())?;
        let index_prefix = self
            .index_prefix
            .as_ref()
            .ok_or_else(|| "index_prefix is not set".to_string())?;

        let metadata = load_metadata_from_file(&FILE_STORAGE, data_path)
            .map_err(|err| format!("failed to read dataset metadata: {err}"))?;

        if metadata.ndims() != self.dim as usize {
            return Err(format!(
                "dataset dimension {} does not match builder dimension {}",
                metadata.ndims(),
                self.dim
            ));
        }

        if metadata.npoints() == 0 {
            return Err("dataset must contain at least one vector".to_string());
        }

        ensure_parent_directory(index_prefix)?;

        let config = GraphConfigBuilder::new_with(
            self.pruned_degree as usize,
            MaxDegree::new(self.max_degree as usize),
            self.l_build as usize,
            self.metric.to_metric().into(),
            |builder| {
                builder.alpha(self.alpha);
                builder.saturate_after_prune(true);
            },
        )
        .build()
        .map_err(|err| err.to_string())?;

        let build_params = DiskIndexBuildParameters::new(
            MemoryBudget::try_from_gb(self.build_ram_limit_gb).map_err(|err| err.to_string())?,
            QuantizationType::FP,
            NumPQChunks::new_with(self.pq_chunks, self.dim as usize)
                .map_err(|err| err.to_string())?,
        );

        let index_config = IndexConfiguration::new(
            self.metric.to_metric(),
            self.dim as usize,
            metadata.npoints(),
            ONE,
            self.num_threads,
            config,
        )
        .with_pseudo_rng();

        match self.associated_data_record_size {
            0 => self.build_typed::<GraphDataF32Vector>(
                data_path,
                index_prefix,
                build_params,
                index_config,
                None,
            ),
            8 => self.build_typed::<GraphDataF32VectorPayload8>(
                data_path,
                index_prefix,
                build_params,
                index_config,
                Some(
                    self.associated_data_path
                        .clone()
                        .ok_or_else(|| "associated_data_path is not set".to_string())?,
                ),
            ),
            12 => self.build_typed::<GraphDataF32VectorPayload12>(
                data_path,
                index_prefix,
                build_params,
                index_config,
                Some(
                    self.associated_data_path
                        .clone()
                        .ok_or_else(|| "associated_data_path is not set".to_string())?,
                ),
            ),
            other => Err(format!("unsupported associated_data_record_size {other}")),
        }
    }

    fn build_typed<Data>(
        &self,
        data_path: &String,
        index_prefix: &String,
        build_params: DiskIndexBuildParameters,
        index_config: IndexConfiguration,
        associated_data_path: Option<String>,
    ) -> Result<(), String>
    where
        Data: GraphDataType<VectorDataType = f32, VectorIdType = u32>,
    {
        let writer = DiskIndexWriter::new(
            data_path.clone(),
            index_prefix.clone(),
            associated_data_path,
            DISK_SECTOR_LEN,
        )
        .map_err(|err| err.to_string())?;

        let mut builder = DiskIndexBuilder::<Data, LocalFileStorageProvider>::new(
            &FILE_STORAGE,
            build_params,
            index_config,
            writer,
        )
        .map_err(|err| err.to_string())?;

        builder.build().map_err(|err| err.to_string())?;
        Ok(())
    }
}

struct DiskIndexSearchState {
    dim: u32,
    num_points: u64,
    disk_index_file_size: u64,
    payload_record_size: u32,
    searcher: DiskSearcher,
}

impl DiskIndexSearchState {
    fn open(
        index_prefix: String,
        dim: u32,
        metric: DiskANNMetric,
        num_threads: u32,
        search_io_limit: u32,
        num_nodes_to_cache: u32,
    ) -> Result<Self, String> {
        if dim == 0 {
            return Err("dimension must be > 0".to_string());
        }
        if num_threads == 0 {
            return Err("num_threads must be > 0".to_string());
        }
        if search_io_limit == 0 {
            return Err("search_io_limit must be > 0".to_string());
        }

        if !index_files_exist(&index_prefix) {
            return Err(format!("index files are missing for prefix {index_prefix}"));
        }

        let vertex_provider_factory =
            DiskVertexProviderFactory::<GraphDataF32Vector, AlignedFileReaderFactory>::new(
                AlignedFileReaderFactory::new(get_disk_index_file(&index_prefix)),
                if num_nodes_to_cache > 0 {
                    CachingStrategy::StaticCacheWithBfsNodes(num_nodes_to_cache as usize)
                } else {
                    CachingStrategy::None
                },
            )
            .map_err(|err| err.to_string())?;

        let header = vertex_provider_factory
            .get_header()
            .map_err(|err| err.to_string())?;

        let num_points = header.metadata().num_pts;
        let disk_index_file_size = header.metadata().disk_index_file_size;

        if header.metadata().dims != dim as usize {
            return Err(format!(
                "index dimension {} does not match requested dimension {}",
                header.metadata().dims,
                dim
            ));
        }

        let payload_record_size = header.metadata().associated_data_length as u32;
        let searcher = match payload_record_size {
            0 => DiskSearcher::NoPayload(Self::open_typed::<GraphDataF32Vector>(
                &index_prefix,
                metric,
                num_threads,
                search_io_limit,
                num_nodes_to_cache,
            )?),
            8 => DiskSearcher::Payload8(Self::open_typed::<GraphDataF32VectorPayload8>(
                &index_prefix,
                metric,
                num_threads,
                search_io_limit,
                num_nodes_to_cache,
            )?),
            12 => DiskSearcher::Payload12(Self::open_typed::<GraphDataF32VectorPayload12>(
                &index_prefix,
                metric,
                num_threads,
                search_io_limit,
                num_nodes_to_cache,
            )?),
            other => {
                return Err(format!(
                    "unsupported DiskANN associated_data_length {other}; expected 0, 8, or 12"
                ));
            }
        };

        Ok(Self {
            dim,
            num_points,
            disk_index_file_size,
            payload_record_size,
            searcher,
        })
    }

    fn open_typed<Data>(
        index_prefix: &str,
        metric: DiskANNMetric,
        num_threads: u32,
        search_io_limit: u32,
        num_nodes_to_cache: u32,
    ) -> Result<
        DiskIndexSearcher<Data, DiskVertexProviderFactory<Data, AlignedFileReaderFactory>>,
        String,
    >
    where
        Data: GraphDataType<VectorDataType = f32, VectorIdType = u32>,
    {
        let vertex_provider_factory =
            DiskVertexProviderFactory::<Data, AlignedFileReaderFactory>::new(
                AlignedFileReaderFactory::new(get_disk_index_file(index_prefix)),
                if num_nodes_to_cache > 0 {
                    CachingStrategy::StaticCacheWithBfsNodes(num_nodes_to_cache as usize)
                } else {
                    CachingStrategy::None
                },
            )
            .map_err(|err| err.to_string())?;

        let index_reader = DiskIndexReader::<f32>::new(
            get_pq_pivot_file(index_prefix),
            get_compressed_pq_file(index_prefix),
            &FILE_STORAGE,
        )
        .map_err(|err| err.to_string())?;

        DiskIndexSearcher::<Data, DiskVertexProviderFactory<Data, AlignedFileReaderFactory>>::new(
            num_threads as usize,
            search_io_limit as usize,
            &index_reader,
            vertex_provider_factory,
            metric.to_metric(),
            None,
        )
        .map_err(|err| err.to_string())
    }

    fn search(
        &self,
        query: &[f32],
        k: u32,
        search_list_size: u32,
        beam_width: u32,
    ) -> Result<(Vec<u64>, Vec<f32>, Option<Vec<u8>>), String> {
        if query.len() != self.dim as usize {
            return Err(format!(
                "dimension mismatch: query dimension {} does not match index dimension {}",
                query.len(),
                self.dim
            ));
        }

        if k == 0 {
            return Ok((Vec::new(), Vec::new(), None));
        }

        // Pad query to match DiskANN's internal memory_aligned_dimension.
        // The disk vertex provider returns vectors of that padded length,
        // and the SIMD distance kernel requires both operands to have equal length.
        let aligned_dim = (self.dim as usize).next_multiple_of(DISKANN_VECTOR_ALIGNMENT);
        let mut padded_buf;
        let aligned_query: &[f32] = if aligned_dim != query.len() {
            padded_buf = vec![0.0f32; aligned_dim];
            padded_buf[..query.len()].copy_from_slice(query);
            &padded_buf
        } else {
            query
        };

        let effective_beam_width = if beam_width == 0 {
            None
        } else {
            Some(beam_width as usize)
        };

        match &self.searcher {
            DiskSearcher::NoPayload(searcher) => {
                let result = searcher
                    .search(
                        aligned_query,
                        k,
                        search_list_size.max(k),
                        effective_beam_width,
                        None,
                        false,
                    )
                    .map_err(|err| err.to_string())?;
                let mut ids = Vec::with_capacity(result.results.len());
                let mut distances = Vec::with_capacity(result.results.len());
                for item in result.results {
                    ids.push(u64::from(item.vertex_id));
                    distances.push(item.distance);
                }
                Ok((ids, distances, None))
            }
            DiskSearcher::Payload8(searcher) => {
                let result = searcher
                    .search(
                        aligned_query,
                        k,
                        search_list_size.max(k),
                        effective_beam_width,
                        None,
                        false,
                    )
                    .map_err(|err| err.to_string())?;
                let mut ids = Vec::with_capacity(result.results.len());
                let mut distances = Vec::with_capacity(result.results.len());
                let mut payload = Vec::with_capacity(result.results.len() * 8);
                for item in result.results {
                    ids.push(u64::from(item.vertex_id));
                    distances.push(item.distance);
                    payload.extend_from_slice(&item.data);
                }
                Ok((ids, distances, Some(payload)))
            }
            DiskSearcher::Payload12(searcher) => {
                let result = searcher
                    .search(
                        aligned_query,
                        k,
                        search_list_size.max(k),
                        effective_beam_width,
                        None,
                        false,
                    )
                    .map_err(|err| err.to_string())?;
                let mut ids = Vec::with_capacity(result.results.len());
                let mut distances = Vec::with_capacity(result.results.len());
                let mut payload = Vec::with_capacity(result.results.len() * 12);
                for item in result.results {
                    ids.push(u64::from(item.vertex_id));
                    distances.push(item.distance);
                    payload.extend_from_slice(&item.data);
                }
                Ok((ids, distances, Some(payload)))
            }
        }
    }
}

fn set_last_error(msg: impl Into<String>) {
    LAST_ERROR.with(|cell| {
        *cell.borrow_mut() = msg.into();
    });
}

fn next_handle() -> i64 {
    NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
}

fn read_c_string(ptr: *const c_char, argument_name: &str) -> Result<String, String> {
    if ptr.is_null() {
        return Err(format!("{argument_name} is null"));
    }

    let cstr = unsafe { CStr::from_ptr(ptr) };
    let value = cstr
        .to_str()
        .map_err(|err| format!("{argument_name} is not valid UTF-8: {err}"))?;

    if value.is_empty() {
        return Err(format!("{argument_name} must not be empty"));
    }

    Ok(value.to_string())
}

fn ensure_parent_directory(pathlike: &str) -> Result<(), String> {
    let path = Path::new(pathlike);
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|err| {
                format!(
                    "failed to create parent directory {}: {err}",
                    parent.display()
                )
            })?;
        }
    }
    Ok(())
}

fn index_files_exist(index_prefix: &str) -> bool {
    FILE_STORAGE.exists(&get_disk_index_file(index_prefix))
        && FILE_STORAGE.exists(&get_pq_pivot_file(index_prefix))
        && FILE_STORAGE.exists(&get_compressed_pq_file(index_prefix))
}

fn catch_ffi<F: FnOnce() -> i64 + panic::UnwindSafe>(f: F) -> i64 {
    match panic::catch_unwind(f) {
        Ok(value) => value,
        Err(error) => {
            let message = if let Some(text) = error.downcast_ref::<&str>() {
                text.to_string()
            } else if let Some(text) = error.downcast_ref::<String>() {
                text.clone()
            } else {
                "unknown panic".to_string()
            };

            set_last_error(format!("panic: {message}"));
            ERR_PANIC
        }
    }
}

#[no_mangle]
pub extern "C" fn diskann_create_disk_builder(
    dim: u32,
    metric: DiskANNMetric,
    pruned_degree: u32,
    max_degree: u32,
    l_build: u32,
    alpha: f32,
    num_threads: u32,
    pq_chunks: u32,
    build_ram_limit_gb: f64,
) -> i64 {
    catch_ffi(|| {
        match DiskIndexBuildState::new(
            dim,
            metric,
            pruned_degree,
            max_degree,
            l_build,
            alpha,
            num_threads,
            pq_chunks,
            build_ram_limit_gb,
        ) {
            Ok(builder) => {
                let handle = next_handle();
                BUILDERS
                    .lock()
                    .unwrap()
                    .insert(handle, Arc::new(Mutex::new(builder)));
                handle
            }
            Err(err) => {
                set_last_error(err);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn diskann_drop_builder(handle: i64) {
    let _ = panic::catch_unwind(|| {
        BUILDERS.lock().unwrap().remove(&handle);
    });
}

/// Look up a builder handle, clone its `Arc<Mutex<_>>`, and release the global
/// `BUILDERS` lock before the caller does any real work on the state. All
/// builder FFI entry points go through this helper.
fn lookup_builder(handle: i64) -> Result<Arc<Mutex<DiskIndexBuildState>>, ()> {
    let builders = BUILDERS.lock().unwrap();
    match builders.get(&handle) {
        Some(builder) => Ok(Arc::clone(builder)),
        None => {
            set_last_error(format!("invalid builder handle: {handle}"));
            Err(())
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn diskann_builder_set_data_path(
    handle: i64,
    data_path: *const c_char,
) -> i64 {
    catch_ffi(move || {
        let data_path = match read_c_string(data_path, "data_path") {
            Ok(path) => path,
            Err(err) => {
                set_last_error(err);
                return ERR_NULL_PTR;
            }
        };

        let builder = match lookup_builder(handle) {
            Ok(b) => b,
            Err(()) => return ERR_INVALID_HANDLE,
        };
        builder.lock().unwrap().set_data_path(data_path);
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_builder_set_index_prefix(
    handle: i64,
    index_prefix: *const c_char,
) -> i64 {
    catch_ffi(move || {
        let index_prefix = match read_c_string(index_prefix, "index_prefix") {
            Ok(prefix) => prefix,
            Err(err) => {
                set_last_error(err);
                return ERR_NULL_PTR;
            }
        };

        let builder = match lookup_builder(handle) {
            Ok(b) => b,
            Err(()) => return ERR_INVALID_HANDLE,
        };
        builder.lock().unwrap().set_index_prefix(index_prefix);
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_builder_set_associated_data_path(
    handle: i64,
    associated_data_path: *const c_char,
    record_size: u32,
) -> i64 {
    catch_ffi(move || {
        let associated_data_path = match read_c_string(associated_data_path, "associated_data_path")
        {
            Ok(path) => path,
            Err(err) => {
                set_last_error(err);
                return ERR_NULL_PTR;
            }
        };

        let builder = match lookup_builder(handle) {
            Ok(b) => b,
            Err(()) => return ERR_INVALID_HANDLE,
        };
        let result = builder
            .lock()
            .unwrap()
            .set_associated_data_path(associated_data_path, record_size);
        match result {
            Ok(()) => 0,
            Err(err) => {
                set_last_error(err);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn diskann_builder_build(handle: i64) -> i64 {
    catch_ffi(|| {
        let builder = match lookup_builder(handle) {
            Ok(b) => b,
            Err(()) => return ERR_INVALID_HANDLE,
        };

        // Snapshot the build configuration under the per-handle mutex,
        // then release the mutex before calling `build()`. The actual
        // graph construction is the multi-minute hot path and must not
        // hold any FFI lock — that's the whole point of the rewrite.
        let snapshot = builder.lock().unwrap().clone();
        drop(builder);

        match snapshot.build() {
            Ok(()) => 0,
            Err(err) => {
                set_last_error(err);
                -1
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_open_searcher(
    index_prefix: *const c_char,
    dim: u32,
    metric: DiskANNMetric,
    num_threads: u32,
    search_io_limit: u32,
    num_nodes_to_cache: u32,
) -> i64 {
    catch_ffi(move || {
        let index_prefix = match read_c_string(index_prefix, "index_prefix") {
            Ok(prefix) => prefix,
            Err(err) => {
                set_last_error(err);
                return ERR_NULL_PTR;
            }
        };

        match DiskIndexSearchState::open(
            index_prefix,
            dim,
            metric,
            num_threads,
            search_io_limit,
            num_nodes_to_cache,
        ) {
            Ok(searcher) => {
                let handle = next_handle();
                SEARCHERS.lock().unwrap().insert(handle, Arc::new(searcher));
                handle
            }
            Err(err) => {
                set_last_error(err);
                -1
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn diskann_close_searcher(handle: i64) {
    let _ = panic::catch_unwind(|| {
        SEARCHERS.lock().unwrap().remove(&handle);
    });
}

/// Look up a searcher handle, clone its `Arc`, and release the global
/// `SEARCHERS` lock before the caller does any work. All searcher FFI
/// entry points go through this helper so the global lock is only held
/// for a HashMap lookup, never during the actual search.
fn lookup_searcher(handle: i64) -> Result<Arc<DiskIndexSearchState>, ()> {
    let searchers = SEARCHERS.lock().unwrap();
    match searchers.get(&handle) {
        Some(searcher) => Ok(Arc::clone(searcher)),
        None => {
            set_last_error(format!("invalid searcher handle: {handle}"));
            Err(())
        }
    }
}

#[no_mangle]
pub extern "C" fn diskann_searcher_num_points(handle: i64) -> i64 {
    catch_ffi(|| match lookup_searcher(handle) {
        Ok(state) => state.num_points as i64,
        Err(()) => ERR_INVALID_HANDLE,
    })
}

#[no_mangle]
pub extern "C" fn diskann_searcher_dimensions(handle: i64) -> i64 {
    catch_ffi(|| match lookup_searcher(handle) {
        Ok(state) => i64::from(state.dim),
        Err(()) => ERR_INVALID_HANDLE,
    })
}

#[no_mangle]
pub extern "C" fn diskann_searcher_memory_usage(handle: i64) -> i64 {
    catch_ffi(|| match lookup_searcher(handle) {
        Ok(state) => state.disk_index_file_size as i64,
        Err(()) => ERR_INVALID_HANDLE,
    })
}

#[no_mangle]
pub extern "C" fn diskann_searcher_payload_record_size(handle: i64) -> i64 {
    catch_ffi(|| match lookup_searcher(handle) {
        Ok(state) => i64::from(state.payload_record_size),
        Err(()) => ERR_INVALID_HANDLE,
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_search_disk_index(
    handle: i64,
    query_ptr: *const f32,
    dim: u32,
    k: u32,
    search_list_size: u32,
    beam_width: u32,
    results_ptr: *mut u64,
    distances_ptr: *mut f32,
) -> i64 {
    catch_ffi(move || {
        if query_ptr.is_null() {
            set_last_error("query_ptr is null");
            return ERR_NULL_PTR;
        }

        if k > 0 && (results_ptr.is_null() || distances_ptr.is_null()) {
            set_last_error("output pointer is null");
            return ERR_NULL_PTR;
        }

        let query = unsafe { std::slice::from_raw_parts(query_ptr, dim as usize) };

        let searcher = match lookup_searcher(handle) {
            Ok(s) => s,
            Err(()) => return ERR_INVALID_HANDLE,
        };

        match searcher.search(query, k, search_list_size, beam_width) {
            Ok((ids, distances, _payload)) => {
                let result_count = ids.len();
                if result_count > 0 {
                    let result_buffer =
                        unsafe { std::slice::from_raw_parts_mut(results_ptr, result_count) };
                    let distance_buffer =
                        unsafe { std::slice::from_raw_parts_mut(distances_ptr, result_count) };
                    result_buffer.copy_from_slice(&ids);
                    distance_buffer.copy_from_slice(&distances);
                }
                result_count as i64
            }
            Err(err) => {
                if err.contains("dimension mismatch") {
                    set_last_error(err);
                    ERR_DIM_MISMATCH
                } else {
                    set_last_error(err);
                    -1
                }
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_search_disk_index_with_payload(
    handle: i64,
    query_ptr: *const f32,
    dim: u32,
    k: u32,
    search_list_size: u32,
    beam_width: u32,
    results_ptr: *mut u64,
    distances_ptr: *mut f32,
    payload_ptr: *mut u8,
    payload_record_size: u32,
) -> i64 {
    catch_ffi(move || {
        if query_ptr.is_null() {
            set_last_error("query_ptr is null");
            return ERR_NULL_PTR;
        }

        if k > 0 && (results_ptr.is_null() || distances_ptr.is_null() || payload_ptr.is_null()) {
            set_last_error("output pointer is null");
            return ERR_NULL_PTR;
        }

        let query = unsafe { std::slice::from_raw_parts(query_ptr, dim as usize) };

        let searcher = match lookup_searcher(handle) {
            Ok(s) => s,
            Err(()) => return ERR_INVALID_HANDLE,
        };

        if searcher.payload_record_size != payload_record_size {
            set_last_error(format!(
                "payload_record_size mismatch: index has {}, caller requested {}",
                searcher.payload_record_size, payload_record_size
            ));
            return -1;
        }

        match searcher.search(query, k, search_list_size, beam_width) {
            Ok((ids, distances, payload)) => {
                let result_count = ids.len();
                if result_count > 0 {
                    let payload = match payload {
                        Some(payload) => payload,
                        None => {
                            set_last_error("index search returned no payload");
                            return -1;
                        }
                    };
                    if payload.len() != result_count * payload_record_size as usize {
                        set_last_error(format!(
                            "payload size mismatch: got {} bytes for {} results * {} bytes",
                            payload.len(),
                            result_count,
                            payload_record_size
                        ));
                        return -1;
                    }
                    let result_buffer =
                        unsafe { std::slice::from_raw_parts_mut(results_ptr, result_count) };
                    let distance_buffer =
                        unsafe { std::slice::from_raw_parts_mut(distances_ptr, result_count) };
                    let payload_buffer = unsafe {
                        std::slice::from_raw_parts_mut(
                            payload_ptr,
                            result_count * payload_record_size as usize,
                        )
                    };
                    result_buffer.copy_from_slice(&ids);
                    distance_buffer.copy_from_slice(&distances);
                    payload_buffer.copy_from_slice(&payload);
                }
                result_count as i64
            }
            Err(err) => {
                if err.contains("dimension mismatch") {
                    set_last_error(err);
                    ERR_DIM_MISMATCH
                } else {
                    set_last_error(err);
                    -1
                }
            }
        }
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_index_file_exists(index_prefix: *const c_char) -> i64 {
    catch_ffi(move || {
        let index_prefix = match read_c_string(index_prefix, "index_prefix") {
            Ok(prefix) => prefix,
            Err(err) => {
                set_last_error(err);
                return ERR_NULL_PTR;
            }
        };

        if index_files_exist(&index_prefix) {
            1
        } else {
            0
        }
    })
}

/// Stateless batched distance kernel matching the index `metric`.
///
/// Computes `out[i] = distance(query, candidates[i*dim ..(i+1)*dim])` for `i` in `[0, n)`,
/// using the same SIMD kernel that DiskANN uses internally during graph search. Intended
/// for callers (e.g. ClickHouse's unindexed-parts dispatch) that want kernel parity with
/// the index path without holding any builder/searcher handle.
///
/// Returns 0 on success, or one of `ERR_NULL_PTR` / `ERR_PANIC` on failure.
///
/// Semantics (matches DiskANN's internal definitions):
///   * `L2`: squared L2 distance.
///   * `Cosine`: 1 - cosine_similarity, valid for unnormalised vectors.
///   * `InnerProduct`: negative inner product.
///   * `CosineNormalized`: 1 - dot product, valid for normalised vectors.
#[no_mangle]
pub unsafe extern "C" fn diskann_compute_distances(
    metric: DiskANNMetric,
    dim: u32,
    query_ptr: *const f32,
    candidates_ptr: *const f32,
    n: u64,
    out_ptr: *mut f32,
) -> i64 {
    catch_ffi(move || {
        if n == 0 {
            return 0;
        }

        if query_ptr.is_null() || candidates_ptr.is_null() || out_ptr.is_null() {
            set_last_error("query_ptr / candidates_ptr / out_ptr is null");
            return ERR_NULL_PTR;
        }

        if dim == 0 {
            set_last_error("dim must be > 0 when n > 0");
            return ERR_DIM_MISMATCH;
        }

        let dim_usize = dim as usize;
        let n_usize = n as usize;

        let total = match dim_usize.checked_mul(n_usize) {
            Some(v) => v,
            None => {
                set_last_error("dim * n overflow");
                return ERR_DIM_MISMATCH;
            }
        };

        let query = unsafe { std::slice::from_raw_parts(query_ptr, dim_usize) };
        let candidates = unsafe { std::slice::from_raw_parts(candidates_ptr, total) };
        let out = unsafe { std::slice::from_raw_parts_mut(out_ptr, n_usize) };

        let comparer =
            <f32 as DistanceProvider<f32>>::distance_comparer(metric.to_metric(), Some(dim_usize));

        for i in 0..n_usize {
            let begin = i * dim_usize;
            let cand = &candidates[begin..begin + dim_usize];
            out[i] = comparer.call(query, cand);
        }
        0
    })
}

#[no_mangle]
pub unsafe extern "C" fn diskann_last_error(buf: *mut c_char, buf_size: u64) -> i64 {
    if buf.is_null() || buf_size == 0 {
        return -1;
    }

    LAST_ERROR.with(|cell| {
        let error = cell.borrow();
        if error.is_empty() {
            return -1;
        }

        let bytes = error.as_bytes();
        let copy_len = bytes.len().min(buf_size as usize - 1);
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf as *mut u8, copy_len);
            *buf.add(copy_len) = 0;
        }
        copy_len as i64
    })
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    use diskann_utils::io::write_bin;
    use diskann_utils::views::{Init, Matrix};

    use super::*;

    fn unique_test_dir(name: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("clickhouse-diskann-test-{name}-{nanos}"));
        fs::create_dir_all(&path).unwrap();
        path.to_string_lossy().into_owned()
    }

    fn write_test_fbin(path: &str, rows: usize, dim: usize) {
        let mut next = 1.0f32;
        let matrix = Matrix::new(
            Init(|| {
                let value = next;
                next += 1.0;
                value
            }),
            rows,
            dim,
        );

        let mut writer = FILE_STORAGE.create_for_write(path).unwrap();
        write_bin(matrix.as_view(), &mut writer).unwrap();
    }

    fn write_test_payload8(path: &str, rows: usize) {
        let mut writer = FILE_STORAGE.create_for_write(path).unwrap();
        writer.write_all(&(rows as u32).to_le_bytes()).unwrap();
        writer.write_all(&1u32.to_le_bytes()).unwrap();
        for row in 0..rows {
            let mut payload = [0u8; 8];
            payload[..4].copy_from_slice(&(row as u32).to_le_bytes());
            payload[4..].copy_from_slice(&((row as u32) + 1000).to_le_bytes());
            writer.write_all(&payload).unwrap();
        }
    }

    #[test]
    fn builds_and_searches_real_disk_index() {
        let dir = unique_test_dir("build-search");
        let data_path = format!("{dir}/vectors.fbin");
        let index_prefix = format!("{dir}/test_index");
        write_test_fbin(&data_path, 32, 8);

        let builder =
            diskann_create_disk_builder(8, DiskANNMetric::L2, 16, 32, 64, 1.2, 1, 4, 0.25);
        assert!(builder > 0);

        let data_path_c = CString::new(data_path.clone()).unwrap();
        let index_prefix_c = CString::new(index_prefix.clone()).unwrap();
        assert_eq!(
            unsafe { diskann_builder_set_data_path(builder, data_path_c.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe { diskann_builder_set_index_prefix(builder, index_prefix_c.as_ptr()) },
            0
        );
        assert_eq!(diskann_builder_build(builder), 0);
        assert_eq!(
            unsafe { diskann_index_file_exists(index_prefix_c.as_ptr()) },
            1
        );

        let searcher = unsafe {
            diskann_open_searcher(index_prefix_c.as_ptr(), 8, DiskANNMetric::L2, 1, 4, 0)
        };
        assert!(searcher > 0);

        let query = [1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mut ids = vec![0u64; 5];
        let mut distances = vec![0.0f32; 5];
        let found = unsafe {
            diskann_search_disk_index(
                searcher,
                query.as_ptr(),
                8,
                5,
                20,
                4,
                ids.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };

        assert!(found > 0);
        assert_eq!(ids[0], 0);
        assert!(distances[0] >= 0.0);

        diskann_close_searcher(searcher);
        diskann_drop_builder(builder);
    }

    #[test]
    fn builds_and_searches_disk_index_with_payload8() {
        let dir = unique_test_dir("build-search-payload8");
        let data_path = format!("{dir}/vectors.fbin");
        let payload_path = format!("{dir}/payload8.bin");
        let index_prefix = format!("{dir}/test_index");
        write_test_fbin(&data_path, 32, 8);
        write_test_payload8(&payload_path, 32);

        let builder =
            diskann_create_disk_builder(8, DiskANNMetric::L2, 16, 32, 64, 1.2, 1, 4, 0.25);
        assert!(builder > 0);

        let data_path_c = CString::new(data_path.clone()).unwrap();
        let payload_path_c = CString::new(payload_path.clone()).unwrap();
        let index_prefix_c = CString::new(index_prefix.clone()).unwrap();
        assert_eq!(
            unsafe { diskann_builder_set_data_path(builder, data_path_c.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe { diskann_builder_set_index_prefix(builder, index_prefix_c.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe {
                diskann_builder_set_associated_data_path(builder, payload_path_c.as_ptr(), 8)
            },
            0
        );
        assert_eq!(diskann_builder_build(builder), 0);

        let searcher = unsafe {
            diskann_open_searcher(index_prefix_c.as_ptr(), 8, DiskANNMetric::L2, 1, 4, 0)
        };
        assert!(searcher > 0);
        assert_eq!(diskann_searcher_payload_record_size(searcher), 8);

        let query = [1.0f32, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let mut ids = vec![0u64; 5];
        let mut distances = vec![0.0f32; 5];
        let mut payload = vec![0u8; 5 * 8];
        let found = unsafe {
            diskann_search_disk_index_with_payload(
                searcher,
                query.as_ptr(),
                8,
                5,
                20,
                4,
                ids.as_mut_ptr(),
                distances.as_mut_ptr(),
                payload.as_mut_ptr(),
                8,
            )
        };
        assert!(found > 0);
        assert_eq!(ids[0], 0);
        assert_eq!(&payload[..8], &[0, 0, 0, 0, 232, 3, 0, 0]);

        diskann_close_searcher(searcher);
        diskann_drop_builder(builder);
    }

    #[test]
    fn searcher_metadata_accessors() {
        let dir = unique_test_dir("searcher-metadata");
        let data_path = format!("{dir}/vectors.fbin");
        let index_prefix = format!("{dir}/test_index");
        let num_rows: usize = 32;
        let dim: u32 = 8;
        write_test_fbin(&data_path, num_rows, dim as usize);

        let builder =
            diskann_create_disk_builder(dim, DiskANNMetric::L2, 16, 32, 64, 1.2, 1, 4, 0.25);
        assert!(builder > 0);

        let data_path_c = CString::new(data_path.clone()).unwrap();
        let index_prefix_c = CString::new(index_prefix.clone()).unwrap();
        assert_eq!(
            unsafe { diskann_builder_set_data_path(builder, data_path_c.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe { diskann_builder_set_index_prefix(builder, index_prefix_c.as_ptr()) },
            0
        );
        assert_eq!(diskann_builder_build(builder), 0);

        let searcher = unsafe {
            diskann_open_searcher(index_prefix_c.as_ptr(), dim, DiskANNMetric::L2, 1, 4, 0)
        };
        assert!(searcher > 0);

        assert_eq!(diskann_searcher_num_points(searcher), num_rows as i64);
        assert_eq!(diskann_searcher_dimensions(searcher), dim as i64);
        assert!(diskann_searcher_memory_usage(searcher) > 0);

        // Invalid handle should return ERR_INVALID_HANDLE.
        assert_eq!(diskann_searcher_num_points(999999), ERR_INVALID_HANDLE);
        assert_eq!(diskann_searcher_dimensions(999999), ERR_INVALID_HANDLE);
        assert_eq!(diskann_searcher_memory_usage(999999), ERR_INVALID_HANDLE);

        diskann_close_searcher(searcher);
        diskann_drop_builder(builder);
    }

    #[test]
    fn rejects_dimension_mismatch_on_search() {
        let dir = unique_test_dir("dim-mismatch");
        let data_path = format!("{dir}/vectors.fbin");
        let index_prefix = format!("{dir}/test_index");
        write_test_fbin(&data_path, 16, 6);

        let builder =
            diskann_create_disk_builder(6, DiskANNMetric::L2, 16, 32, 64, 1.2, 1, 3, 0.25);
        assert!(builder > 0);

        let data_path_c = CString::new(data_path.clone()).unwrap();
        let index_prefix_c = CString::new(index_prefix.clone()).unwrap();
        assert_eq!(
            unsafe { diskann_builder_set_data_path(builder, data_path_c.as_ptr()) },
            0
        );
        assert_eq!(
            unsafe { diskann_builder_set_index_prefix(builder, index_prefix_c.as_ptr()) },
            0
        );
        assert_eq!(diskann_builder_build(builder), 0);

        let searcher = unsafe {
            diskann_open_searcher(index_prefix_c.as_ptr(), 6, DiskANNMetric::L2, 1, 4, 0)
        };
        assert!(searcher > 0);

        let query = [1.0f32, 2.0, 3.0];
        let mut ids = vec![0u64; 3];
        let mut distances = vec![0.0f32; 3];
        let rc = unsafe {
            diskann_search_disk_index(
                searcher,
                query.as_ptr(),
                3,
                3,
                10,
                2,
                ids.as_mut_ptr(),
                distances.as_mut_ptr(),
            )
        };

        assert_eq!(rc, ERR_DIM_MISMATCH);
        diskann_close_searcher(searcher);
        diskann_drop_builder(builder);
    }
}
