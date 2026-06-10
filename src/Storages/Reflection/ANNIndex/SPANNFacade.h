#pragma once

#include "config.h"

#if USE_SPTAG

#include <Core/Types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>


namespace DB::SPANNFacade
{

enum class Metric : UInt8
{
    L2 = 0,
    Cosine = 1,
    InnerProduct = 2,
};

enum class BuildStage : UInt8
{
    NotStarted = 0,
    CollectRows = 1,
    ValidatePayload = 2,
    BuildSPTAGIndex = 3,
    SaveSPTAGIndex = 4,
    End = 5,
    Unknown = 255,
};

struct BuildStatusSnapshot
{
    BuildStage stage = BuildStage::NotStarted;
    BuildStage next_stage = BuildStage::NotStarted;
    UInt8 started = 0;
    UInt8 finished = 0;
    UInt8 failed = 0;
    UInt64 progress = 0;
    UInt64 progress_total = 0;
    UInt64 rows_processed = 0;
    UInt64 rows_total = 0;
    UInt64 vector_bytes = 0;
    UInt8 has_payload = 0;
    UInt64 payload_record_size = 0;
    String error_message;
    std::map<String, String> settings;
};

class BuildStatus
{
public:
    BuildStatusSnapshot snapshot() const;

    void markStarted(UInt64 rows_total_);
    void setStage(BuildStage stage_, BuildStage next_stage_);
    void setProgress(UInt64 progress_, UInt64 progress_total_);
    void setRows(UInt64 rows_processed_, UInt64 rows_total_);
    void setVectorBytes(UInt64 vector_bytes_);
    void setPayload(UInt8 has_payload_, UInt64 payload_record_size_);
    void setSettings(std::map<String, String> settings_);
    void markFinished();
    void markFailed(const String & error_message_);

private:
    mutable std::mutex mutex;
    BuildStatusSnapshot current;
};

using BuildStatusPtr = std::shared_ptr<BuildStatus>;

void registerBuildStatus(const String & task_id, BuildStatusPtr status);
void unregisterBuildStatus(const String & task_id);
std::optional<BuildStatusSnapshot> getBuildStatusForTask(const String & task_id);

struct BuildParams
{
    /// Mandatory.
    Metric metric = Metric::L2;
    UInt32 dim = 0;

    /// SelectHead.* — head sampling for the BKT head index.
    /// `select_type` picks the head-sampling strategy: SPTAG's default `"BKT"`
    /// builds a BKT and runs the serial `SelectHeadDynamically` binary search
    /// (`SPANNIndex.cpp:827`) — high recall, but linear in head_ratio × N and
    /// single-threaded. `"Random"` skips that step entirely and shuffles the
    /// base set, picking the first `ratio × N` indices in milliseconds.
    String select_type             = "BKT";
    float  head_ratio              = 0.1f;
    UInt32 select_samples_number   = 1000;
    UInt32 select_threshold        = 6;
    UInt32 split_factor            = 5;
    UInt32 split_threshold         = 25;

    /// BuildHead.* — internal BKT graph construction. Forwarded to the inner
    /// BKT `VectorIndex` via `SPANN::Index::SetParameter("BuildHead.*", ...)`.
    UInt32 bkt_number                  = 1;
    UInt32 bkt_kmeans_k                = 32;
    UInt32 bkt_leaf_size               = 8;
    UInt32 neighborhood_size           = 32;
    UInt32 cef                         = 1000;
    UInt32 max_check_for_refine_graph  = 8192;
    UInt32 refine_iterations           = 2;
    UInt32 tpt_number                  = 32;
    float  rng_factor                  = 1.0f;

    /// BuildSSDIndex.* — posting layout (build-time, structural).
    UInt32 posting_page_limit          = 15;
    UInt32 posting_vector_limit        = 118;
    UInt32 replica_count               = 8;
    /// `num_threads` historically wired SelectHead, BuildHead and BuildSSDIndex
    /// to the same value. SPTAG's K-means inside `BKTree::BuildTrees` reaches
    /// `KmeansAssign` with batches of 1000 sample vectors per iteration; at
    /// `_TH=32` each thread gets ~32 distance evaluations, which is dwarfed by
    /// `std::thread` construction (~10–50 µs). The end result is a 1.5-core
    /// workload that scales backwards with thread count. The other two phases
    /// (RNG graph refinement in BuildHead, candidate searching in BuildSSDIndex)
    /// are true parallel and want all available CPU. Splitting the knob lets
    /// the defaults capture both shapes (4 / 32 / 32) without forcing users
    /// to know SPTAG internals.
    UInt32 num_threads                 = 4;
    /// SPTAG `SelectHead.NumberOfThreads`. K-means-on-1k-sample is bound by
    /// `std::thread` overhead; default kept tiny (matches old `num_threads=4`
    /// SPTAG default).
    UInt32 select_head_threads         = 4;
    /// SPTAG `BuildHead.NumberOfThreads`. Drives both the head-set BKTree
    /// build (same K-means overhead pattern as SelectHead) and the RNG graph
    /// TpTree partition (true parallel). Default favours the parallel phase.
    UInt32 build_head_threads          = 32;
    /// `IOThreadsPerHandler` in SPTAG terminology — the size of SPTAG's
    /// internal async-IO workspace pool for `ExtraFileController`. The
    /// SPTAG default of 4 underflows under concurrent search; we raise the
    /// default to 16 and let users override it.
    UInt32 io_threads                  = 16;

    /// Build-time `SPTAG` switches. They shape either the input-reader path or
    /// the persisted posting format and have no search-time twin.
    bool   mmap_vectors                  = true;
    bool   select_head_parallel          = true;
    bool   enable_data_compression       = false;
    bool   enable_delta_encoding         = false;
    bool   enable_posting_list_rearrange = false;

    /// BuildSSDIndex.* — search-time tunables (also overridable via session
    /// settings; the values here are the DDL-baked defaults).
    UInt32 search_posting_page_limit   = 15;
    UInt32 internal_result_num         = 64;
    UInt32 max_check                   = 4096;
    float  max_dist_ratio              = 10000.0f;
    UInt32 hash_table_exponent         = 4;
    UInt32 io_timeout_us               = 30;
};

struct SearchResult
{
    std::vector<UInt64> vids;
    std::vector<float> distances;
    /// Optional inline payload, parallel to `vids`. Either empty (no
    /// metadata in the index) or `vids.size() * record_size` bytes long,
    /// where `record_size` is governed by
    /// `ANNIndexPayloadCodec::Version` baked into this MI-part's
    /// `coverage.json::payload_format_version`. The Facade is neutral
    /// about which version is in play; the caller validates the size
    /// against its expected `record_size`.
    std::vector<uint8_t> payload_bytes;
};

String metricName(Metric metric);
UInt64 metricId(Metric metric);

class Searcher
{
public:
    Searcher(const String & folder_path, const BuildParams & params_);
    ~Searcher();

    Searcher(const Searcher &) = delete;
    Searcher & operator=(const Searcher &) = delete;

    Searcher(Searcher &&) noexcept;
    Searcher & operator=(Searcher &&) noexcept;

    SearchResult search(const float * query, UInt32 dim, size_t candidate_limit) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
    BuildParams params;
};

void buildIndex(const BuildParams & params, float * vectors, UInt64 rows, const String & folder_path, BuildStatus * status = nullptr);

/// Build with per-vector inline payload. `payload_bytes` MUST be
/// `rows * record_size` bytes long, in row order matching `vectors`. The
/// metadata is persisted under `folder_path` as SPTAG `metadata.bin` +
/// `metadataIndex.bin`, automatically reloaded on `Searcher` open.
/// `record_size` is supplied by `ANNIndexPayloadCodec::recordSize` for
/// the chosen `Version`.
void buildIndexWithPayload(
    const BuildParams & params,
    float * vectors,
    UInt64 rows,
    const uint8_t * payload_bytes,
    UInt64 record_size,
    const String & folder_path,
    BuildStatus * status = nullptr);

void computeDistances(
    Metric metric,
    UInt32 dim,
    const float * query,
    const float * candidates,
    UInt64 rows,
    float * out);

}

#endif
