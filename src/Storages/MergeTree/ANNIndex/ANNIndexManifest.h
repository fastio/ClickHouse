#pragma once

#include <Core/Types.h>
#include <Disks/IVolume.h>

#include <memory>
#include <vector>

namespace DB
{

struct IDiskTransaction;
using DiskTransactionPtr = std::shared_ptr<IDiskTransaction>;

/// Shape fingerprint describing "what an index shaped like this one looks like":
///   - `dim`         — vector dimensionality
///   - `metric`      — distance metric id (matches `DiskANNMetric`: L2 = 0, Cosine = 1, ...)
///   - `algorithm`   — algorithm identifier (currently only `"diskann"`)
///   - `params_hash` — XXH64 over the sorted `k=v` algorithm parameter string
///
/// If the fingerprint computed from the current `CREATE INDEX` definition does not match the
/// one stored on disk, the manager must retire all existing groups and start fresh.
struct ANNIndexShapeFingerprint
{
    UInt32 dim = 0;
    UInt8 metric = 0;
    String algorithm;
    UInt64 params_hash = 0;

    bool operator==(const ANNIndexShapeFingerprint & rhs) const = default;
};

/// Representation of `<relative_root_path>/manifest.json`.
///
/// JSON shape:
///   {
///     "version": 1,
///     "shape": {
///       "dim":         128,
///       "metric":      0,
///       "algorithm":   "diskann",
///       "params_hash": "0x..."       // hex string
///     },
///     "hash_algo":      "sipHash64",
///     "hash_seed":      "0x...",     // hex string
///     "active_groups":  ["<uuid>", ...],
///     "retired_groups": ["<uuid>", ...]
///   }
///
/// The manifest is a **table-level** artefact and therefore lives outside any group directory.
/// Reads/writes go through `VolumePtr` + `relative_root_path` directly rather than through
/// `IANNGroupStorage` (which is per-group by design).
struct ANNIndexManifest
{
    UInt32 version = 1;
    ANNIndexShapeFingerprint shape;
    String hash_algo = "sipHash64";
    UInt64 hash_seed = 0;
    std::vector<String> active_groups;
    std::vector<String> retired_groups;

    static constexpr std::string_view FILE_NAME = "manifest.json";

    /// Load `manifest.json` from `<relative_root_path>/manifest.json`. If the file does not
    /// exist an empty manifest is returned. Malformed JSON is reported as `CORRUPTED_DATA`.
    static ANNIndexManifest loadOrEmpty(VolumePtr volume, const std::string & relative_root_path);

    /// Persist the manifest.
    ///
    /// - When `txn` is non-null all writes are routed through the transaction so that the
    ///   caller can commit them atomically together with the rest of the operation.
    /// - When `txn` is null the write is performed in two steps (`manifest.json.tmp` + rename)
    ///   to survive a crash in the middle of the write.
    void writeTo(VolumePtr volume,
                 const std::string & relative_root_path,
                 DiskTransactionPtr txn = nullptr) const;
};

}
