#pragma once
#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/DiskANNIndex.h>

namespace DB
{

struct StorageInMemoryMetadata;

/// Snapshot of the parameters extracted from `INDEX ... TYPE ann(...)`.
///
/// The DDL layer's only responsibility is to parse the arguments, validate them and materialise
/// this struct. All subsequent logic (build, search, group lifecycle) lives in
/// `ANNIndexManager` / `IANNIndexBuilder` / `ANNIndexGroup` and consumes the fields below.
struct ANNIndexDefinition
{
    ANNIndexShapeFingerprint shape;         /// dim / metric / algorithm / params_hash
    DiskANNBuildOptions build_options;      /// Parameters handed to `DiskANNDiskIndexBuilder`
    DiskANNSearchOptions search_defaults;   /// Parameters handed to every `DiskANNDiskIndexSearcher`
    String hash_algo = "sipHash64";         /// Fixed for now; here for future expansion
    UInt64 hash_seed = 0;                   /// Seed for the `partition_id -> UInt64` hash
    String vector_column_name;              /// `index.column_names[0]`; consumed by the builder
};

/// DDL-only front for the table-level DiskANN index.
///
/// Inherits from `IMergeTreeIndex` to hook into the existing secondary-index registration path,
/// but deliberately opts out of all per-granule storage / filtering semantics:
///   - `getSubstreams()` returns `{}`  -> no `skp_idx_*` files are produced or read;
///   - `isVectorSimilarityIndex()` / `isTextIndex()` return false -> the index is invisible to
///     the per-part index-selection heuristics;
///   - `createIndexGranule` / `createIndexAggregator` / `createIndexCondition` return `nullptr`
///     because the write / read pipelines never reach those methods once `getSubstreams` is empty.
///
/// The real construction and query paths live in `ANNIndexManager`, which is owned by
/// `MergeTreeData` and wired up by `StorageMergeTree`.
class MergeTreeIndexANN final : public IMergeTreeIndex
{
public:
    MergeTreeIndexANN(const IndexDescription & index_, ANNIndexDefinition definition_);
    ~MergeTreeIndexANN() override = default;

    MergeTreeIndexSubstreams getSubstreams() const override { return {}; }
    bool isVectorSimilarityIndex() const override { return false; }
    bool isTextIndex() const override { return false; }

    /// These exist because the base class declares them pure virtual. They intentionally return
    /// `nullptr`: `getSubstreams()` returns `{}`, so the write pipeline never calls
    /// `createIndexAggregator` / `createIndexGranule`, and the read pipeline never reaches
    /// `createIndexCondition` (skip-index selection filters this index out via the overrides above).
    /// If any of them are ever invoked, the caller will hit a null deref - which is the correct
    /// failure mode because it means the index landed on a code path that must not use it.
    MergeTreeIndexGranulePtr createIndexGranule() const override { return nullptr; }
    MergeTreeIndexAggregatorPtr createIndexAggregator() const override { return nullptr; }
    MergeTreeIndexConditionPtr createIndexCondition(
        const ActionsDAG::Node * /*predicate*/, ContextPtr /*context*/) const override { return nullptr; }

    const ANNIndexDefinition & getDefinition() const { return definition; }

private:
    ANNIndexDefinition definition;
};

/// Factory entry points registered with `MergeTreeIndexFactory`.
MergeTreeIndexPtr annIndexCreator(const IndexDescription & index);
void annIndexValidator(const IndexDescription & index, bool attach);

/// Helpers that walk `StorageInMemoryMetadata.secondary_indices` and extract ANN information.
/// Returns `true` iff an `ann` index is present. When `true`, `out_shape` / `out_definition`
/// are populated by re-running the argument parser on the found index.
bool extractANNShapeFromMetadata(const StorageInMemoryMetadata & metadata, ANNIndexShapeFingerprint & out_shape);
bool extractANNDefinitionFromMetadata(const StorageInMemoryMetadata & metadata, ANNIndexDefinition & out_definition);

/// Convenience: returns the source column name of the single `ann` index in `metadata`, or the
/// empty string when the table has no such index.
String getANNIndexColumnName(const StorageInMemoryMetadata & metadata);

}

#endif
