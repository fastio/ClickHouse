#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB
{

class ReadFromMergeTree;

/// Structural rewrite, applied at plan-optimization time: make the covered
/// `ReadFromMergeTree` emit the `_distance` virtual column. This is independent
/// of the search results, and it MUST run during optimization because the
/// upstream `SortingStep` / `ExpressionStep` headers are rebuilt from the read
/// step's output header in the same pass.
///
/// This is the body of the engine callback ferried to the framework via
/// `ReflectionReadHintRealization::apply_structure_to_read_step`.
void applyANNIndexStructureToReadStep(
    ReadFromMergeTree & read_step, bool keep_search_column, const String & search_column);

/// Data injection, applied lazily at pipeline-build time: attach the
/// per-source-part nearest-neighbour hits to the read step and to the analysis
/// result the pipeline will actually read. The low-level hint-to-part plumbing
/// (`applyANNIndexHints`) lives in MergeTree (`RangesInDataPart.h`) because it
/// is a read-execution concern shared with the read step itself.
void applyANNIndexHintsToReadStep(ReadFromMergeTree & read_step, ANNIndexHints hints);

}
