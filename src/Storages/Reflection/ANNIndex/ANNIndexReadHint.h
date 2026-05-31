#pragma once

#include <Core/Types.h>
#include <Storages/MergeTree/VectorSearchUtils.h>

namespace DB
{

class ReadFromMergeTree;

/// Prepare the covered `ReadFromMergeTree` to emit the `_distance` virtual
/// column and attach the per-source-part nearest-neighbour hits, including to
/// any cached analysed result the optimizer already produced for coverage/cost.
///
/// This is the body of the engine callback ferried to the framework via
/// `ReflectionReadHintRealization::apply_to_covered_read_step`. The low-level
/// hint-to-part plumbing (`applyANNIndexHints`) lives in MergeTree
/// (`RangesInDataPart.h`) because it is a read-execution concern shared with the
/// read step itself.
void applyANNIndexHintsToReadStep(
    ReadFromMergeTree & read_step, ANNIndexHints hints, bool keep_search_column, const String & search_column);

}
