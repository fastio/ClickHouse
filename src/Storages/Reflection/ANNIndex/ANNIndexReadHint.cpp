#include <Storages/Reflection/ANNIndex/ANNIndexReadHint.h>

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <utility>


namespace DB
{

void applyANNIndexStructureToReadStep(
    ReadFromMergeTree & read_step, bool keep_search_column, const String & search_column)
{
    if (keep_search_column)
        read_step.addDistanceColumnForVectorSearch();
    else
        read_step.replaceVectorColumnWithDistanceColumn(search_column);
}

void applyANNIndexHintsToReadStep(ReadFromMergeTree & read_step, ANNIndexHints hints)
{
    /// Runs at pipeline-build time, after `getAnalysisResult` has produced (or
    /// reused) the analysis result the pipeline will read. Apply hints to that
    /// exact instance — an optimization-time pointer could miss a freshly
    /// computed result.
    if (auto analyzed = read_step.getAnalyzedResult())
        applyANNIndexHints(analyzed->parts_with_ranges, hints);

    read_step.setANNIndexHints(std::move(hints));
}

}
