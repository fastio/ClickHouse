#include <Storages/Reflection/ANNIndex/ANNIndexReadHint.h>

#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <utility>


namespace DB
{

void applyANNIndexHintsToReadStep(
    ReadFromMergeTree & read_step, ANNIndexHints hints, bool keep_search_column, const String & search_column)
{
    if (keep_search_column)
        read_step.addDistanceColumnForVectorSearch();
    else
        read_step.replaceVectorColumnWithDistanceColumn(search_column);

    /// The optimizer calls selectRangesToRead for coverage/cost before the
    /// winner is known. If that cached analysis already exists, setting hints
    /// on the read step alone is too late: apply them to the cached parts too.
    if (auto analyzed = read_step.getAnalyzedResult())
        applyANNIndexHints(analyzed->parts_with_ranges, hints);

    read_step.setANNIndexHints(std::move(hints));
}

}
