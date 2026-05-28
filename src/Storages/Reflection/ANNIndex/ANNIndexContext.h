#pragma once

#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/// Metadata handed to an IANNIndexAlgorithm when the enclosing
/// storage is instantiated. Algorithms treat this as read-only context and
/// never mutate the fields. Extended in later stages with build / load paths
/// (see stage-0 §0.6.4 for the full SDK surface).
struct ANNIndexContext
{
    StorageID source_table_id = StorageID::createEmpty();
    Names indexed_columns;
    String family;
    String impl;
    ContextPtr query_context;
};

}
