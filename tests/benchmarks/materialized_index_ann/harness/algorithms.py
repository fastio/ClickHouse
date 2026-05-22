"""
Algorithm registry for the ANN benchmark harness.

An `AlgoSpec` captures everything that differs between MaterializedIndex
algorithms (DiskANN, SPANN, …):

  * default build parameters (the kwargs passed to `ann(...)` in DDL)
  * the `ProfileEvent` to assert fired during the search phase (proves the
    optimizer routed through the right algorithm, not a brute-force fallback)
  * default per-query session settings exposed by the algorithm (today only
    DiskANN exposes any: search_list_size, search_beam_width)

`build_create_sql(...)` renders the DDL given the merged build params.
Per-algorithm parameter type rules (float vs int vs string) live in
`_render_param` so the DDL is well-typed regardless of where the value
came from (preset JSON, CLI override, code default).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict


_FLOAT_PARAM_NAMES = {"alpha", "head_ratio", "build_ram_limit_gb"}
_STRING_PARAM_NAMES = {"metric"}


def _render_param(name: str, value) -> str:
    if name in _STRING_PARAM_NAMES:
        return f"{name} = '{value}'"
    if name in _FLOAT_PARAM_NAMES:
        return f"{name} = {float(value)}"
    return f"{name} = {int(value)}"


@dataclass
class AlgoSpec:
    name: str                                       # 'diskann' | 'spann'
    family: str                                     # 'ann'
    default_build_params: Dict[str, object]
    search_profile_event: str
    default_search_settings: Dict[str, object] = field(default_factory=dict)

    def build_create_sql(self, index_name: str, source_table: str, indexed_column: str,
                         build_params: Dict[str, object], sync_timeout_sec: int) -> str:
        kv = ",\n    ".join(_render_param(k, v) for k, v in build_params.items())
        return f"""
SET allow_experimental_materialized_index = 1;
DROP TABLE IF EXISTS {index_name} SYNC;
CREATE MATERIALIZED INDEX {index_name}
ON {source_table} ({indexed_column})
TYPE {self.family}('{self.name}',
    {kv})
ENGINE = MaterializedIndex
SETTINGS materialized_index_sync_timeout = {sync_timeout_sec};
"""


DISKANN = AlgoSpec(
    name="diskann",
    family="ann",
    default_build_params={
        "metric": "L2",
        "dim": 128,
        "pq_chunks": 32,
        "pruned_degree": 64,
        "max_degree": 96,
        "l_build": 128,
        "alpha": 1.2,
        "num_threads": 16,
    },
    search_profile_event="MaterializedIndexDiskANNSearchStarted",
    default_search_settings={
        "materialized_index_diskann_search_list_size": 200,
        "materialized_index_diskann_search_beam_width": 16,
    },
)


SPANN = AlgoSpec(
    name="spann",
    family="ann",
    default_build_params={
        "metric": "L2",
        "dim": 128,
        "num_threads": 16,
    },
    search_profile_event="MaterializedIndexSPANNSearchStarted",
    # Per-query SPTAG runtime overrides — exposed as session settings via
    # `materialized_index_spann_search_*` (the SPTAG INI keys that are
    # re-applied after LoadIndex; see SPANNFacade.cpp). `0` means "fall back
    # to whatever the CREATE DDL baked into the index". Build-only params
    # (head_ratio, posting_page_limit, posting_vector_limit, replica_count,
    # num_threads, io_threads) require DROP/CREATE to change.
    default_search_settings={
        "materialized_index_spann_search_posting_page_limit": 0,
        "materialized_index_spann_search_internal_result_num": 0,
        "materialized_index_spann_search_max_check": 0,
        "materialized_index_spann_search_max_dist_ratio": 0,
        "materialized_index_spann_search_hash_table_exponent": 0,
    },
)


ALGOS: Dict[str, AlgoSpec] = {
    DISKANN.name: DISKANN,
    SPANN.name: SPANN,
}
