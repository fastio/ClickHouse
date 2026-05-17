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
        "materialized_index_diskann_search_beam_width": 4,
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
    # SPANN search params (search_posting_page_limit, internal_result_num,
    # max_check) are baked into the SPTAG index at CREATE time and are not
    # currently overridable per query.
    default_search_settings={},
)


ALGOS: Dict[str, AlgoSpec] = {
    DISKANN.name: DISKANN,
    SPANN.name: SPANN,
}
