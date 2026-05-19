---
description: 'System table containing metadata and runtime state for every MATERIALIZED INDEX in all tables.'
keywords: ['system table', 'materialized_indexes']
sidebar_label: 'materialized_indexes'
sidebar_position: 75
slug: /operations/system-tables/materialized_indexes
title: 'system.materialized_indexes'
doc_type: 'reference'
---

# system.materialized_indexes {#system-materialized-indexes}

## Description {#description}

Lists every `MATERIALIZED INDEX` object visible to the current user with its static metadata, on-disk counters for active materialized-index-parts, and scheduler observability fields.

Some columns are reserved placeholders and are always `NULL` until the engine exposes lifecycle state, coverage ratio, or creation timestamps (`state`, `coverage_ratio`, `creation_time`, `last_refresh_time`).

See [`MaterializedIndex`](/engines/table-engines/mergetree-family/materialized-index) for the engine-level description, [`system.materialized_index_parts`](/operations/system-tables/materialized_index_parts) for per-part detail, and [`CREATE MATERIALIZED INDEX`](/sql-reference/statements/create/materialized-index) for the DDL surface.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database the materialized index lives in.
- `name` ([String](/sql-reference/data-types/string)) — Materialized-index name.
- `uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the materialized index (populated for Atomic databases).
- `source_database` ([String](/sql-reference/data-types/string)) — Database of the source table.
- `source_table` ([String](/sql-reference/data-types/string)) — Name of the source table.
- `family` ([String](/sql-reference/data-types/string)) — Algorithm family declared in the `TYPE` clause (for example, `ann`).
- `impl` ([String](/sql-reference/data-types/string)) — Algorithm implementation declared in the `TYPE` clause (for example, `diskann`).
- `engine` ([String](/sql-reference/data-types/string)) — Storage engine backing the index (`MaterializedIndex` or `ReplicatedMaterializedIndex`).
- `state` (Nullable([String](/sql-reference/data-types/string))) — Lifecycle state of the index. Placeholder (`NULL`) until the engine reports a real state.
- `coverage_ratio` (Nullable([Float64](/sql-reference/data-types/float))) — Fraction of source rows covered by the index. Placeholder (`NULL`) until coverage tracking is wired into this column.
- `materialized_index_part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of active materialized-index-parts persisted for the index.
- `total_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of rows across active materialized-index-parts.
- `total_bytes_on_disk` ([UInt64](/sql-reference/data-types/int-uint)) — On-disk footprint of active materialized-index-parts in bytes.
- `consecutive_remap_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of consecutive Remap cycles since the last Build (starvation-protection counter).
- `backlog_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Rows in uncovered source parts waiting for a BuildBatch.
- `backlog_bytes` ([UInt64](/sql-reference/data-types/int-uint)) — Bytes in uncovered source parts waiting for a BuildBatch.
- `backlog_parts` ([UInt64](/sql-reference/data-types/int-uint)) — Number of uncovered source parts waiting for a BuildBatch.
- `pending_task_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of background tasks currently reserved or running.
- `ready_materialized_index_part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of ready materialized-index-parts in scheduler state.
- `repeated_failure_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of task keys currently in retry backoff or quarantine.
- `tombstone_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Total tombstone locator rows across active materialized-index-parts.
- `tombstone_ratio` ([Float64](/sql-reference/data-types/float)) — Ratio of tombstone locator rows to rows across active materialized-index-parts.
- `retry_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of consecutive resource/backoff postponements.
- `next_retry_time` (Nullable([DateTime](/sql-reference/data-types/datetime))) — Next time a postponed task may be retried. `NULL` when no backoff is active.
- `last_error` ([String](/sql-reference/data-types/string)) — Last scheduler resource/backoff reason (empty when none).
- `comment` ([String](/sql-reference/data-types/string)) — User-provided comment from `CREATE ... COMMENT '...'`.
- `creation_time` (Nullable([DateTime](/sql-reference/data-types/datetime))) — When the index was created. Placeholder (`NULL`) until metadata exposes a creation timestamp.
- `last_refresh_time` (Nullable([DateTime](/sql-reference/data-types/datetime))) — Last successful refresh of the index. Placeholder (`NULL`) until the refresh pipeline reports it.

## Example {#example}

```sql
SELECT
    name,
    state,
    coverage_ratio,
    materialized_index_part_count,
    total_rows,
    backlog_parts,
    pending_task_count
FROM system.materialized_indexes
WHERE database = currentDatabase()
ORDER BY name;
```

On a newly created index with no built parts yet, placeholder columns are `NULL` while counters are zero:

```sql
SELECT state, coverage_ratio, creation_time
FROM system.materialized_indexes
WHERE database = currentDatabase() AND name = 'vectors_mi';
```

```text
┌─state─┬─coverage_ratio─┬─creation_time─┐
│ ᴺᵁᴸᴸ  │           ᴺᵁᴸᴸ │          ᴺᵁᴸᴸ │
└───────┴────────────────┴───────────────┘
```

## See also {#see-also}

- [`MaterializedIndex`](/engines/table-engines/mergetree-family/materialized-index)
- [`system.materialized_index_parts`](/operations/system-tables/materialized_index_parts)
- [`CREATE MATERIALIZED INDEX`](/sql-reference/statements/create/materialized-index)
