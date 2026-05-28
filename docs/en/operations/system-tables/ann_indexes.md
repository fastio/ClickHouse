---
description: 'Deprecated compatibility system table containing metadata and runtime state for ANNIndex reflections.'
keywords: ['system table', 'ann_indexes']
sidebar_label: 'ann_indexes'
sidebar_position: 75
slug: /operations/system-tables/ann_indexes
title: 'system.ann_indexes'
doc_type: 'reference'
---

# system.ann_indexes {#system-ann-indexes}

## Description {#description}

Deprecated compatibility table. Lists every `ANNIndex` reflection visible to the current user with its static metadata, on-disk counters for active reflection parts, and scheduler observability fields.

Some columns are reserved placeholders and are always `NULL` until the engine exposes lifecycle state, coverage ratio, or creation timestamps (`state`, `coverage_ratio`, `creation_time`, `last_refresh_time`).

Use [`system.reflections`](/operations/system-tables/reflections) for new queries. See [`ANNIndex`](/engines/table-engines/mergetree-family/ann-index) for the engine-level description, [`system.reflection_parts`](/operations/system-tables/reflection_parts) for per-part detail, and [`CREATE REFLECTION`](/sql-reference/statements/create/reflection) for the DDL surface.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database the reflection lives in.
- `name` ([String](/sql-reference/data-types/string)) — Reflection name.
- `uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the reflection (populated for Atomic databases).
- `source_database` ([String](/sql-reference/data-types/string)) — Database of the source table.
- `source_table` ([String](/sql-reference/data-types/string)) — Name of the source table.
- `family` ([String](/sql-reference/data-types/string)) — Algorithm family declared in the `TYPE` clause (for example, `ann`).
- `impl` ([String](/sql-reference/data-types/string)) — Algorithm implementation declared in the `TYPE` clause (for example, `diskann`).
- `engine` ([String](/sql-reference/data-types/string)) — Storage engine backing the reflection.
- `state` (Nullable([String](/sql-reference/data-types/string))) — Lifecycle state of the reflection. Placeholder (`NULL`) until the engine reports a real state.
- `coverage_ratio` (Nullable([Float64](/sql-reference/data-types/float))) — Fraction of source rows covered by the reflection. Placeholder (`NULL`) until coverage tracking is wired into this column.
- `ann_index_part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of active ann-index parts persisted for the index.
- `total_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of rows across active ann-index parts.
- `total_bytes_on_disk` ([UInt64](/sql-reference/data-types/int-uint)) — On-disk footprint of active ann-index parts in bytes.
- `consecutive_remap_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of consecutive Remap cycles since the last Build (starvation-protection counter).
- `backlog_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Rows in uncovered source parts waiting for a BuildBatch.
- `backlog_bytes` ([UInt64](/sql-reference/data-types/int-uint)) — Bytes in uncovered source parts waiting for a BuildBatch.
- `backlog_parts` ([UInt64](/sql-reference/data-types/int-uint)) — Number of uncovered source parts waiting for a BuildBatch.
- `pending_task_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of background tasks currently reserved or running.
- `ready_ann_index_part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of ready ann-index parts in scheduler state.
- `repeated_failure_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of task keys currently in retry backoff or quarantine.
- `tombstone_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Total tombstone locator rows across active ann-index parts.
- `tombstone_ratio` ([Float64](/sql-reference/data-types/float)) — Ratio of tombstone locator rows to rows across active ann-index parts.
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
    ann_index_part_count,
    total_rows,
    backlog_parts,
    pending_task_count
FROM system.ann_indexes
WHERE database = currentDatabase()
ORDER BY name;
```

On a newly created index with no built parts yet, placeholder columns are `NULL` while counters are zero:

```sql
SELECT state, coverage_ratio, creation_time
FROM system.ann_indexes
WHERE database = currentDatabase() AND name = 'vectors_mi';
```

```text
┌─state─┬─coverage_ratio─┬─creation_time─┐
│ ᴺᵁᴸᴸ  │           ᴺᵁᴸᴸ │          ᴺᵁᴸᴸ │
└───────┴────────────────┴───────────────┘
```

## See also {#see-also}

- [`ANNIndex`](/engines/table-engines/mergetree-family/ann-index)
- [`system.reflections`](/operations/system-tables/reflections)
- [`CREATE REFLECTION`](/sql-reference/statements/create/reflection)
