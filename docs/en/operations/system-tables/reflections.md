---
description: 'System table containing metadata and runtime state for every reflection.'
keywords: ['system table', 'reflections']
sidebar_label: 'reflections'
sidebar_position: 76
slug: /operations/system-tables/reflections
title: 'system.reflections'
doc_type: 'reference'
---

# system.reflections {#system-reflections}

## Description {#description}

Lists every `REFLECTION` object visible to the current user with its source table, engine identity, internal storage, and scheduler observability fields.

`ANNIndex` reflections are also exposed through the compatibility table [`system.ann_indexes`](/operations/system-tables/ann_indexes).

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database the reflection lives in.
- `name` ([String](/sql-reference/data-types/string)) — Reflection name.
- `uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the reflection.
- `source_database` ([String](/sql-reference/data-types/string)) — Database of the source table.
- `source_table` ([String](/sql-reference/data-types/string)) — Name of the source table.
- `family` ([String](/sql-reference/data-types/string)) — Reflection engine family, for example `ann`.
- `impl` ([String](/sql-reference/data-types/string)) — Reflection engine implementation, for example `diskann`.
- `engine` ([String](/sql-reference/data-types/string)) — Storage engine backing the reflection.
- `internal_storage` ([String](/sql-reference/data-types/string)) — Internal storage table name, when exposed by the engine.
- `state` (Nullable([String](/sql-reference/data-types/string))) — Lifecycle state. Placeholder (`NULL`) until an engine reports a framework state.
- `coverage_ratio` (Nullable([Float64](/sql-reference/data-types/float))) — Fraction of source rows covered by the reflection.
- `part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of active reflection parts.
- `pending_task_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of background tasks currently reserved or running.
- `backlog_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Rows waiting for background reflection work.
- `retry_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of consecutive resource/backoff postponements.
- `next_retry_time` (Nullable([DateTime](/sql-reference/data-types/datetime))) — Next time a postponed task may be retried.
- `last_error` ([String](/sql-reference/data-types/string)) — Last scheduler error or backoff reason.

## Example {#example}

```sql
SELECT
    name,
    family,
    impl,
    source_table,
    part_count,
    pending_task_count
FROM system.reflections
WHERE database = currentDatabase()
ORDER BY name;
```

## See also {#see-also}

- [`system.reflection_parts`](/operations/system-tables/reflection_parts)
- [`system.ann_indexes`](/operations/system-tables/ann_indexes)
