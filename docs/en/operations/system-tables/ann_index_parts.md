---
description: 'Deprecated compatibility system table containing one row per active ANNIndex reflection part.'
keywords: ['system table', 'ann_index_parts']
sidebar_label: 'ann_index_parts'
sidebar_position: 74
slug: /operations/system-tables/ann_index_parts
title: 'system.ann_index_parts'
doc_type: 'reference'
---

# system.ann_index_parts {#system-ann-index-parts}

## Description {#description}

Deprecated compatibility table. Lists every active reflection part for each `ANNIndex` reflection visible to the current user. One row corresponds to one on-disk part in the inner `MergeTree` storage backing the reflection.

Provenance fields (`source_partition_id`, `source_min_block`, `source_max_block`, `tombstone_rows`) are read from `header.json` when present. Parts that predate metadata finalization may expose empty or zero provenance values.

Use [`system.reflection_parts`](/operations/system-tables/reflection_parts) for new queries. See [`system.reflections`](/operations/system-tables/reflections) for reflection-level aggregates and [`ANNIndex`](/engines/table-engines/mergetree-family/ann-index) for the engine overview.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database of the reflection that owns this part.
- `index_name` ([String](/sql-reference/data-types/string)) — Name of the reflection that owns this part.
- `part_name` ([String](/sql-reference/data-types/string)) — On-disk name of the reflection part.
- `part_uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the reflection part.
- `physical_partition_id` ([String](/sql-reference/data-types/string)) — Partition id encoded in the part name (stable hash of the source partition id via `MergeTreePartition::getID`).
- `source_partition_id` ([String](/sql-reference/data-types/string)) — Source-table partition id this part covers, read from `header.json`.
- `source_min_block` ([Int64](/sql-reference/data-types/int-uint)) — Lowest source-part block number covered by this part, read from `header.json`.
- `source_max_block` ([Int64](/sql-reference/data-types/int-uint)) — Highest source-part block number covered by this part, read from `header.json`.
- `min_block` ([Int64](/sql-reference/data-types/int-uint)) — Materialized-index-part `min_block` from the part name.
- `max_block` ([Int64](/sql-reference/data-types/int-uint)) — Materialized-index-part `max_block` from the part name.
- `level` ([UInt32](/sql-reference/data-types/int-uint)) — Materialized-index-part level from the part name.
- `rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of source rows indexed by this part.
- `tombstone_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of tombstone locator rows recorded in this part.
- `tombstone_ratio` ([Float64](/sql-reference/data-types/float)) — Ratio of `tombstone_rows` to `rows` in this part.
- `bytes_on_disk` ([UInt64](/sql-reference/data-types/int-uint)) — Disk footprint of the reflection part in bytes.
- `active` ([UInt8](/sql-reference/data-types/int-uint)) — Whether the part is in the `Active` state. This table currently exposes only active parts (`1`).

## Example {#example}

```sql
SELECT
    index_name,
    part_name,
    source_partition_id,
    rows,
    bytes_on_disk,
    active
FROM system.ann_index_parts
WHERE database = currentDatabase()
ORDER BY index_name, part_name;
```

After `SYSTEM SYNC REFLECTION`, each built part should appear with non-zero `rows` and `bytes_on_disk`:

```sql
SELECT
    count() AS part_count,
    sum(rows) AS total_rows,
    min(source_min_block) AS min_source_block,
    max(source_max_block) AS max_source_block
FROM system.ann_index_parts
WHERE database = currentDatabase() AND index_name = 'vectors_mi';
```

## See also {#see-also}

- [`system.reflection_parts`](/operations/system-tables/reflection_parts)
- [`ANNIndex`](/engines/table-engines/mergetree-family/ann-index)
- [`CREATE REFLECTION`](/sql-reference/statements/create/reflection)
