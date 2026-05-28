---
description: 'System table containing one row per active reflection part.'
keywords: ['system table', 'reflection_parts']
sidebar_label: 'reflection_parts'
sidebar_position: 77
slug: /operations/system-tables/reflection_parts
title: 'system.reflection_parts'
doc_type: 'reference'
---

# system.reflection_parts {#system-reflection-parts}

## Description {#description}

Lists every active reflection part visible to the current user. One row corresponds to one on-disk part in the internal storage backing a reflection.

`ANNIndex` reflection parts are also exposed through the compatibility table [`system.ann_index_parts`](/operations/system-tables/ann_index_parts), which includes ANN-specific provenance columns.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database of the reflection that owns this part.
- `reflection_name` ([String](/sql-reference/data-types/string)) — Name of the reflection that owns this part.
- `family` ([String](/sql-reference/data-types/string)) — Reflection engine family.
- `impl` ([String](/sql-reference/data-types/string)) — Reflection engine implementation.
- `part_name` ([String](/sql-reference/data-types/string)) — On-disk name of the reflection part.
- `part_uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the reflection part.
- `physical_partition_id` ([String](/sql-reference/data-types/string)) — Partition id encoded in the part name.
- `min_block` ([Int64](/sql-reference/data-types/int-uint)) — Reflection-part `min_block` from the part name.
- `max_block` ([Int64](/sql-reference/data-types/int-uint)) — Reflection-part `max_block` from the part name.
- `level` ([UInt32](/sql-reference/data-types/int-uint)) — Reflection-part level from the part name.
- `rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of rows represented by this reflection part.
- `bytes_on_disk` ([UInt64](/sql-reference/data-types/int-uint)) — Disk footprint of the reflection part in bytes.
- `active` ([UInt8](/sql-reference/data-types/int-uint)) — Whether the part is in the `Active` state. This table currently exposes only active parts (`1`).

## Example {#example}

```sql
SELECT
    reflection_name,
    part_name,
    rows,
    bytes_on_disk
FROM system.reflection_parts
WHERE database = currentDatabase()
ORDER BY reflection_name, part_name;
```

## See also {#see-also}

- [`system.reflections`](/operations/system-tables/reflections)
- [`system.ann_index_parts`](/operations/system-tables/ann_index_parts)
