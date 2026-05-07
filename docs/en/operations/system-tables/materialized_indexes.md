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

Lists every `MATERIALIZED INDEX` object visible to the current user with its static metadata and lifecycle state.

Several columns are currently placeholders populated with a constant value (`0`, `NULL`, or `'Initialized'`). They become meaningful once the background build, refresh, and replication pipelines land in a subsequent release; the schema is fixed now so that user queries written today continue to work.

See [`MaterializedIndex`](/engines/table-engines/mergetree-family/materialized-index) for the engine-level description and [`CREATE MATERIALIZED INDEX`](/sql-reference/statements/create/materialized-index) for the DDL surface.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database the materialized index lives in.
- `name` ([String](/sql-reference/data-types/string)) — Materialized-index name.
- `uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the materialized index (populated for Atomic databases).
- `source_database` ([String](/sql-reference/data-types/string)) — Database of the source table.
- `source_table` ([String](/sql-reference/data-types/string)) — Name of the source table.
- `family` ([String](/sql-reference/data-types/string)) — Algorithm family declared in the `TYPE` clause (for example, `ann`).
- `impl` ([String](/sql-reference/data-types/string)) — Algorithm implementation declared in the `TYPE` clause (for example, `MockAnn`).
- `engine` ([String](/sql-reference/data-types/string)) — Storage engine backing the index (`MaterializedIndex` or `ReplicatedMaterializedIndex`).
- `state` ([String](/sql-reference/data-types/string)) — Lifecycle state of the index. Always `'Initialized'` in this release; populated with `'Building'` / `'Active'` in a later release.
- `coverage_ratio` ([Float64](/sql-reference/data-types/float)) — Fraction of source rows covered by the index. Placeholder (always `0`) until the build pipeline lands.
- `mi_part_count` ([UInt64](/sql-reference/data-types/int-uint)) — Number of parts persisted for the index. Placeholder (always `0`) until the build pipeline lands.
- `total_rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of rows in the index. Placeholder (always `0`) until the build pipeline lands.
- `total_bytes_on_disk` ([UInt64](/sql-reference/data-types/int-uint)) — On-disk footprint of the index in bytes. Placeholder (always `0`) until the build pipeline lands.
- `comment` ([String](/sql-reference/data-types/string)) — User-provided comment from `CREATE ... COMMENT '...'`.
- `creation_time` ([DateTime](/sql-reference/data-types/datetime)) — When the index was created. Placeholder (always epoch) until the build pipeline lands.
- `last_refresh_time` (Nullable([DateTime](/sql-reference/data-types/datetime))) — Last successful refresh of the index. Placeholder (always `NULL`) until the refresh pipeline lands.

## Example {#example}

```sql
SELECT database, name, family, impl, engine, state
FROM system.materialized_indexes
ORDER BY database, name;
```

```text
┌─database─┬─name───────┬─family─┬─impl────┬─engine────────────┬─state───────┐
│ default  │ vectors_mi │ ann    │ MockAnn │ MaterializedIndex │ Initialized │
└──────────┴────────────┴────────┴─────────┴───────────────────┴─────────────┘
```

## See also {#see-also}

- [`MaterializedIndex`](/engines/table-engines/mergetree-family/materialized-index)
- [`CREATE MATERIALIZED INDEX`](/sql-reference/statements/create/materialized-index)
