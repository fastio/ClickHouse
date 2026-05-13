---
description: 'Materialized indexes are standalone catalog objects backed by a MergeTree-family engine that precompute auxiliary structures over a source table.'
sidebar_label: 'MaterializedIndex'
sidebar_position: 45
slug: /engines/table-engines/mergetree-family/materialized-index
title: 'MaterializedIndex'
doc_type: 'reference'
---

# MaterializedIndex {#materialized-index}

`MaterializedIndex` is a catalog object that precomputes an auxiliary structure (for example, an approximate nearest-neighbour graph) over a source `MergeTree`-family table. Unlike `MATERIALIZED VIEW` or skip indexes, a materialized index is an independent `MergeTree`-backed entity that tracks its source by `(block_number, block_offset)`. Queries do not read or write a materialized index directly; the query optimizer can rewrite matching sub-plans to consume it.

This release ships the DDL surface, catalog integration, background build/reconcile paths, and optimizer integration for matching vector-search queries.

:::warning Backward-incompatible change
Introducing a materialized index writes new syntax into the source table's metadata `.sql` file. Older ClickHouse versions that do not recognize `MATERIALIZED INDEX` cannot start against a data directory that contains one. Drop every materialized index before rolling back:

```sql
DROP MATERIALIZED INDEX IF EXISTS <name> SYNC;
```
:::

## Prerequisites {#prerequisites}

At `CREATE` time the server enforces the following checks. Every violation surfaces as an explicit error with a remediation hint.

1. The source table exists.
2. The source engine is in the `MergeTree` family.
3. The source has `enable_block_number_column = 1`.
4. The source has `enable_block_offset_column = 1`.
5. The source has `assign_part_uuids = 1`.
6. `TYPE <family>('<impl>')` is present.
7. The declared algorithm family is registered.
8. The declared implementation is supported by the family.
9. The algorithm accepts the indexed expression (column list).
10. The materialized-index name is free in the target database.

The caller must additionally hold `SELECT` on the source table. Replication must match: a `Replicated*` source requires `ENGINE = ReplicatedMaterializedIndex` on the index, and a plain `MergeTree`-family source requires `ENGINE = MaterializedIndex`. The `allow_materialized_index_engine_mismatch` setting is reserved for recovery scenarios only.

## Lifecycle {#lifecycle}

A materialized index advances through a small state machine that is persisted in [`system.materialized_indexes`](/operations/system-tables/materialized_indexes).

| State | Meaning |
|---|---|
| `Initialized` | The catalog entry exists; no parts have been built yet. |
| `Building` | A background pipeline is materializing parts for new source ranges. |
| `Active` | Query coverage is sufficient for the optimizer to rewrite matching plans onto the index. |

## Dependency Model {#dependency-model}

`MaterializedIndex` registers its source table as a referential dependency. This relationship is used both by the query optimizer, which searches for materialized indexes dependent on a source table, and by DDL guards.

The dependency has the following effects:

- Dropping or renaming the source table is rejected while a materialized index depends on it, unless referential dependency checks are disabled with `check_referential_table_dependencies = 0`.
- Dropping, renaming, or semantically changing an indexed source column is rejected to prevent the materialized index metadata and data from becoming invalid.
- Inserts into the source table are not pushed into the materialized index through the `MATERIALIZED VIEW` pipeline; materialized-index background tasks reconcile source parts independently.

## Comparison with related features {#comparison-with-related-features}

| Feature | Storage | Query surface | Refresh | Typical use |
|---|---|---|---|---|
| `MATERIALIZED VIEW` | User-defined target table | `SELECT` from view | `INSERT` trigger on source | Aggregations, denormalization |
| Skip index (`ADD INDEX`) | Embedded in source parts | Transparent pruning | Maintained with merges | Granule-level pruning |
| `MaterializedIndex` | Independent `MergeTree` table | Transparent optimizer rewrite | Background build over `(block_number, block_offset)` | Approximate search, secondary structures |

## Reference {#reference}

See [`CREATE MATERIALIZED INDEX`](/sql-reference/statements/create/materialized-index) for the DDL surface and [`system.materialized_indexes`](/operations/system-tables/materialized_indexes) for the inspection interface.
