---
description: 'ANNIndex reflections are standalone catalog objects backed by a MergeTree-family engine that precompute auxiliary structures over a source table.'
sidebar_label: 'ANNIndex'
sidebar_position: 45
slug: /engines/table-engines/mergetree-family/ann-index
title: 'ANNIndex'
doc_type: 'reference'
---

# ANNIndex {#ann-index}

`ANNIndex` is a reflection engine that precomputes an auxiliary structure (for example, an approximate nearest-neighbour graph) over a source `MergeTree`-family table. Unlike `MATERIALIZED VIEW` or skip indexes, a reflection is an independent `MergeTree`-backed entity that tracks its source by `(block_number, block_offset)`. Queries do not read or write a reflection directly; the query optimizer can rewrite matching sub-plans to consume it.

This release ships the DDL surface, catalog integration, background build/reconcile paths, and optimizer integration for matching vector-search queries.

:::warning Backward-incompatible change
Introducing a reflection writes new syntax into the metadata `.sql` file. Older ClickHouse versions that do not recognize `REFLECTION` cannot start against a data directory that contains one. Drop every reflection before rolling back:

```sql
DROP REFLECTION IF EXISTS <name> SYNC;
```
:::

## Prerequisites {#prerequisites}

At `CREATE` time the server enforces the following checks. Every violation surfaces as an explicit error with a remediation hint.

1. The source table exists.
2. The source engine is in the `MergeTree` family.
3. The source has `enable_block_number_column = 1`.
4. The source has `enable_block_offset_column = 1`.
5. The source has `assign_part_uuids = 1`.
6. An `ANNIndex` or `ReplicatedANNIndex` engine is present.
7. The declared algorithm family is registered.
8. The declared implementation is supported by the family.
9. The algorithm accepts the indexed expression (column list).
10. The reflection name is free in the target database.

The caller must additionally hold `SELECT` on the source table. Replication must match: a `Replicated*` source requires `ENGINE = ReplicatedANNIndex` on the reflection, and a plain `MergeTree`-family source requires `ENGINE = ANNIndex`. The `allow_ann_index_engine_mismatch` setting is reserved for recovery scenarios only.

## Lifecycle {#lifecycle}

Background tasks reconcile source parts against active reflection parts and advance coverage over time. Inspect progress with [`system.reflections`](/operations/system-tables/reflections) (reflection-level counters and scheduler fields) and [`system.reflection_parts`](/operations/system-tables/reflection_parts) (per-part detail).

The `state` column in `system.reflections` is not populated yet (`NULL`). A future release may expose explicit lifecycle values such as `Initialized`, `Building`, and `Active`.

## Part metadata {#part-metadata}

Each active `ANNIndex` part stores framework-owned metadata alongside algorithm-private files. New parts include:

- `ann_format.json` — framework format version, engine name, algorithm family and implementation, algorithm data version, parameter fingerprint, and build timestamp.
- `ann_coverage.json` — source parts covered by the ANN part and their row counts.
- Algorithm-private files returned by the registered implementation through `getAlgorithmPrivatePaths`.

The compatibility files `header.json` and `coverage.json` are still written for the current lookup and remap path.

## Dependency Model {#dependency-model}

`ANNIndex` registers its source table as a referential dependency. This relationship is used both by the query optimizer, which searches for reflections dependent on a source table, and by DDL guards.

The dependency has the following effects:

- Dropping or renaming the source table is rejected while a reflection depends on it, unless referential dependency checks are disabled with `check_referential_table_dependencies = 0`.
- Dropping, renaming, or semantically changing an indexed source column is rejected to prevent the reflection metadata and data from becoming invalid.
- Inserts into the source table are not pushed into the reflection through the `MATERIALIZED VIEW` pipeline; reflection background tasks reconcile source parts independently.

## Comparison with related features {#comparison-with-related-features}

| Feature | Storage | Query surface | Refresh | Typical use |
|---|---|---|---|---|
| `MATERIALIZED VIEW` | User-defined target table | `SELECT` from view | `INSERT` trigger on source | Aggregations, denormalization |
| Skip index (`ADD INDEX`) | Embedded in source parts | Transparent pruning | Maintained with merges | Granule-level pruning |
| `ANNIndex` | Independent `MergeTree` table | Transparent optimizer rewrite | Background build over `(block_number, block_offset)` | Approximate search, secondary structures |

## Algorithm availability {#algorithm-availability}

| `TYPE` implementation | Platforms |
|---|---|
| `ann('diskann', ...)` | All platforms where ClickHouse ships DiskANN support in the build. |
| `ann('spann', ...)` | **Linux x86_64** only, when the server is built with **`USE_SPTAG`**. Not available on ARM64 or other architectures; functional tests use the `use-sptag` and `no-cpu-aarch64` tags. |

See [`CREATE REFLECTION`](/sql-reference/statements/create/reflection) for parameter details.

## Reference {#reference}

See [`CREATE REFLECTION`](/sql-reference/statements/create/reflection) for the DDL surface, [`system.reflections`](/operations/system-tables/reflections) and [`system.reflection_parts`](/operations/system-tables/reflection_parts) for inspection.
