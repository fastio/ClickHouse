---
description: 'Documentation for CREATE / DROP / RENAME / DETACH / ATTACH REFLECTION and the related SYSTEM subcommands'
sidebar_label: 'REFLECTION'
sidebar_position: 44
slug: /sql-reference/statements/create/reflection
title: 'CREATE REFLECTION'
doc_type: 'reference'
---

# CREATE REFLECTION {#create-reflection}

A `REFLECTION` is a catalog object backed by a `MergeTree`-family engine that precomputes auxiliary structures over a source table. The query engine does not expose a reflection to `SELECT` or `INSERT` directly; matching queries can consume it through optimizer rewrites.

This page covers the full DDL surface shipped in the current release. See [`ANNIndex`](/engines/table-engines/mergetree-family/ann-index) for the engine-level description and [`system.reflections`](/operations/system-tables/reflections) for the inspection schema.

## Syntax {#syntax}

```sql
CREATE REFLECTION [IF NOT EXISTS] [db.]name [UUID 'uuid']
ON [source_db.]source_table (indexed_column [, ...])
ENGINE = { ANNIndex(algorithm) | ReplicatedANNIndex(algorithm, zk_path, replica) }
[SETTINGS name = value [, ...]]
[COMMENT 'text']
```

- `name` — identifier of the reflection. Must be unique inside the target database.
- `source_table` — source table the index is built on. Must already exist and must be in the `MergeTree` family.
- `indexed_column` — one or more columns of the source table to feed into the algorithm. Each algorithm family validates its own shape; for `ann('diskann')`, a single `Array(Float32)` column is required.
- `ENGINE` — mandatory. Must match the replication flavour of the source table: `ANNIndex(algorithm)` for a plain source, `ReplicatedANNIndex(algorithm, ...)` for a replicated source.
- `SETTINGS` — mandatory for `ann_metric` and `ann_dimension`. Settings with a `diskann_` prefix can only be used with `ANNIndex(diskann)`, and settings with a `spann_` prefix can only be used with `ANNIndex(spann)`.

## Examples {#examples}

### Plain source and plain reflection {#plain-source-and-plain-reflection}

```sql
CREATE TABLE vectors
(
    id   UInt64,
    body String,
    vec  Array(Float32),
)
ENGINE = MergeTree
ORDER BY id
SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, enable_block_offset_column = 1;

CREATE REFLECTION vectors_mi
ON vectors (vec)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 128
COMMENT 'candidate vectors for query rewrite';
```

### Replicated source and replicated reflection {#replicated-source-and-replicated-reflection}

```sql
CREATE REFLECTION vectors_mi
ON vectors (vec)
ENGINE = ReplicatedANNIndex(diskann, '/clickhouse/tables/{uuid}/{shard}', '{replica}')
SETTINGS ann_metric = 'L2', ann_dimension = 128;
```

## ANN implementations {#ann-implementations}

The `ann` family can use different backend implementations.

### `diskann` {#ann-diskann}

`diskann` is the default disk-resident ANN backend. It supports `Array(Float32)` vectors with `L2Distance` or `cosineDistance` queries.

```sql
CREATE REFLECTION vectors_diskann
ON vectors (vec)
ENGINE = ANNIndex(diskann)
SETTINGS ann_metric = 'L2', ann_dimension = 128;
```

### `spann` {#ann-spann}

`spann` uses Microsoft SPTAG's SPANN backend. It supports the same `Array(Float32)` indexed column shape and `L2Distance` / `cosineDistance` query pattern as `diskann`.

```sql
CREATE REFLECTION vectors_spann
ON vectors (vec)
ENGINE = ANNIndex(spann)
SETTINGS ann_metric = 'L2', ann_dimension = 128;
```

The `spann` backend is available only in **Linux x86_64** server binaries built with **`USE_SPTAG`** (the Microsoft SPTAG dependency). It is **not** shipped on other platforms (for example **ARM64** / Apple Silicon CI images) and is skipped by functional tests tagged `use-sptag` and `no-cpu-aarch64`. It builds indexes from immutable source parts, does not support incremental `AddIndex` / `DeleteIndex`, and requires a local filesystem path for its `algorithm_private_spann` files. During build it keeps the indexed vectors in memory, so large parts require memory proportional to `rows * dim * sizeof(Float32)` plus SPTAG build overhead.

## Replication settings {#replication-settings}

`ReplicatedANNIndex` uses the inner `ReplicatedMergeTree` table to distribute committed reflection parts. The following settings control which replicas execute ANN background work:

- `ann_build_all_replicas` — default `false`. When `false`, one replica builds and the others fetch the committed part.
- `ann_remap_all_replicas` — default `true`. Remap is deterministic and usually cheaper to run on every replica than to fetch.
- `ann_compact_all_replicas` — always `false`. Setting it to `true` is rejected because compact rewrites the ready ANN part set.

## Prerequisites {#prerequisites}

Every `CREATE REFLECTION` is validated against the checks documented in [`ANNIndex` / Prerequisites](/engines/table-engines/mergetree-family/ann-index#prerequisites). Violations produce `UNKNOWN_TABLE`, `BAD_ARGUMENTS`, or `INCORRECT_QUERY` errors with a remediation hint.

## Constraints {#constraints}

- `SELECT` from a reflection is rejected with `NOT_IMPLEMENTED`.
- `INSERT` into a reflection is rejected with `NOT_IMPLEMENTED`.
- `ALTER TABLE ... DROP PARTITION` and `ALTER TABLE ... DROP PART` are supported on the reflection itself. They remove reflection parts only and do not modify source-table data. Missing coverage can be rebuilt by background reflection builds.
- `ATTACH PARTITION` / `REPLACE PARTITION` / `MOVE PARTITION` are rejected with `NOT_IMPLEMENTED`.
- A `MergeTree`-family source must have `assign_part_uuids = 1`, `enable_block_number_column = 1`, and `enable_block_offset_column = 1`.

## Dependency Model {#dependency-model}

A reflection records its source table as a referential dependency. This lets the optimizer find candidate reflections for queries over the source table, and prevents accidental source-table removal while a reflection still depends on it.

While a reflection exists:

- `DROP TABLE source` and `RENAME TABLE source TO ...` are rejected with `HAVE_DEPENDENT_OBJECTS`, unless `check_referential_table_dependencies = 0` is set.
- `ALTER TABLE source DROP COLUMN indexed_column`, `ALTER TABLE source RENAME COLUMN indexed_column TO ...`, and semantic type/default changes of an indexed column are rejected with `ALTER_OF_COLUMN_IS_FORBIDDEN`.
- Metadata-only changes that do not change the indexed column values, such as `COMMENT COLUMN`, remain allowed.

A reflection is not a materialized view dependency: inserts into the source table are not pushed into the reflection through the `MATERIALIZED VIEW` insert pipeline. Background reflection tasks reconcile source parts independently.

## ALTER REFLECTION {#alter-reflection}

`ALTER REFLECTION` is not part of the current DDL surface. Drop and recreate the reflection to change its definition.

## DROP REFLECTION {#drop-reflection}

```sql
DROP REFLECTION [IF EXISTS] [db.]name [SYNC]
```

`SYNC` waits for storage shutdown before returning, matching the semantics of `DROP TABLE ... SYNC`. Attempting to drop a non-reflection table with this form fails with `INCORRECT_QUERY`.

## RENAME, DETACH, ATTACH {#rename-detach-attach}

A reflection is a catalog table object, so it participates in the regular `RENAME TABLE`, `DETACH TABLE`, and `ATTACH TABLE` flows:

```sql
RENAME TABLE vectors_mi TO vectors_mi_v2;
DETACH TABLE vectors_mi_v2;
ATTACH TABLE vectors_mi_v2;
```

The optional qualifier `RENAME REFLECTION ...` is accepted for parity with `DROP REFLECTION`; it rewrites to the bare form above.

## DESCRIBE and SHOW CREATE {#describe-and-show-create}

```sql
DESCRIBE REFLECTION [db.]name;
SHOW CREATE TABLE [db.]name;
```

`DESCRIBE REFLECTION` accepts the `REFLECTION` qualifier and returns the same output as its bare counterpart. Use `SHOW CREATE TABLE` to inspect the persisted `CREATE REFLECTION` metadata.

## SYSTEM subcommands {#system-subcommands}

```sql
SYSTEM REFRESH REFLECTION [db.]name;
SYSTEM START REFLECTION BUILDS [db.]name;
SYSTEM STOP REFLECTION BUILDS [db.]name;
SYSTEM START REFLECTION REMAPS [db.]name;
SYSTEM STOP REFLECTION REMAPS [db.]name;
SYSTEM SYNC REFLECTION [db.]name;
```

All six subcommands parse and dispatch cleanly in this release; the background pipelines they drive come online in a later release and the current implementation only records operator intent in the server log.

## Permissions {#permissions}

`CREATE REFLECTION` requires `CREATE REFLECTION` on the reflection name and `SELECT` on the source table. `DROP REFLECTION` requires `DROP REFLECTION`, and `SYSTEM ... REFLECTION` subcommands require `SYSTEM REFLECTIONS`.
