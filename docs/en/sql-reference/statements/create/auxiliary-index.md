---
description: 'Documentation for CREATE / ALTER / DROP / RENAME / DETACH / ATTACH MATERIALIZED INDEX and the related SYSTEM subcommands'
sidebar_label: 'MATERIALIZED INDEX'
sidebar_position: 44
slug: /sql-reference/statements/create/materialized-index
title: 'CREATE MATERIALIZED INDEX'
doc_type: 'reference'
---

# CREATE MATERIALIZED INDEX {#create-materialized-index}

A `MATERIALIZED INDEX` is a catalog object backed by a `MergeTree`-family engine that precomputes auxiliary structures over a source table. The query engine does not expose a materialized index to `SELECT` or `INSERT` directly; matching queries can consume it through optimizer rewrites.

This page covers the full DDL surface shipped in the current release. See [`MaterializedIndex`](/engines/table-engines/mergetree-family/materialized-index) for the engine-level description and [`system.materialized_indexes`](/operations/system-tables/materialized_indexes) for the inspection schema.

## Syntax {#syntax}

```sql
CREATE MATERIALIZED INDEX [IF NOT EXISTS] [db.]name [UUID 'uuid']
ON [source_db.]source_table (indexed_column [, ...])
TYPE family('impl'[, build_param = value [, ...]])
ENGINE = { MaterializedIndex | ReplicatedMaterializedIndex[(zk_path, replica)] }
[SETTINGS name = value [, ...]]
[COMMENT 'text']
```

- `name` — identifier of the materialized index. Must be unique inside the target database.
- `source_table` — source table the index is built on. Must already exist and must be in the `MergeTree` family.
- `indexed_column` — one or more columns of the source table to feed into the algorithm. Each algorithm family validates its own shape; for `ann('diskann')`, a single `Array(Float32)` column is required.
- `TYPE family('impl'[, ...])` — mandatory. `family` names the registered algorithm family (for example, `ann`); `impl` names the concrete implementation (for example, `diskann`).
- `ENGINE` — mandatory. Must match the replication flavour of the source table: `MaterializedIndex` for a plain source, `ReplicatedMaterializedIndex(...)` for a replicated source.

## Examples {#examples}

### Plain source and plain index {#plain-source-and-plain-index}

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

CREATE MATERIALIZED INDEX vectors_mi
ON vectors (vec)
TYPE ann('diskann', metric = 'L2', dim = 128)
ENGINE = MaterializedIndex
COMMENT 'candidate vectors for query rewrite';
```

### Replicated source and replicated index {#replicated-source-and-replicated-index}

```sql
CREATE MATERIALIZED INDEX vectors_mi
ON vectors (vec)
TYPE ann('diskann', metric = 'L2', dim = 128)
ENGINE = ReplicatedMaterializedIndex('/clickhouse/tables/{uuid}/{shard}', '{replica}');
```

## ANN implementations {#ann-implementations}

The `ann` family can use different backend implementations.

### `diskann` {#ann-diskann}

`diskann` is the default disk-resident ANN backend. It supports `Array(Float32)` vectors with `L2Distance` or `cosineDistance` queries.

```sql
CREATE MATERIALIZED INDEX vectors_diskann
ON vectors (vec)
TYPE ann('diskann', metric = 'L2', dim = 128)
ENGINE = MaterializedIndex;
```

### `spann` {#ann-spann}

`spann` uses Microsoft SPTAG's SPANN backend. It supports the same `Array(Float32)` indexed column shape and `L2Distance` / `cosineDistance` query pattern as `diskann`.

```sql
CREATE MATERIALIZED INDEX vectors_spann
ON vectors (vec)
TYPE ann('spann', metric = 'L2', dim = 128)
ENGINE = MaterializedIndex;
```

The `spann` backend is available only in **Linux x86_64** server binaries built with **`USE_SPTAG`** (the Microsoft SPTAG dependency). It is **not** shipped on other platforms (for example **ARM64** / Apple Silicon CI images) and is skipped by functional tests tagged `use-sptag` and `no-cpu-aarch64`. It builds indexes from immutable source parts, does not support incremental `AddIndex` / `DeleteIndex`, and requires a local filesystem path for its `algorithm_private_spann` files. During build it keeps the indexed vectors in memory, so large parts require memory proportional to `rows * dim * sizeof(Float32)` plus SPTAG build overhead.

## Prerequisites {#prerequisites}

Every `CREATE MATERIALIZED INDEX` is validated against the checks documented in [`MaterializedIndex` / Prerequisites](/engines/table-engines/mergetree-family/materialized-index#prerequisites). Violations produce `UNKNOWN_TABLE`, `BAD_ARGUMENTS`, or `INCORRECT_QUERY` errors with a remediation hint.

## Constraints {#constraints}

- `SELECT` from a materialized index is rejected with `NOT_IMPLEMENTED`.
- `INSERT` into a materialized index is rejected with `NOT_IMPLEMENTED`.
- `ALTER TABLE ... DROP PARTITION` and `ALTER TABLE ... DROP PART` are supported on the materialized index itself. They remove materialized-index parts only and do not modify source-table data. Missing index coverage can be rebuilt by background materialized-index builds.
- `ATTACH PARTITION` / `REPLACE PARTITION` / `MOVE PARTITION` are rejected with `NOT_IMPLEMENTED`.
- A `MergeTree`-family source must have `assign_part_uuids = 1`, `enable_block_number_column = 1`, and `enable_block_offset_column = 1`.

## Dependency Model {#dependency-model}

A materialized index records its source table as a referential dependency. This lets the optimizer find candidate materialized indexes for queries over the source table, and prevents accidental source-table removal while an index still depends on it.

While a materialized index exists:

- `DROP TABLE source` and `RENAME TABLE source TO ...` are rejected with `HAVE_DEPENDENT_OBJECTS`, unless `check_referential_table_dependencies = 0` is set.
- `ALTER TABLE source DROP COLUMN indexed_column`, `ALTER TABLE source RENAME COLUMN indexed_column TO ...`, and semantic type/default changes of an indexed column are rejected with `ALTER_OF_COLUMN_IS_FORBIDDEN`.
- Metadata-only changes that do not change the indexed column values, such as `COMMENT COLUMN`, remain allowed.

A materialized index is not a materialized view dependency: inserts into the source table are not pushed into the index through the `MATERIALIZED VIEW` insert pipeline. Background materialized-index tasks reconcile source parts independently.

## ALTER MATERIALIZED INDEX {#alter-materialized-index}

```sql
ALTER MATERIALIZED INDEX [db.]name
    MODIFY TYPE family('impl'[, build_param = value [, ...]])
  | MODIFY SETTING name = value [, ...]
  | RESET SETTING name [, ...]
  | MODIFY COMMENT 'text'
```

The four `ALTER` subcommands parse and dispatch through the regular alter pipeline. The storage does not execute them in this release — they are reserved for future work.

## DROP MATERIALIZED INDEX {#drop-materialized-index}

```sql
DROP MATERIALIZED INDEX [IF EXISTS] [db.]name [SYNC]
```

`SYNC` waits for storage shutdown before returning, matching the semantics of `DROP TABLE ... SYNC`. Attempting to drop a non-materialized-index table with this form fails with `INCORRECT_QUERY`.

## RENAME, DETACH, ATTACH {#rename-detach-attach}

A materialized index is a catalog table object, so it participates in the regular `RENAME TABLE`, `DETACH TABLE`, and `ATTACH TABLE` flows:

```sql
RENAME TABLE vectors_mi TO vectors_mi_v2;
DETACH TABLE vectors_mi_v2;
ATTACH TABLE vectors_mi_v2;
```

The optional qualifier `RENAME MATERIALIZED INDEX ...` is accepted for parity with `DROP MATERIALIZED INDEX`; it rewrites to the bare form above.

## DESCRIBE and SHOW CREATE {#describe-and-show-create}

```sql
DESCRIBE MATERIALIZED INDEX [db.]name;
SHOW CREATE MATERIALIZED INDEX [db.]name;
```

Both forms accept the `MATERIALIZED INDEX` qualifier and return the same output as their bare counterparts.

## SYSTEM subcommands {#system-subcommands}

```sql
SYSTEM REFRESH MATERIALIZED INDEX [db.]name;
SYSTEM START MATERIALIZED INDEX BUILDS [db.]name;
SYSTEM STOP MATERIALIZED INDEX BUILDS [db.]name;
SYSTEM START MATERIALIZED INDEX REMAPS [db.]name;
SYSTEM STOP MATERIALIZED INDEX REMAPS [db.]name;
SYSTEM SYNC MATERIALIZED INDEX [db.]name;
```

All six subcommands parse and dispatch cleanly in this release; the background pipelines they drive come online in a later release and the current implementation only records operator intent in the server log.

## BACKUP {#backup}

```sql
BACKUP TABLE [db.]source_table TO Disk(...) WITH MATERIALIZED INDEXES;
```

The parser accepts the `WITH MATERIALIZED INDEXES` clause; the backup engine itself does not yet materialize indexes into a backup.

## Permissions {#permissions}

`CREATE MATERIALIZED INDEX` requires `SELECT` on the source table in addition to the table-level grants for the index name. The four `ALTER MATERIALIZED INDEX` subcommands each map to a dedicated access flag (`ALTER_MATERIALIZED_INDEX_MODIFY_TYPE`, `ALTER_MATERIALIZED_INDEX_MODIFY_SETTING`, `ALTER_MATERIALIZED_INDEX_RESET_SETTING`, `ALTER_MATERIALIZED_INDEX_MODIFY_COMMENT`) under the umbrella `ALTER_MATERIALIZED_INDEX` group.
