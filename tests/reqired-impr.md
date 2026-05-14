# MaterializedIndex 关键模块实现分析与改进建议

## 总体结论

当前 `MaterializedIndex` 三个关键模块已经有清晰骨架，单机非复制场景下具备可用基础，但还不是完整生产级实现。

- 后台任务：核心设计有效，但失败处理、并发能力和复制能力不完整。
- 查询加速：核心改写路径有效，正确性较稳健，但支持的查询形态偏窄。
- DDL：`CREATE` / `DROP` 路径较完整，但 `ALTER`、部分 `SYSTEM` 命令、`BACKUP` 和 replicated 语义仍有明显占位。

| 模块 | 有效性 | 完整性 | 主要短板 |
|---|---:|---:|---|
| 后台任务 | 较有效 | 中等 | failure backoff 未接入、单任务瓶颈、replicated 未实现 |
| 查询加速 | 有效 | 中等偏低 | plan shape 太窄，`WHERE` / `PREWHERE` / 多键排序支持不足 |
| DDL | `CREATE` / `DROP` 有效 | 中等 | `ALTER`、`SYSTEM REFRESH` / `START` / `STOP`、`BACKUP`、replicated 多处占位 |

## 1. 后台任务

### 当前实现是否有效

有效。后台任务核心路径是：

- 启动时加载已有 MI parts 的 `coverage.json`，重建内存覆盖关系：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:221`
- 调度入口为 `StorageMaterializedIndex::scheduleDataProcessingJob`：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:659`
- 每轮取源表 parts 和 MI parts 快照：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:674`
- 通过 `SnapshotDiffReconciler` 计算需要 `Build`、`Remap`、`Compact` 的任务：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:703`
- 根据 backlog、starvation protection、compact 条件决策：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:704`
- 提交 `Build` / `Remap` / `Compact` 后台任务：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:733`

这个设计是合理的：用 source part UUID 和 MI part `coverage.json` 做覆盖关系，适合 ClickHouse `MergeTree` parts 生命周期。重启后也能从磁盘 active parts 重建状态：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:263`。

### 主要问题

#### 1. 每个 MI 实例同一时刻只能跑一个任务，吞吐受限

调度时明确要求当前无 building part、无 active task、无 resource backoff：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:685`

```cpp
if (!currently_building_materialized_index_parts.empty() || scheduler_state.hasActiveTasks() || scheduler_state.isResourceBackoffActive())
    return false;
```

这保证正确性，但大规模 backfill 或大量新 parts 追赶时会慢。

#### 2. 已有 per-task failure backoff 代码，但生产路径没有调用

`recordTaskFailure` 在 scheduler state 中存在：`src/Storages/MaterializedIndex/MaterializedIndexSchedulerState.h:417`

但实际 `src` 中只有测试调用，没有任务失败路径调用。也就是说，某个 part 一直失败时，可能反复调度，形成重试风暴。

#### 3. 资源失败退避是固定间隔，不是指数退避

`postponeForResourceFailure` 每次用固定 setting：`src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:378`

状态里只是 `now + backoff`：`src/Storages/MaterializedIndex/MaterializedIndexSchedulerState.h:397`

磁盘满、内存不足、输入超过限制时，固定重试会产生噪音。

#### 4. `uncovered_source_backlog` 读写没有完整锁保护

backlog 更新和选择发生在：

- `src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:466`
- `src/Storages/MaterializedIndex/StorageMaterializedIndex.cpp:518`

当前单任务调度下风险较低，但未来并行化会有数据竞争隐患。

#### 5. 复制版后台任务基本未实现

`ReplicatedMaterializedIndex` 的后台调度直接返回 `false`：`src/Storages/MaterializedIndex/StorageReplicatedMaterializedIndex.cpp:45`

这意味着复制源表虽然能创建对应 MI，但实际不会 `Build` / `Remap`。

### 更好的实现方案

优先级建议：

1. 先接入 per-task failure backoff。
   - `Build` failure key 可用 source part UUID set。
   - `Remap` failure key 可用 affected MI part UUID + delta source UUID。
   - `Compact` failure key 可用 affected MI parts UUID。
   - 调度选择 batch 时跳过处于 backoff 的 failure key。

2. 资源退避改指数退避。
   - base = `materialized_index_resource_failure_backoff`
   - cap 到 5 分钟或 10 分钟
   - 成功提交任务后清理 resource backoff。

3. 有限并行化。
   - 把“每 MI 一个任务”改为可配置并发。
   - 默认仍为 1，实验性打开为 2/4。
   - 需要同时保护 `currently_building_materialized_index_parts`、backlog、future part reservation。

4. 复制版明确禁用或真正实现。
   - 如果短期不做复制调度，应在 DDL 创建时拒绝 `ReplicatedMaterializedIndex`，避免用户误解。
   - 如果要支持，应该类似 `ReplicatedMergeTree`：ZooKeeper task log + leader build + follower fetch + coverage 同步。

## 2. 查询加速

### 当前实现是否有效

有效。核心优化器在：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:205`

当前只匹配严格形态：

```text
LimitStep -> SortingStep -> ExpressionStep -> ReadFromMergeTree
```

匹配后提取：

- `LIMIT K`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:268`
- 单列 `ORDER BY`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:275`
- 距离函数：`L2Distance`、`cosineDistance`、`dotProduct`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:289`
- 搜索列和 reference vector：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:300`

然后：

- 找到依赖源表的 MI candidates：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:755`
- 做算法 `match`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:813`
- 计算 ready parts、coverage、cost：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:817`
- 选择 cost 更低的 MI：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:851`
- 执行 MI search，生成 hints：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:861`
- 全覆盖时直接改写 RFMT 和表达式：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:869`
- 部分覆盖时拆成 covered MI 分支 + uncovered brute-force 分支：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:877`

这个设计比较稳健：不满足条件时 fallback，不太容易产生错误结果。

### 主要问题

#### 1. 查询 plan shape 过于严格

只接受 `Limit -> Sorting -> Expression -> RFMT`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:205`

常见查询如果带 `FilterStep` / `WHERE` / `JOIN` / 子查询，容易不匹配。

#### 2. 显式 `PREWHERE` 直接放弃

`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:735`

```cpp
if (rfmt.getPrewhereInfo())
    return give_up("query has PREWHERE which the MaterializedIndex rewrite does not support");
```

这会让很多 ClickHouse 常见查询形态无法用 MI。

#### 3. `ORDER BY` 只能单列

`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:275`

所以不支持：

```sql
ORDER BY L2Distance(vec, q), id
```

这类 tie-breaking 很常见。

#### 4. `SELECT` 输出不能依赖搜索列

`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:743`

如果用户想 `SELECT vec, L2Distance(vec, q) ...`，MI 会放弃。

#### 5. `dotProduct` 被优化器识别，但算法层可能不支持

优化器支持 `dotProduct`：`src/Processors/QueryPlan/Optimizations/optimizeMaterializedIndex.cpp:289`

但当前 DiskANN 主要是 `L2` / `cosine` 路径，用户可能看到“语法像支持，实际不生效”。

### 更好的实现方案

优先级建议：

1. 支持 `FilterStep` 穿透。
   - 允许形态：`Limit -> Sorting -> Expression -> Filter* -> ReadFromMergeTree`
   - 部分覆盖拆分时，Filter 复制到 covered/uncovered 两个分支。
   - 这是提升实际可用性的最高优先级。

2. 支持多列 `ORDER BY`。
   - 只要求第一列是距离函数。
   - 后续排序键保留给原 `SortingStep` 处理。

3. 允许输出搜索列。
   - 不要用 `_distance` 替换掉 vector 列。
   - 改为额外投影 `_distance` 虚拟列。

4. `PREWHERE` 兼容。
   - 如果 `PREWHERE` 不依赖搜索列，可以保留。
   - MI search 结果作为候选，再和 `PREWHERE` 条件交集。

5. 把 MI 选择过程暴露给 `EXPLAIN`。
   - 输出候选 MI 名称、coverage、fallback cost、MI cost、是否被 setting 禁用。
   - 对调试非常重要。

## 3. DDL

### 当前实现是否有效

部分有效。

`CREATE MATERIALIZED INDEX` 路径比较完整：

- Parser：`src/Parsers/ParserCreateQuery.cpp:1859`
- `TYPE family('impl', params)` 解析：`src/Parsers/ParserCreateQuery.cpp:1819`
- Engine 白名单，只允许 `MaterializedIndex` / `ReplicatedMaterializedIndex`：`src/Parsers/ParserCreateQuery.cpp:1976`
- 默认 engine 为 `MaterializedIndex`：`src/Interpreters/InterpreterCreateQuery.cpp:1344`
- 实验开关：`src/Interpreters/InterpreterCreateQuery.cpp:1739`
- 前置校验：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:84`

前置校验也比较完整：

- 源表必须存在：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:94`
- 源表必须是 `MergeTree` family：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:104`
- 源表 replication 和 MI engine 必须匹配：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:112`
- 源表必须打开 `_block_number`、`_block_offset`、`assign_part_uuids`：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:141`
- 校验 algorithm family/impl：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:164`
- 校验 indexed expression：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:196`
- 需要源表 `SELECT` 权限：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:215`

依赖关系也接入了：

- MI 对源表是 referential dependency：`src/Databases/DDLDependencyVisitor.cpp:188`
- 修改/删除/重命名被 MI 引用的源列会被拦截：`src/Storages/MergeTree/MergeTreeData.cpp:4401`

`DROP MATERIALIZED INDEX` 也有专门语法和校验：

- Parser：`src/Parsers/ParserDropQuery.cpp:120`
- Interpreter 防止对非 MI 表执行 `DROP MATERIALIZED INDEX`：`src/Interpreters/InterpreterDropQuery.cpp:201`

### 主要问题

#### 1. `ALTER MATERIALIZED INDEX` 只解析，不执行

`src/Interpreters/InterpreterAlterQuery.cpp:347`

所有 ALTER MI 都直接：

```cpp
throw Exception(ErrorCodes::NOT_IMPLEMENTED,
    "ALTER MATERIALIZED INDEX is not supported; drop and recreate the index to change its definition");
```

如果当前设计就是 immutable index，这可以接受；但语法和 access type 已接入，会给用户“即将可用”的预期。

#### 2. `SYSTEM REFRESH` / `START` / `STOP MATERIALIZED INDEX` 基本是占位

`src/Interpreters/InterpreterSystemQuery.cpp:878`

目前只是 log：

```cpp
LOG_INFO(log, "SYSTEM {} received; intent recorded.", ...)
```

真正可用的是 `SYSTEM SYNC MATERIALIZED INDEX`：`src/Interpreters/InterpreterSystemQuery.cpp:1999`

#### 3. `BACKUP ... WITH MATERIALIZED INDEXES` 只解析，不执行

只看到 AST/parser 字段：

- `src/Parsers/ParserBackupQuery.cpp:376`
- `src/Parsers/ASTBackupQuery.cpp:329`

没看到 Backup 实现消费 `with_materialized_indexes`。

#### 4. `ReplicatedMaterializedIndex` DDL 允许，但后台任务未实现

DDL 校验强制 replicated source 用 replicated MI：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:112`

但 storage 后台任务禁用：`src/Storages/MaterializedIndex/StorageReplicatedMaterializedIndex.cpp:45`

这是当前最容易误导用户的地方。

#### 5. `ATTACH` 路径校验偏宽

`ATTACH` 时只校验源表和 engine match，然后直接 return：`src/Interpreters/validateMaterializedIndexPrerequisites.cpp:134`

这是为了兼容已有 metadata，但也意味着坏的 metadata 更容易被 attach 进来，后续才在运行期暴露问题。

### 更好的实现方案

1. 明确 DDL 能力边界。
   - 如果 `ALTER` 暂不支持，可以考虑暂时不暴露完整 ALTER 子语法，或者错误信息更明确。
   - `SYSTEM REFRESH` / `START` / `STOP` 若未实现，应返回 `NOT_IMPLEMENTED`，不要只 log。

2. 实现 `SYSTEM REFRESH MATERIALIZED INDEX`。
   - 清理当前 coverage 或标记所有 source parts 需要 rebuild。
   - 唤醒 background assignee。
   - 配合 `SYSTEM SYNC` 等待完成。

3. 实现 `STOP` / `START BUILDS` / `REMAPS` 的持久或内存开关。
   - storage 内保存 `builds_paused` / `remaps_paused`。
   - 调度器在 `choose` 前检查。
   - `system.materialized_indexes` 暴露状态。

4. 处理 replicated DDL 和后台语义不一致。
   - 短期：拒绝创建 `ReplicatedMaterializedIndex`，或标为 experimental-disabled。
   - 长期：实现 replicated MI task log。

5. `Backup` / `Restore` 真正落地。
   - backup MI metadata `.sql`
   - backup MI data parts
   - restore 时恢复 referential dependency 和 `coverage.json`

## 建议的推进顺序

1. 先修后台失败重试和指数退避，避免异常数据或资源不足导致反复重试。
2. 查询优化支持 `FilterStep` / `WHERE` 穿透，这是提升用户实际命中率最大的改动。
3. 处理 `ReplicatedMaterializedIndex` 的语义缺口：要么禁止，要么实现。
4. 实现 `SYSTEM REFRESH` 和 `START` / `STOP`，让运维可控。
5. 补 `EXPLAIN` 可观测性，否则用户很难判断为什么 MI 没生效。
