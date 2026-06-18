# Reflection ANN 索引重构方案

> 目标：简化 reflection 的 build / remap / compact——主要是 **locator 的存储与回源**——但**保留 remap、保留多源图**这两个核心价值。本文为讨论收敛后的设计稿。

## 0. 一句话结论

- **保留多源图 + 保留 remap**；给每张图设**行数上限**（如 1000 万）。
- **build 时 part 整进整出**（普通 part 不跨图，仅超大 part 切分）→ 让 locator 的 part 区分符退化成 **range 编码**。
- locator 从「4 个 MergeTree 列 + per-row payload + hot/cold codec」改为「part 内**不透明文件** `stable_id.bin` + `offset.bin` + **range 段表**」。
- 源 embedding 列用 `Vector(T, N)`（FLAT 布局）；未被图覆盖的最新/小数据走**暴力扫**，也作退化兜底。

---

## 1. 背景：当前复杂度根因

- 一张 MI-part 图**多源**（覆盖 N 个 source part）：`uuid_to_payload_part_id` 把 N 个 uuid 压成 `0..N-1`（`BuildTask.cpp:774-783`）。
- 回源靠 **4 个 MergeTree 列**：`source_uuid` / `part_offset` / `block_number` / `block_offset`（`registerStorageANNIndex.cpp:362-369`），逐行写入（`BuildTask.cpp:203-251`）。
- 列存导致散乱点查要读整 granule → 又造了 `payload_part_id` + `ANNIndexPayloadCodec` 的 **hot/cold 双路**绕开 IO（`ANNIndexMatcher.cpp:389-532`、`readLocatorRows`）。
- 源端 merge/mutation → `RemapTask`（1396 行）用 `MergedBlockOutputStream` **整段重写 4 列** + N-to-M 派生 + hardlink。

复杂度集中在「locator 是逐行重写的列 + 多源 per-row 区分符 + hot/cold 双路」。

---

## 2. 核心决策

| 决策 | 内容 | 理由 |
|---|---|---|
| 保留 remap | merge/mutation 只挪 locator，不重建图 | 重建图 O(N·log N) 贵；remap 只动 O(行) 的廉价数据 |
| 保留多源图 | 一张图覆盖多个 source part | 批量摊销、图少、fan-out 低、靠 remap 吸收变更不重建 |
| **图行数上限**（如 1000 万） | build/compact/rebuild 都 ≤ 上限 | 把所有重型操作的最坏情况钉死，不随 part（可达 150GB）放大 |
| **part-atomic build** | 普通 part 整进一张图；仅 >上限 的超大 part 切多张图 | 使每张图 = 整 part 的连续拼接 → locator part 区分符可 range 编码 |
| FLAT 列分层 | 源列 `Vector(T,N)`；未覆盖数据暴力扫 | 最新/小数据不进图，省 build/remap；图退化时可回退暴力 |

---

## 3. 数据模型

- 一张图 = 一个 DiskANN 图，`internal_id ∈ [0, N)`，**build 时定死、永不变**，`N ≤ 行数上限`。
- 图**不跨分区**；`{block_number, block_offset}` 在**分区内单调唯一**，是行的持久身份（merge 保留，`_block_number`/`_block_offset` 持久虚拟列）。
- 一张图覆盖多个 source part，但每个 part 的行在图里是**一段连续的 internal_id**（part-atomic build 保证）。

---

## 4. Locator 设计（核心）

### 4.1 磁盘形态：part 内不透明文件 + 单锚列

像 DiskANN 图一样，用 `writeFile`/`readFile`/折进 `checksums.txt`（已有机制：`BuildTask.cpp:534/579`、`DiskANNAlgorithm.cpp:331/718`），**不走列管线**：

```
锚列（1 个常量 MergeTree 列）        ← 满足 ≥1 列，规避 0 列风险（见 §9）
stable_id.bin                       ← 不可变：internal_id -> {block_number, block_offset}；remap 用
offset.bin                          ← 可变：  internal_id -> part_offset（UInt32，0xFFFFFFFF=TOMBSTONE）；查询用；identity 态不落盘
range 段表（header.json 或小文件）   ← 可变：  K 段 [internal_id 区间) -> 当前 source part
DiskANN 图文件                       ← algorithm-private 不透明文件
header.json / count.txt / checksums.txt / partition·minmax   ← part 骨架
```

- `stable_id.bin`：查询永不碰（冷文件）。
- `offset.bin`：稠密、**不压缩**（mmap 后 O(1) 随机访问）；`internal_id` 是隐式下标。`is_identity` 时（刚 build、或只经历 lightweight `DELETE`）`offset[i]==i`，**整个文件不存在**。

### 4.2 part 区分符 = range 段表（不是 per-row）

part-atomic build 让每张图按 part 顺序拼接，`internal_id` 天然分段：

```
graph:  [0,|A|)→A   [|A|,|A|+|B|)→B   [|A|+|B|,N)→D
```

故「这行属于哪个 part」**不用 per-row 存**，只存 K 段（K = 图里的 part 数）。这是相对当前 per-row `payload_part_id` 的根本简化。

### 4.3 range 段表不会退化为 map

两个不变量：
1. `internal_id` build 时定死 → 段边界**不可移动** → 划分只能**变粗**。
2. 没有任何操作把一个 part 的行拆到两个 part（merge 合整 part、mutation 1:1）→ 每段**整体重贴标签**，永不分裂。

⟹ 段数从 build 的 K 出发**单调不增**，相邻同目标可 coalesce 变少，极端收敛回 1 段（单源）。段数 ∈ [不同目标 part 数, K]，永远 ≤ K，绝不趋向 N。（对比：per-row 才是永远 N 条的 map。）

### 4.4 内存结构：有序割点数组 + 二分

```cpp
struct GraphRangeMap
{
    std::vector<UInt32> starts;   // K 条，每段起始 internal_id，升序，starts[0]==0
    std::vector<UInt32> targets;  // K 条，每段当前 part（查询准备阶段可预解析成活跃 part 下标）
};

UInt32 lookupTarget(UInt32 internal_id) const
{
    auto it = std::upper_bound(starts.begin(), starts.end(), internal_id);
    return targets[(it - starts.begin()) - 1];
}
```

- 拆成两数组（SoA）：二分只碰 `starts`，cache 密；`targets` 命中后取一次。
- O(log K)，K 小且全在 L1 → 微秒级，相对 ANN search + IO 可忽略。
- 开图时加载一次、缓存（与 `ANNSearcherCache` 并列），不每查询重读；remap 时维护（重贴 + coalesce）。
- 可选：命中量与 K 都大时，把这批命中按 `internal_id` 排序后与 K 段**一次归并**，O(hits+K) 取代 O(hits·log K)。

### 4.5 对回当前 4 个 locator 列

| 当前（MergeTree 列，逐行） | 新结构 |
|---|---|
| `source_uuid` | range 段表 `targets`（K 条，不是 per-row） |
| `part_offset` | `offset.bin`（可变，identity 态不落盘） |
| `block_number` + `block_offset` | `stable_id.bin`（不可变） |

删除：`readLocatorRows` 的列式读、hot/cold payload codec、per-row `payload_part_id`。

### 4.6 ANN 图 payload：空 + 查询一次回源

**铁律：payload 只放不可变数据。** graph 内嵌 payload 一旦可变，remap 就得回写图（DiskANN 盘上格式按节点交错 vector+payload+neighbors），砸了「remap 不碰图」的根。凡 remap 会改的绝不进 payload。

- **locator 不进 payload**（与当前 hot-path 相反）：`part_offset` 是 remap 每次都改的；且 `offset.bin` 已是 O(1) mmap，hot-path 想省的 IO 本就没了——收益归零、代价致命。当前喂 locator 的 `supportsPerVectorPayload` 路（`BuildTask.cpp:275-279`）删除。
- **stable_id 不进 payload**：只给 remap 用，查询无需。
- ⟹ **per-vector 额外 payload = 空**。图内每节点只有：PQ 压缩向量（遍历）+ re-rank 向量 f32/SQ（精排）+ 邻接表。**re-rank 向量图内自带**，不回查源 `Vector` 列（遍历本身就要读向量，回查是灾难；与源列重复一份是「FLAT 层 + 索引」共存的固有成本，f32↔SQ 为召回/存储旋钮）。

**查询阶段做一次 `internal_id` 回源**（唯一路径，无 hot/cold 双路）：

```
search → {internal_id, 距离}
   → 段表二分 lookupTarget(internal_id) → 当前 part
   → offset.bin[internal_id]（identity 态用 internal_id 本身）→ part_offset
   → {part, part_offset}
```

> 例外：未来若要 hybrid / filtered search，可把**不可变**过滤属性放 payload 就地剪枝；**可变**属性不行，仍走 `ReadFromMergeTree` 后置过滤。

---

## 5. 查询路径

1. ANN search → `internal_id` 集合 + 距离。
2. **每个命中做一次 `internal_id` 回源**（唯一路径，payload 为空、无 hot/cold）：`lookupTarget(internal_id)` → 当前 part；`offset.bin[internal_id]`（`is_identity` 时直接用 `internal_id`）→ `part_offset`；`TOMBSTONE` 跳过。
3. 按当前 part 的 uuid 归组成 `NearestNeighbours{rows=part_offsets, distances}`，喂 `ReadFromMergeTree` 作位置过滤（`MergeTreeRangeReader.cpp:1369-1389`），与 `_row_exists` 叠加。
4. **未被图覆盖的 part**（FLAT 层）：暴力扫 `Vector` 列，topk 跨 part 归并——复用已存在的「部分覆盖 Union」（`ANNIndexMatcher.cpp:634-690` 的 `full_coverage`/`uncovered_source_rows`）。

`{part_uuid, part_offset}` 的来源：`part_uuid` 来自段表 `targets`（每段一次，配活跃 part），`part_offset` 来自 `offset.bin`（每行一次）。

---

## 6. Build 路径

1. 选分区内未覆盖的 part，**bin-pack 到 ~上限**（part-atomic：普通 part 整进；>上限 的超大 part 切成多张连续切片图）。
2. 按 part 顺序流式读，分配 `internal_id 0..N-1`；记录段边界（part-atomic → 连续）。
3. 写 `stable_id.bin`（取 `_block_number`/`_block_offset`，`BuildTask.cpp:145-146`）、`offset.bin`（identity → 省略）、range 段表、锚列、DiskANN 图。
4. 触发：未覆盖量攒到 ~上限，或按 age/稳定性（沿用现有 `uncovered_source_backlog` 的批量逻辑，加上限约束）。

---

## 7. Remap 路径

merge/mutation 产出新 part C，对「段指向被合并掉的旧 part」的图：

1. 扫 C 一遍 `_block_number/_block_offset` → `map: {block,offset} -> C_offset`（**多个图/段共享这一次扫描**）。
2. 每个受影响段：`offset[i] = map[stable_id[i]]`，查不到 = `TOMBSTONE`；段 `target` 重贴成 C；清 `is_identity`。
3. 相邻同目标段 coalesce。
4. **图与 `stable_id.bin` 不动**；只重写 `offset.bin`（受影响切片）+ 段表。代价 ≤ 上限，被它紧跟的 merge 的 O(C) 盖住。

配对：段 `target` 以 part 标识（name/uuid）记录；reconcile 找「覆盖旧 part lineage 的活跃 part」= C。
（可选优化：若 merge 能直接吐 `old_offset→new_offset` provenance（`merged_part_offsets` 基础设施已存在），可省扫 C 与 `stable_id.bin`，remap 变纯 gather。先用自包含的 `stable_id.bin` 方案。）

---

## 8. Compact 路径

- 触发：tombstone 比例越阈值；或同一 part 被过多图覆盖（fan-out 痛）。
- 动作：对活跃行**重建一张 ≤上限 的图**（向量取自 `Vector` 列），GC 掉 tombstone 死 `internal_id`，段表重置为 identity/单段。
- 重型但**有界**（≤上限），且择机摊销。

---

## 9. Tombstone / lightweight delete

- **正确性免费**：命中 = part_offset = 位置过滤，被删行由 `_row_exists` 在同一次读滤掉。无需 sentinel/locator 改写。
- **两类删除分清**：

| 状态 | 谁挡 | 体现 |
|---|---|---|
| 物理还在、被标记删除 | `_row_exists` | `offset[i]` 指向 C 里那行，读到被 mask 滤掉 |
| 已被 merge 物理回收 | `offset.bin` TOMBSTONE | `offset[i]=0xFFFFFFFF`，直接跳过 |

- 生命周期：`_row_exists` mask（identity 态，offset 不变）→ 下次 merge 物理回收 → `offset.bin` TOMBSTONE（图里 `internal_id` 还在，占候选预算）→ 比例越阈值 compact 重建移除。
- **放宽索引有效性**（必做）：lightweight `DELETE` 不重写数据列（offset 不变），旧图仍 offset-valid；但 `coveredEntryMatchesActiveSourcePart`（`ANNIndexMatcher.cpp:127-133`）按 `mutation` 严格比对会误判 → 改为按数据身份（partition + block-range）匹配、允许更高 lightweight-delete mutation，读时套当前 `_row_exists`。heavyweight mutation（重写 part、offset 变）才触发 remap/重建。
- **候选预算污染**缓解：① over-fetch 按删除比例放大 candidate_limit（`k/(1-d)`，比例从 `rows_count` vs `existing_rows_count` 得）；② tombstone 比例触发 compact；③（可选）`_row_exists` bitmap 作 deleted-set 喂进搜索遍历跳过。

---

## 10. FLAT 层（`Vector(T,N)`）

- 源 embedding 列声明为 `Vector(T, N)`（PR https://github.com/ClickHouse/ClickHouse/pull/106851，FLAT 布局，无 offsets，SIMD 友好）。
- 职责：① 还没被任何图覆盖的**最新增量**（build 滞后窗口）暴力扫；② 极小阈值以下不值得建图的暴力扫；③ 某图被删/改弄得太空洞时 **drop 回退暴力扫**（compact 从必须变可选）。

---

## 11. 相对当前代码的改动

- **保留**：多源图、remap、compact、scheduler、matcher 框架、DiskANN facade。
- **改/删**：4 个列式 locator 列 + `readLocatorRows` + hot/cold payload codec + per-row `payload_part_id` + 算法 per-vector payload 喂入（payload 置空，查询改为一次 `internal_id` 回源）→ 不透明 `stable_id.bin`/`offset.bin` + range 段表；`RemapTask` 整段重写 → 只重写 `offset.bin` 受影响切片 + 段表。
- **新增**：`Vector(T,N)` 源列 + FLAT 暴力层；图行数上限；part-atomic build；range 段表 + 二分；identity-by-default；锚列。

---

## 12. 关键参数

- 图行数上限（如 1000 万）。
- build 阈值 / 稳定性门控（沿用 `ann_index_build_min_rows`/`min_bytes`，语义改为「攒到上限/稳定才建」）。
- compact 的 tombstone 比例阈值、fan-out 阈值。
- over-fetch 系数。

---

## 13. 开放问题 / 待定

1. **锚列内容**：常量 `partition_id`（图本就分区内）vs 退化占位列。（单源时曾定 `source_part_name`，多源后该角色由段表承担，锚列回到最小职责。）
2. **段表落盘**：`header.json` 内 vs 独立小文件；remap 的就地更新 vs 整体重写。
3. **`offset.bin` remap**：受影响切片就地更新 vs 整文件重写（均 ≤上限）。
4. **超大 part（>上限）切分**：每切片为单源段，切分边界与 reconcile 细节。
5. **配对/ reconcile**：段 `target` 用 name vs uuid；lightweight delete 的数据身份匹配实现。
6. **0 列形态**：暂保留锚列规避风险；真去 0 列需验证 `require_columns_checksums` / 副本 fetch / index granularity 三处，作后续。
7. **旧盘兼容**：已有多源 MI-part（带 4 列 locator）→ 升级即 drop+rebuild（reflection 可从源重算，符合 `IReflection` 语义）。
8. **同一 part 内 covered/uncovered 行级切分**：part 被图覆盖一部分、另一部分是新插入未覆盖行时，查询如何在单 part 内拆 covered（段表/offset 给出的集合）与 uncovered（其余 → 暴力扫）。
