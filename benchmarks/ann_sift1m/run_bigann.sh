#!/usr/bin/env bash
# BIGANN-1B (NeurIPS '21 Big ANN Benchmarks): 训练 + recall 评测
# 数据规模: 1B base (uint8), 10K query (uint8), 128-d L2
# 直接修改下面的参数块，再执行 ./run_bigann.sh 即可。
#
# 重要警告:
#   - 总下载体积 ~135 GB；首次运行 download 阶段约 30-60 分钟 (取决于带宽)。
#   - 入 ClickHouse 后 base 表约 512 GB (uint8 -> Float32 4x)，加 ANN 图约
#     400 GB，整库 ~900 GB。请确认 /data 至少有 1.2 TB 可用空间。
#   - SYSTEM BUILD ANN INDEX 在 64 核机器上预期 4-6 天。期间不要重启 server。
#   - 默认 KEEP_TABLE=1：sweep 跑完不会 DROP base 表。改 BUILD_CFG 重跑前
#     必须手动 DROP，否则会复用旧索引、新参数不生效。

set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# ====================== 可调参数 ======================
# --- 数据集 ---
DATASET="bigann-1B-euclidean"
# 行数限制: 0 = 全量 1B，>0 = 切片 (注意：GT 文件对应 1B，切片后 recall 会失真)
# 仅用于 harness 烟雾测试，不要拿切片结果当 benchmark 用。
ROWS_LIMIT=0

# --- ClickHouse 连接 ---
CH_BIN="${CH_BIN:-/data/develop/feature-knn-3/build/programs/clickhouse}"
HTTP_PORT="${CH_HTTP:-8124}"
TCP_PORT="${CH_TCP:-9100}"
HTTP_TIMEOUT="24h"             # INSERT/OPTIMIZE 在 1B 上要小时级，远超默认 30m

# --- 训练 (build) ---
BUILD_CFG="bigann"             # 对应 configs/build_bigann.env (DiskANN paper 1B 调参)
SCENARIO="single_group_billion" # ann_group_max_rows=1.5B
BUILD_TIMEOUT="168h"           # 7 天，给 DiskANN paper 5 天 + 余量
OPTIMIZE_BEFORE_BUILD=1        # 必开：1B INSERT 必产多 part，要先 FINAL 合一
KEEP_TABLE=1                   # 强烈建议 1：5 天的 build 不能丢

# --- 搜索参数扫表 ---
# 1B 上 sls 要拉得更高才能爬到高 recall (图直径远大于 1M)
SLS_LIST="100,200,400,800,1600"
BEAM_WIDTH=8
SEARCH_IO_LIMIT=2000           # 大 graph 单查询的磁盘读次数上限要再放宽

# --- recall / QPS 评测 ---
K=10
QUERIES_PER_CELL=1000          # BIGANN public query 总共 10K，挑 1K 跑 sweep
WARMUP_QUERIES=200
RUNS=3
CONCURRENCIES="1,32"

# ====================== 执行 ======================
mkdir -p tmp data

# 多文件，依赖 download.sh 自身的 skip-if-exists 逻辑
echo "[run_bigann] 检查 / 下载数据集 (~135 GB，首次较慢)..."
./download.sh --dataset "$DATASET"

LOG="tmp/run_bigann_$(date +%s).log"
echo "[run_bigann] 日志: $LOG"
echo "[run_bigann] 训练 + 评测开始 (dataset=$DATASET, build_cfg=$BUILD_CFG, scenario=$SCENARIO)"
echo "[run_bigann] 预计 build 4-6 天，期间结果增量写入 results/<run_id>/sweep.tsv"

ARGS=(
    --dataset="$DATASET"
    --http-port="$HTTP_PORT"
    --tcp-port="$TCP_PORT"
    --clickhouse-binary="$CH_BIN"
    --scenarios="$SCENARIO"
    --build-cfgs="$BUILD_CFG"
    --sls-list="$SLS_LIST"
    --beam-width="$BEAM_WIDTH"
    --search-io-limit="$SEARCH_IO_LIMIT"
    --k="$K"
    --queries-per-cell="$QUERIES_PER_CELL"
    --warmup-queries="$WARMUP_QUERIES"
    --runs="$RUNS"
    --build-timeout="$BUILD_TIMEOUT"
    --http-timeout="$HTTP_TIMEOUT"
)
[ -n "$CONCURRENCIES" ] && ARGS+=( --concurrencies="$CONCURRENCIES" )
[ "$KEEP_TABLE" = "1" ]            && ARGS+=( --keep-table )
[ "$OPTIMIZE_BEFORE_BUILD" = "1" ] && ARGS+=( --optimize-before-build )
[ "$ROWS_LIMIT" -gt 0 ]            && ARGS+=( --rows-limit="$ROWS_LIMIT" )

./run.sh "${ARGS[@]}" 2>&1 | tee "$LOG"

echo "[run_bigann] 完成，结果目录: $(ls -td results/*/ | head -1)"
