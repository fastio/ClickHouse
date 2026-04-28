#!/usr/bin/env bash
# SIFT-1M (sift-128-euclidean): 训练 + recall 评测
# 数据规模: 1M base, 10k query, 128-d L2
# 直接修改下面的参数块，再执行 ./run_sift.sh 即可。

set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# ====================== 可调参数 ======================
# --- 数据集 ---
DATASET="sift-128-euclidean"

# --- ClickHouse 连接 ---
CH_BIN="${CH_BIN:-/data/develop/feature-knn-3/build/programs/clickhouse}"
HTTP_PORT="${CH_HTTP:-8124}"
TCP_PORT="${CH_TCP:-9100}"

# --- 训练 (build) ---
BUILD_CFG="paper"          # 对应 configs/build_paper.env
SCENARIO="single_group"    # 对应 scenarios/single_group.env
BUILD_TIMEOUT="30m"        # 单组 1M 行通常 5-10 分钟
OPTIMIZE_BEFORE_BUILD=0    # 1M 行 INSERT 通常已是单 part，不需要 OPTIMIZE
KEEP_TABLE=0               # 1: 复用已有 base 表 (跳过重建)，0: 每次重建

# --- 搜索参数扫表 ---
SLS_LIST="10,30,50,100,200"   # ann_search_list_size 序列
BEAM_WIDTH=8                  # ann_beam_width
SEARCH_IO_LIMIT=500           # search_io_limit (单次搜索的最大磁盘读次数)

# --- recall / QPS 评测 ---
K=10                          # Recall@K
QUERIES_PER_CELL=1000         # 每个 cell 的查询数
WARMUP_QUERIES=200            # 每个 cell 前的 warm-up 查询数
RUNS=3                        # 每个 cell 重复次数 (取中位数稳定)
CONCURRENCIES="1,32"          # 并发等级，留空则用 1,nproc

# ====================== 执行 ======================
mkdir -p tmp data

if [ ! -f "data/${DATASET}.hdf5" ]; then
    echo "[run_sift] 数据集不存在，开始下载..."
    ./download.sh --dataset "$DATASET"
fi

LOG="tmp/run_sift_$(date +%s).log"
echo "[run_sift] 日志: $LOG"
echo "[run_sift] 训练 + 评测开始 (dataset=$DATASET, build_cfg=$BUILD_CFG, scenario=$SCENARIO)"

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
)
[ -n "$CONCURRENCIES" ] && ARGS+=( --concurrencies="$CONCURRENCIES" )
[ "$KEEP_TABLE" = "1" ]            && ARGS+=( --keep-table )
[ "$OPTIMIZE_BEFORE_BUILD" = "1" ] && ARGS+=( --optimize-before-build )

./run.sh "${ARGS[@]}" 2>&1 | tee "$LOG"

echo "[run_sift] 完成，结果目录: $(ls -td results/*/ | head -1)"
