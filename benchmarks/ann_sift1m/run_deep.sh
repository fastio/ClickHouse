#!/usr/bin/env bash
# DEEP-image (deep-image-96-angular): 训练 + recall 评测
# 数据规模: 9.99M base, 10k query, 96-d cosine
# 直接修改下面的参数块，再执行 ./run_deep.sh 即可。

set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# ====================== 可调参数 ======================
# --- 数据集 ---
DATASET="deep-image-96-angular"

# --- ClickHouse 连接 ---
CH_BIN="${CH_BIN:-/data/develop/feature-knn-3/build/programs/clickhouse}"
HTTP_PORT="${CH_HTTP:-8124}"
TCP_PORT="${CH_TCP:-9100}"

# --- 训练 (build) ---
BUILD_CFG="deep"               # 对应 configs/build_deep.env (针对 10M 行调过)
SCENARIO="single_group_large"  # ann_group_max_rows=15M，把 10M 装进单组
BUILD_TIMEOUT="2h"             # 10M 行 + 96-d，单组建图通常 30-60min，给 2h 余量
OPTIMIZE_BEFORE_BUILD=1        # 必开：流式 INSERT 会产生多 part，单组要先 OPTIMIZE FINAL
KEEP_TABLE=1                   # 强烈建议开：建一次复用，sls 扫表跳过 30+ 分钟的重建

# --- 搜索参数扫表 ---
# 10M 数据点的图直径更大，sls 需要拉得更高才能爬到高 recall
SLS_LIST="50,100,200,400,800"
BEAM_WIDTH=8
SEARCH_IO_LIMIT=1000           # 大数据集每次搜索的磁盘读次数上限要放宽

# --- recall / QPS 评测 ---
K=10
QUERIES_PER_CELL=1000
WARMUP_QUERIES=200
RUNS=3
CONCURRENCIES="1,32"

# ====================== 执行 ======================
mkdir -p tmp data

if [ ! -f "data/${DATASET}.hdf5" ]; then
    echo "[run_deep] 数据集不存在，开始下载 (~3.7GB)..."
    ./download.sh --dataset "$DATASET"
fi

LOG="tmp/run_deep_$(date +%s).log"
echo "[run_deep] 日志: $LOG"
echo "[run_deep] 训练 + 评测开始 (dataset=$DATASET, build_cfg=$BUILD_CFG, scenario=$SCENARIO)"

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

echo "[run_deep] 完成，结果目录: $(ls -td results/*/ | head -1)"
