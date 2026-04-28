#!/usr/bin/env bash
# GIST-1M (gist-960-euclidean): 训练 + recall 评测
# 数据规模: 1M base, 1k query, 960-d L2
# 直接修改下面的参数块，再执行 ./run_gist.sh 即可。

set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# ====================== 可调参数 ======================
# --- 数据集 ---
DATASET="gist-960-euclidean"

# --- ClickHouse 连接 ---
CH_BIN="${CH_BIN:-/data/develop/feature-knn-3/build/programs/clickhouse}"
HTTP_PORT="${CH_HTTP:-8124}"
TCP_PORT="${CH_TCP:-9100}"

# --- 训练 (build) ---
BUILD_CFG="gist"           # 对应 configs/build_gist.env (针对 960-d 调过)
SCENARIO="single_group"
BUILD_TIMEOUT="60m"        # 960-d 高维向量训练慢，给 1h 余量
OPTIMIZE_BEFORE_BUILD=0
KEEP_TABLE=0               # 1: 复用 base 表 (调 sls 不重建)，加速二次实验

# --- 搜索参数扫表 ---
# GIST 的高维度让 recall 曲线起步更陡，更小的 sls 已能区分；按需补 400/800
SLS_LIST="20,50,100,200,400"
BEAM_WIDTH=8
SEARCH_IO_LIMIT=500

# --- recall / QPS 评测 ---
# GIST 只有 1k query，把 queries-per-cell 上限拉到 1000 已用满
K=10
QUERIES_PER_CELL=1000
WARMUP_QUERIES=100
RUNS=3
CONCURRENCIES="1,16"

# ====================== 执行 ======================
mkdir -p tmp data

if [ ! -f "data/${DATASET}.hdf5" ]; then
    echo "[run_gist] 数据集不存在，开始下载 (~3.6GB)..."
    ./download.sh --dataset "$DATASET"
fi

LOG="tmp/run_gist_$(date +%s).log"
echo "[run_gist] 日志: $LOG"
echo "[run_gist] 训练 + 评测开始 (dataset=$DATASET, build_cfg=$BUILD_CFG, scenario=$SCENARIO)"

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

echo "[run_gist] 完成，结果目录: $(ls -td results/*/ | head -1)"
