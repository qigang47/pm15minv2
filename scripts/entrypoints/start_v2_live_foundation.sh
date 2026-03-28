#!/bin/bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: start_v2_live_foundation.sh

Env overrides:
  CONDA_ENV                               default: pm15min
  MALLOC_ARENA_MAX                        default: 2
  V2_LIVE_FOUNDATION_MARKETS              default: sol,xrp
  V2_LIVE_FOUNDATION_SHARED               default: 0
  V2_LIVE_FOUNDATION_CYCLE                default: 15m
  V2_LIVE_FOUNDATION_SURFACE              default: live
  V2_LIVE_FOUNDATION_ITERATIONS           default: 0
  V2_LIVE_FOUNDATION_SLEEP_SEC            default: 1
  V2_LIVE_FOUNDATION_MARKET_DEPTH         default: 1
  V2_LIVE_FOUNDATION_TIMEOUT_SEC          default: 1.2
  V2_LIVE_FOUNDATION_RECENT_WINDOW_MINUTES default: 15
  V2_LIVE_FOUNDATION_MARKET_CATALOG_REFRESH_SEC default: 300
  V2_LIVE_FOUNDATION_BINANCE_REFRESH_SEC  default: 60
  PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS default: 7,8,9
  PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC default: 0
  PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC default: 0.2
  PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC default: 1.5
  PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC default: 300
  V2_LIVE_FOUNDATION_ORACLE_REFRESH_SEC   default: 60
  V2_LIVE_FOUNDATION_STREAMS_REFRESH_SEC  default: 300
  V2_LIVE_FOUNDATION_ORDERBOOK_REFRESH_SEC default: 0.35
  V2_LIVE_FOUNDATION_MARKET_CATALOG_LOOKBACK_HOURS default: 24
  V2_LIVE_FOUNDATION_MARKET_CATALOG_LOOKAHEAD_HOURS default: 24
  V2_LIVE_FOUNDATION_BINANCE_LOOKBACK_MINUTES default: 2880
  V2_LIVE_FOUNDATION_BINANCE_BATCH_LIMIT  default: 1000
  V2_LIVE_FOUNDATION_ORACLE_LOOKBACK_DAYS default: 2
  V2_LIVE_FOUNDATION_ORACLE_LOOKAHEAD_HOURS default: 24
  V2_LIVE_FOUNDATION_NO_DIRECT_ORACLE     default: 0
  V2_LIVE_FOUNDATION_NO_STREAMS           default: 0
  V2_LIVE_FOUNDATION_NO_ORDERBOOKS        default: 1
  V2_LIVE_FOUNDATION_LOG_DIR              default: var/live/logs/entrypoints
EOF
}

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON|y|Y)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

while getopts "h" opt; do
  case "$opt" in
    h)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done
shift $((OPTIND - 1))

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "$PROJECT_DIR"
export PM15MIN_PROJECT_DIR="$PROJECT_DIR"
export PYTHONPATH="$PROJECT_DIR/src:$PROJECT_DIR:${PYTHONPATH:-}"
# shellcheck source=/dev/null
source "$SCRIPT_DIR/_python_env.sh"

pm15min_load_project_env
pm15min_activate_python
export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-2}"

MARKETS_RAW="${V2_LIVE_FOUNDATION_MARKETS:-sol,xrp}"
SHARED_MODE="${V2_LIVE_FOUNDATION_SHARED:-0}"
MARKETS_CSV="$(echo "$MARKETS_RAW" | tr '[:space:]' ',' | tr -s ',')"
IFS=',' read -r -a RAW_MARKETS <<< "$MARKETS_CSV"
MARKETS_LIST=()
for market in "${RAW_MARKETS[@]}"; do
  market="$(echo "$market" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$market" in
    btc|eth|sol|xrp)
      if [[ ! " ${MARKETS_LIST[*]} " =~ [[:space:]]${market}[[:space:]] ]]; then
        MARKETS_LIST+=("$market")
      fi
      ;;
    "")
      ;;
    *)
      echo "WARN: unsupported v2 live foundation market ignored: $market"
      ;;
  esac
done
if [[ ${#MARKETS_LIST[@]} -eq 0 ]]; then
  MARKETS_LIST=("sol" "xrp")
fi

CYCLE="${V2_LIVE_FOUNDATION_CYCLE:-15m}"
SURFACE="${V2_LIVE_FOUNDATION_SURFACE:-live}"
ITERATIONS="${V2_LIVE_FOUNDATION_ITERATIONS:-0}"
SLEEP_SEC="${V2_LIVE_FOUNDATION_SLEEP_SEC:-1}"
MARKET_DEPTH="${V2_LIVE_FOUNDATION_MARKET_DEPTH:-1}"
TIMEOUT_SEC="${V2_LIVE_FOUNDATION_TIMEOUT_SEC:-1.2}"
RECENT_WINDOW_MINUTES="${V2_LIVE_FOUNDATION_RECENT_WINDOW_MINUTES:-15}"
MARKET_CATALOG_REFRESH_SEC="${V2_LIVE_FOUNDATION_MARKET_CATALOG_REFRESH_SEC:-300}"
BINANCE_REFRESH_SEC="${V2_LIVE_FOUNDATION_BINANCE_REFRESH_SEC:-60}"
ORACLE_REFRESH_SEC="${V2_LIVE_FOUNDATION_ORACLE_REFRESH_SEC:-60}"
STREAMS_REFRESH_SEC="${V2_LIVE_FOUNDATION_STREAMS_REFRESH_SEC:-300}"
ORDERBOOK_REFRESH_SEC="${V2_LIVE_FOUNDATION_ORDERBOOK_REFRESH_SEC:-0.35}"
MARKET_CATALOG_LOOKBACK_HOURS="${V2_LIVE_FOUNDATION_MARKET_CATALOG_LOOKBACK_HOURS:-24}"
MARKET_CATALOG_LOOKAHEAD_HOURS="${V2_LIVE_FOUNDATION_MARKET_CATALOG_LOOKAHEAD_HOURS:-24}"
BINANCE_LOOKBACK_MINUTES="${V2_LIVE_FOUNDATION_BINANCE_LOOKBACK_MINUTES:-2880}"
BINANCE_BATCH_LIMIT="${V2_LIVE_FOUNDATION_BINANCE_BATCH_LIMIT:-1000}"
ORACLE_LOOKBACK_DAYS="${V2_LIVE_FOUNDATION_ORACLE_LOOKBACK_DAYS:-2}"
ORACLE_LOOKAHEAD_HOURS="${V2_LIVE_FOUNDATION_ORACLE_LOOKAHEAD_HOURS:-24}"
NO_DIRECT_ORACLE="${V2_LIVE_FOUNDATION_NO_DIRECT_ORACLE:-0}"
NO_STREAMS="${V2_LIVE_FOUNDATION_NO_STREAMS:-0}"
NO_ORDERBOOKS="${V2_LIVE_FOUNDATION_NO_ORDERBOOKS:-1}"
LOG_DIR="${V2_LIVE_FOUNDATION_LOG_DIR:-$PROJECT_DIR/var/live/logs/entrypoints}"
export PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS="${PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS:-7,8,9}"
export PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC="${PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC:-0}"
export PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC="${PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC:-0.2}"
export PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC="${PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC:-1.5}"
export PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC="${PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC:-300}"

mkdir -p "$LOG_DIR"

echo "============================================="
echo "Starting v2 live foundation loops"
echo "Project: $PROJECT_DIR"
echo "Markets: ${MARKETS_LIST[*]}"
echo "Shared Mode: $SHARED_MODE"
echo "Cycle: $CYCLE"
echo "Surface: $SURFACE"
echo "Iterations: $ITERATIONS"
echo "Sleep Sec: $SLEEP_SEC"
echo "MALLOC_ARENA_MAX: $MALLOC_ARENA_MAX"
echo "Market Catalog Refresh Sec: $MARKET_CATALOG_REFRESH_SEC"
echo "Binance Refresh Sec: $BINANCE_REFRESH_SEC"
echo "Binance Boundary Offsets: $PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS"
echo "Binance Boundary Delay Sec: $PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC"
echo "Binance Retry Interval Sec: $PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC"
echo "Binance Retry Window Sec: $PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC"
echo "Binance Fallback Refresh Sec: $PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC"
echo "Oracle Refresh Sec: $ORACLE_REFRESH_SEC"
echo "Streams Refresh Sec: $STREAMS_REFRESH_SEC"
echo "No Direct Oracle: $NO_DIRECT_ORACLE"
echo "No Streams: $NO_STREAMS"
echo "No Orderbooks: $NO_ORDERBOOKS"
echo "============================================="

for market in "${MARKETS_LIST[@]}"; do
  echo "Stopping previous v2 live foundation for ${market}/${CYCLE}/${SURFACE} ..."
  pkill -f "pm15min data run live-foundation --market ${market} --cycle ${CYCLE} --surface ${SURFACE}" || true
done
pkill -f "pm15min.data.pipelines.foundation_shared" || true
sleep 1

if is_truthy "$SHARED_MODE"; then
  log_path="$LOG_DIR/live_foundation_shared_${CYCLE}_${SURFACE}.out"
  touch "$log_path"
  cmd=(
    "$PYTHON_BIN" -m pm15min.data.pipelines.foundation_shared
    --markets "$MARKETS_CSV"
    --cycle "$CYCLE"
    --surface "$SURFACE"
    --market-depth "$MARKET_DEPTH"
    --timeout-sec "$TIMEOUT_SEC"
    --recent-window-minutes "$RECENT_WINDOW_MINUTES"
    --loop
    --iterations "$ITERATIONS"
    --sleep-sec "$SLEEP_SEC"
    --market-catalog-refresh-sec "$MARKET_CATALOG_REFRESH_SEC"
    --binance-refresh-sec "$BINANCE_REFRESH_SEC"
    --oracle-refresh-sec "$ORACLE_REFRESH_SEC"
    --streams-refresh-sec "$STREAMS_REFRESH_SEC"
    --orderbook-refresh-sec "$ORDERBOOK_REFRESH_SEC"
    --market-catalog-lookback-hours "$MARKET_CATALOG_LOOKBACK_HOURS"
    --market-catalog-lookahead-hours "$MARKET_CATALOG_LOOKAHEAD_HOURS"
    --binance-lookback-minutes "$BINANCE_LOOKBACK_MINUTES"
    --binance-batch-limit "$BINANCE_BATCH_LIMIT"
    --oracle-lookback-days "$ORACLE_LOOKBACK_DAYS"
    --oracle-lookahead-hours "$ORACLE_LOOKAHEAD_HOURS"
  )
  if is_truthy "$NO_DIRECT_ORACLE"; then
    cmd+=(--no-direct-oracle)
  fi
  if is_truthy "$NO_STREAMS"; then
    cmd+=(--no-streams)
  fi
  if is_truthy "$NO_ORDERBOOKS"; then
    cmd+=(--no-orderbooks)
  fi
  nohup "${cmd[@]}" >> "$log_path" 2>&1 &
  echo "started shared v2 live foundation: markets=${MARKETS_LIST[*]} pid=$!"
  echo "  wrapper log: $log_path"
else
  for market in "${MARKETS_LIST[@]}"; do
    log_path="$LOG_DIR/live_foundation_${market}_${CYCLE}_${SURFACE}.out"
    touch "$log_path"
    cmd=(
      "$PYTHON_BIN" -m pm15min data run live-foundation
      --market "$market"
      --cycle "$CYCLE"
      --surface "$SURFACE"
      --market-depth "$MARKET_DEPTH"
      --timeout-sec "$TIMEOUT_SEC"
      --recent-window-minutes "$RECENT_WINDOW_MINUTES"
      --loop
      --iterations "$ITERATIONS"
      --sleep-sec "$SLEEP_SEC"
      --market-catalog-refresh-sec "$MARKET_CATALOG_REFRESH_SEC"
      --binance-refresh-sec "$BINANCE_REFRESH_SEC"
      --oracle-refresh-sec "$ORACLE_REFRESH_SEC"
      --streams-refresh-sec "$STREAMS_REFRESH_SEC"
      --orderbook-refresh-sec "$ORDERBOOK_REFRESH_SEC"
      --market-catalog-lookback-hours "$MARKET_CATALOG_LOOKBACK_HOURS"
      --market-catalog-lookahead-hours "$MARKET_CATALOG_LOOKAHEAD_HOURS"
      --binance-lookback-minutes "$BINANCE_LOOKBACK_MINUTES"
      --binance-batch-limit "$BINANCE_BATCH_LIMIT"
      --oracle-lookback-days "$ORACLE_LOOKBACK_DAYS"
      --oracle-lookahead-hours "$ORACLE_LOOKAHEAD_HOURS"
    )
    if is_truthy "$NO_DIRECT_ORACLE"; then
      cmd+=(--no-direct-oracle)
    fi
    if is_truthy "$NO_STREAMS"; then
      cmd+=(--no-streams)
    fi
    if is_truthy "$NO_ORDERBOOKS"; then
      cmd+=(--no-orderbooks)
    fi

    nohup "${cmd[@]}" >> "$log_path" 2>&1 &
    echo "started v2 live foundation: market=${market} pid=$!"
    echo "  wrapper log: $log_path"
  done
fi

echo ""
echo "Canonical operator checks:"
echo "  PYTHONPATH=src python -m pm15min data run live-foundation --market sol --cycle 15m --surface live --iterations 1 --no-orderbooks"
