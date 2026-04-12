#!/bin/bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: start_v2_orderbook_fleet.sh

Env overrides:
  CONDA_ENV                           default: pm15min
  V2_ORDERBOOK_FLEET_SKIP_CONDA       default: 0
  V2_ORDERBOOK_FLEET_MARKETS          default: btc,eth,sol,xrp
  V2_ORDERBOOK_FLEET_CYCLE            default: 15m
  V2_ORDERBOOK_FLEET_SURFACE          default: live
  V2_ORDERBOOK_FLEET_POLL_SEC         default: 0.35
  V2_ORDERBOOK_FLEET_TIMEOUT_SEC      default: 1.2
  V2_ORDERBOOK_FLEET_RECENT_WINDOW_MINUTES default: 15
  V2_ORDERBOOK_FLEET_MARKET_DEPTH     default: 1
  V2_ORDERBOOK_FLEET_MARKET_START_OFFSET default: 0
  V2_ORDERBOOK_FLEET_ITERATIONS       default: 0
  V2_ORDERBOOK_FLEET_SLEEP_SEC        default: same as poll interval
  V2_ORDERBOOK_FLEET_LOG_PATH         default: var/<surface>/logs/entrypoints/orderbook_fleet_<cycle>_<surface>.out
  V2_ORDERBOOK_FLEET_STOP_MODE        default: scope (scope|global|none)
  PM15MIN_ORDERBOOK_FLEET_SCHEDULER_MODE default: process_per_market
  MALLOC_ARENA_MAX                    default: 2
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

orderbook_recorder_pattern() {
  local market="$1"
  printf '%s' "market='${market}', cycle='${CYCLE}', surface='${SURFACE}'"
}

all_orderbook_recorder_pids() {
  pgrep -f -- "run_orderbook_recorder; cfg = DataConfig.build" || true
}

stop_all_orderbook_recorders() {
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(all_orderbook_recorder_pids)
  if [[ ${#pids[@]} -eq 0 ]]; then
    echo "No existing orderbook recorder processes found."
    return 0
  fi
  echo "Stopping all existing orderbook recorder processes: ${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1
  local remaining=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && remaining+=("$pid")
  done < <(all_orderbook_recorder_pids)
  if [[ ${#remaining[@]} -gt 0 ]]; then
    echo "Force stopping remaining orderbook recorder processes: ${remaining[*]}"
    kill -9 "${remaining[@]}" 2>/dev/null || true
  fi
}

orderbook_recorder_pids() {
  local market="$1"
  local pattern
  pattern="$(orderbook_recorder_pattern "$market")"
  pgrep -f -- "$pattern" || true
}

stop_orderbook_recorder_for_market() {
  local market="$1"
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(orderbook_recorder_pids "$market")
  if [[ ${#pids[@]} -eq 0 ]]; then
    echo "No existing orderbook recorder found for market=${market} cycle=${CYCLE} surface=${SURFACE}"
    return 0
  fi
  echo "Stopping orderbook recorder market=${market} cycle=${CYCLE} surface=${SURFACE} pids=${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1
  local remaining=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && remaining+=("$pid")
  done < <(orderbook_recorder_pids "$market")
  if [[ ${#remaining[@]} -gt 0 ]]; then
    echo "Force stopping orderbook recorder market=${market} cycle=${CYCLE} surface=${SURFACE} pids=${remaining[*]}"
    kill -9 "${remaining[@]}" 2>/dev/null || true
  fi
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
if is_truthy "${V2_ORDERBOOK_FLEET_SKIP_CONDA:-0}"; then
  PYTHON_BIN="${PYTHON_BIN:-$(command -v python3 || true)}"
  if [[ -z "$PYTHON_BIN" || ! -x "$PYTHON_BIN" ]]; then
    echo "❌ V2_ORDERBOOK_FLEET_SKIP_CONDA=1 but no usable python3 found."
    exit 1
  fi
  echo "⚠️  Skip conda activation for orderbook fleet"
  echo "✓ Using Python: ${PYTHON_BIN}"
else
  pm15min_activate_python
fi

MARKETS_RAW="${V2_ORDERBOOK_FLEET_MARKETS:-btc,eth,sol,xrp}"
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
      echo "WARN: unsupported v2 orderbook market ignored: $market"
      ;;
  esac
done
if [[ ${#MARKETS_LIST[@]} -eq 0 ]]; then
  MARKETS_LIST=("btc" "eth" "sol" "xrp")
fi
MARKETS_CSV="$(IFS=,; echo "${MARKETS_LIST[*]}")"

CYCLE="${V2_ORDERBOOK_FLEET_CYCLE:-15m}"
SURFACE="${V2_ORDERBOOK_FLEET_SURFACE:-live}"
POLL_SEC="${V2_ORDERBOOK_FLEET_POLL_SEC:-0.35}"
TIMEOUT_SEC="${V2_ORDERBOOK_FLEET_TIMEOUT_SEC:-1.2}"
RECENT_WINDOW_MINUTES="${V2_ORDERBOOK_FLEET_RECENT_WINDOW_MINUTES:-15}"
MARKET_DEPTH="${V2_ORDERBOOK_FLEET_MARKET_DEPTH:-1}"
MARKET_START_OFFSET="${V2_ORDERBOOK_FLEET_MARKET_START_OFFSET:-0}"
ITERATIONS="${V2_ORDERBOOK_FLEET_ITERATIONS:-0}"
SLEEP_SEC="${V2_ORDERBOOK_FLEET_SLEEP_SEC:-$POLL_SEC}"
LOG_PATH="${V2_ORDERBOOK_FLEET_LOG_PATH:-$PROJECT_DIR/var/${SURFACE}/logs/entrypoints/orderbook_fleet_${CYCLE}_${SURFACE}.out}"
ALLOCATOR_ARENAS="${MALLOC_ARENA_MAX:-2}"
SCHEDULER_MODE="${PM15MIN_ORDERBOOK_FLEET_SCHEDULER_MODE:-process_per_market}"
STOP_MODE="${V2_ORDERBOOK_FLEET_STOP_MODE:-scope}"

export PM15MIN_ORDERBOOK_ASYNC_PERSIST="${PM15MIN_ORDERBOOK_ASYNC_PERSIST:-1}"
export PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES="${PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES:-1}"
export PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL="${PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL:-0}"

export V2_ORDERBOOK_FLEET_MARKETS="$MARKETS_CSV"
export V2_ORDERBOOK_FLEET_CYCLE="$CYCLE"
export V2_ORDERBOOK_FLEET_SURFACE="$SURFACE"
export V2_ORDERBOOK_FLEET_POLL_SEC="$POLL_SEC"
export V2_ORDERBOOK_FLEET_TIMEOUT_SEC="$TIMEOUT_SEC"
export V2_ORDERBOOK_FLEET_RECENT_WINDOW_MINUTES="$RECENT_WINDOW_MINUTES"
export V2_ORDERBOOK_FLEET_MARKET_DEPTH="$MARKET_DEPTH"
export V2_ORDERBOOK_FLEET_MARKET_START_OFFSET="$MARKET_START_OFFSET"
export V2_ORDERBOOK_FLEET_ITERATIONS="$ITERATIONS"
export V2_ORDERBOOK_FLEET_SLEEP_SEC="$SLEEP_SEC"
export PM15MIN_ORDERBOOK_FLEET_SCHEDULER_MODE="$SCHEDULER_MODE"

mkdir -p "$(dirname "$LOG_PATH")"
touch "$LOG_PATH"

echo "============================================="
echo "Starting v2 orderbook fleet"
echo "Project: $PROJECT_DIR"
echo "Markets: ${MARKETS_LIST[*]}"
echo "Cycle: $CYCLE"
echo "Surface: $SURFACE"
echo "Poll Sec: $POLL_SEC"
echo "Timeout Sec: $TIMEOUT_SEC"
echo "Recent Window Minutes: $RECENT_WINDOW_MINUTES"
echo "Market Depth: $MARKET_DEPTH"
echo "Market Start Offset: $MARKET_START_OFFSET"
echo "Iterations: $ITERATIONS"
echo "Scheduler Mode: $SCHEDULER_MODE"
echo "Stop Mode: $STOP_MODE"
echo "MALLOC_ARENA_MAX: $ALLOCATOR_ARENAS"
echo "Async Persist: ${PM15MIN_ORDERBOOK_ASYNC_PERSIST}"
echo "Max Pending Batches: ${PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES}"
echo "Drop Oldest When Full: ${PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL}"
echo "Wrapper Log: $LOG_PATH"
echo "============================================="

echo "Stopping previous v2 orderbook fleet processes..."
pkill -f "pm15min data run orderbook-fleet" || true
sleep 1
case "$STOP_MODE" in
  global)
    stop_all_orderbook_recorders
    ;;
  scope)
    for market in "${MARKETS_LIST[@]}"; do
      stop_orderbook_recorder_for_market "$market"
    done
    ;;
  none)
    echo "Skip stopping existing recorder processes (stop mode: none)"
    ;;
  *)
    echo "❌ Unsupported V2_ORDERBOOK_FLEET_STOP_MODE: $STOP_MODE"
    exit 1
    ;;
esac

cmd=(
  "$PYTHON_BIN" -c
  "import os; from pm15min.data.pipelines.orderbook_fleet import run_orderbook_recorder_fleet; run_orderbook_recorder_fleet(markets=os.environ['V2_ORDERBOOK_FLEET_MARKETS'], cycle=os.environ['V2_ORDERBOOK_FLEET_CYCLE'], surface=os.environ['V2_ORDERBOOK_FLEET_SURFACE'], poll_interval_sec=float(os.environ['V2_ORDERBOOK_FLEET_POLL_SEC']), orderbook_timeout_sec=float(os.environ['V2_ORDERBOOK_FLEET_TIMEOUT_SEC']), recent_window_minutes=int(os.environ['V2_ORDERBOOK_FLEET_RECENT_WINDOW_MINUTES']), market_depth=int(os.environ['V2_ORDERBOOK_FLEET_MARKET_DEPTH']), market_start_offset=int(os.environ['V2_ORDERBOOK_FLEET_MARKET_START_OFFSET']), iterations=int(os.environ['V2_ORDERBOOK_FLEET_ITERATIONS']), loop=True, sleep_sec=float(os.environ['V2_ORDERBOOK_FLEET_SLEEP_SEC']), root=os.environ.get('PM15MIN_PROJECT_DIR'), scheduler_mode=os.environ.get('PM15MIN_ORDERBOOK_FLEET_SCHEDULER_MODE'))"
)

nohup env MALLOC_ARENA_MAX="$ALLOCATOR_ARENAS" "${cmd[@]}" >> "$LOG_PATH" 2>&1 &

echo "started v2 orderbook fleet: pid=$!"
echo "  wrapper log: $LOG_PATH"
echo "  canonical recorder state/log: var/${SURFACE}/state/orderbooks/... and var/${SURFACE}/logs/data/recorders/..."
