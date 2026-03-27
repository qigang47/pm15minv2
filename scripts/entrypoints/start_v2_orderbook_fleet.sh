#!/bin/bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: start_v2_orderbook_fleet.sh

Env overrides:
  CONDA_ENV                           default: pm15min
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
  V2_ORDERBOOK_FLEET_LOG_PATH         default: var/live/logs/entrypoints/orderbook_fleet_<cycle>_<surface>.out
  MALLOC_ARENA_MAX                    default: 2
EOF
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
LOG_PATH="${V2_ORDERBOOK_FLEET_LOG_PATH:-$PROJECT_DIR/var/live/logs/entrypoints/orderbook_fleet_${CYCLE}_${SURFACE}.out}"
ALLOCATOR_ARENAS="${MALLOC_ARENA_MAX:-2}"

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
echo "MALLOC_ARENA_MAX: $ALLOCATOR_ARENAS"
echo "Wrapper Log: $LOG_PATH"
echo "============================================="

echo "Stopping previous v2 orderbook fleet processes..."
pkill -f "pm15min data run orderbook-fleet" || true
sleep 1

cmd=(
  "$PYTHON_BIN" -m pm15min data run orderbook-fleet
  --markets "$MARKETS_CSV"
  --cycle "$CYCLE"
  --surface "$SURFACE"
  --poll-interval-sec "$POLL_SEC"
  --timeout-sec "$TIMEOUT_SEC"
  --recent-window-minutes "$RECENT_WINDOW_MINUTES"
  --market-depth "$MARKET_DEPTH"
  --market-start-offset "$MARKET_START_OFFSET"
  --loop
  --iterations "$ITERATIONS"
  --sleep-sec "$SLEEP_SEC"
)

nohup env MALLOC_ARENA_MAX="$ALLOCATOR_ARENAS" "${cmd[@]}" >> "$LOG_PATH" 2>&1 &

echo "started v2 orderbook fleet: pid=$!"
echo "  wrapper log: $LOG_PATH"
echo "  canonical recorder state/log: var/live/state/orderbooks/... and var/live/logs/data/recorders/..."
