#!/bin/bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: start_v2_live_trading.sh [-f] [-o] [-r]

Options:
  -f    Start the v2 live foundation loops for the selected markets before runner loops
  -o    Start the v2 orderbook fleet for the selected markets before runner loops
  -r    Start the v2 auto-redeem loops for the selected markets after runner loops
  -h    Show help

Env overrides:
  CONDA_ENV                     default: pm15min
  V2_LIVE_MARKETS               default: sol,xrp
  V2_LIVE_PROFILE               default: deep_otm
  V2_LIVE_CYCLE_MINUTES         default: 15
  V2_LIVE_ADAPTER               default: direct
  V2_LIVE_ITERATIONS            default: 0
  V2_LIVE_SLEEP_SEC             default: 0.35
  V2_LIVE_DRY_RUN_SIDE_EFFECTS  default: 0
  V2_LIVE_NO_SIDE_EFFECTS       default: 0
  V2_LIVE_NO_FOUNDATION         default: 0
  V2_LIVE_NO_DIRECT_ORACLE      default: 0
  V2_LIVE_NO_ORDERBOOKS         default: 0
  V2_LIVE_LOG_DIR               default: var/live/logs/entrypoints
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

ENABLE_ORDERBOOK_FLEET=0
ENABLE_AUTO_REDEEM=0
ENABLE_FOUNDATION=0

while getopts "hfor" opt; do
  case "$opt" in
    h)
      usage
      exit 0
      ;;
    f)
      ENABLE_FOUNDATION=1
      ;;
    o)
      ENABLE_ORDERBOOK_FLEET=1
      ;;
    r)
      ENABLE_AUTO_REDEEM=1
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

MARKETS_RAW="${V2_LIVE_MARKETS:-sol,xrp}"
MARKETS_CSV="$(echo "$MARKETS_RAW" | tr '[:space:]' ',' | tr -s ',')"
IFS=',' read -r -a RAW_MARKETS <<< "$MARKETS_CSV"
MARKETS_LIST=()
for market in "${RAW_MARKETS[@]}"; do
  market="$(echo "$market" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$market" in
    sol|xrp)
      if [[ ! " ${MARKETS_LIST[*]} " =~ [[:space:]]${market}[[:space:]] ]]; then
        MARKETS_LIST+=("$market")
      fi
      ;;
    "")
      ;;
    *)
      echo "❌ Unsupported v2 canonical live market: $market"
      echo "   Canonical v2 live scope only supports: sol,xrp"
      exit 1
      ;;
  esac
done
if [[ ${#MARKETS_LIST[@]} -eq 0 ]]; then
  MARKETS_LIST=("sol" "xrp")
fi
MARKETS_CSV="$(IFS=,; echo "${MARKETS_LIST[*]}")"

PROFILE="${V2_LIVE_PROFILE:-deep_otm}"
if [[ "$PROFILE" != "deep_otm" ]]; then
  echo "❌ Unsupported v2 canonical live profile: $PROFILE"
  echo "   Canonical v2 live scope only supports: deep_otm"
  exit 1
fi

CYCLE_MINUTES="${V2_LIVE_CYCLE_MINUTES:-15}"
ADAPTER="${V2_LIVE_ADAPTER:-direct}"
ITERATIONS="${V2_LIVE_ITERATIONS:-0}"
SLEEP_SEC="${V2_LIVE_SLEEP_SEC:-0.35}"
DRY_RUN_SIDE_EFFECTS="${V2_LIVE_DRY_RUN_SIDE_EFFECTS:-0}"
NO_SIDE_EFFECTS="${V2_LIVE_NO_SIDE_EFFECTS:-0}"
NO_FOUNDATION="${V2_LIVE_NO_FOUNDATION:-0}"
NO_DIRECT_ORACLE="${V2_LIVE_NO_DIRECT_ORACLE:-0}"
NO_ORDERBOOKS="${V2_LIVE_NO_ORDERBOOKS:-0}"
LOG_DIR="${V2_LIVE_LOG_DIR:-$PROJECT_DIR/var/live/logs/entrypoints}"

mkdir -p "$LOG_DIR"

echo "============================================="
echo "Starting v2 canonical live runner loops"
echo "Project: $PROJECT_DIR"
echo "Markets: ${MARKETS_LIST[*]}"
echo "Profile: $PROFILE"
echo "Cycle Minutes: $CYCLE_MINUTES"
echo "Adapter: $ADAPTER"
echo "Iterations: $ITERATIONS"
echo "Sleep Sec: $SLEEP_SEC"
echo "Dry Run Side Effects: $DRY_RUN_SIDE_EFFECTS"
echo "No Side Effects: $NO_SIDE_EFFECTS"
echo "No Foundation: $NO_FOUNDATION"
echo "No Direct Oracle: $NO_DIRECT_ORACLE"
echo "No Orderbooks: $NO_ORDERBOOKS"
echo "============================================="

if [[ "$ENABLE_FOUNDATION" == "1" ]]; then
  echo "Starting v2 live foundation for markets: $MARKETS_CSV"
  V2_LIVE_FOUNDATION_MARKETS="$MARKETS_CSV" bash "$SCRIPT_DIR/start_v2_live_foundation.sh"
  echo ""
fi

if [[ "$ENABLE_ORDERBOOK_FLEET" == "1" ]]; then
  echo "Starting v2 orderbook fleet for markets: $MARKETS_CSV"
  V2_ORDERBOOK_FLEET_MARKETS="$MARKETS_CSV" bash "$SCRIPT_DIR/start_v2_orderbook_fleet.sh"
  echo ""
fi

for market in "${MARKETS_LIST[@]}"; do
  echo "Preflight $market ..."
  "$PYTHON_BIN" -m pm15min live check-trading-gateway \
    --market "$market" \
    --profile "$PROFILE" \
    --cycle-minutes "$CYCLE_MINUTES" \
    --adapter "$ADAPTER" \
    --probe-open-orders \
    --probe-positions

  "$PYTHON_BIN" -m pm15min live runner-once \
    --market "$market" \
    --profile "$PROFILE" \
    --cycle-minutes "$CYCLE_MINUTES" \
    --target direction \
    --adapter "$ADAPTER" \
    --dry-run-side-effects

  "$PYTHON_BIN" -m pm15min live show-ready \
    --market "$market" \
    --profile "$PROFILE" \
    --cycle-minutes "$CYCLE_MINUTES" \
    --target direction \
    --adapter "$ADAPTER"

  "$PYTHON_BIN" -m pm15min live show-latest-runner \
    --market "$market" \
    --profile "$PROFILE" \
    --cycle-minutes "$CYCLE_MINUTES" \
    --target direction \
    --risk-only
  echo ""
done

echo "Stopping previous v2 runner loops for profile=${PROFILE} ..."
pkill -f "pm15min live runner-loop .*--profile ${PROFILE}" || true
sleep 1

for market in "${MARKETS_LIST[@]}"; do
  log_path="$LOG_DIR/runner_loop_${market}_${PROFILE}.out"
  touch "$log_path"
  cmd=(
    "$PYTHON_BIN" -m pm15min live runner-loop
    --market "$market"
    --profile "$PROFILE"
    --cycle-minutes "$CYCLE_MINUTES"
    --target direction
    --adapter "$ADAPTER"
    --iterations "$ITERATIONS"
    --sleep-sec "$SLEEP_SEC"
  )
  if is_truthy "$DRY_RUN_SIDE_EFFECTS"; then
    cmd+=(--dry-run-side-effects)
  fi
  if is_truthy "$NO_SIDE_EFFECTS"; then
    cmd+=(--no-side-effects)
  fi
  if is_truthy "$NO_FOUNDATION"; then
    cmd+=(--no-foundation)
  fi
  if is_truthy "$NO_DIRECT_ORACLE"; then
    cmd+=(--no-direct-oracle)
  fi
  if is_truthy "$NO_ORDERBOOKS"; then
    cmd+=(--no-orderbooks)
  fi

  nohup "${cmd[@]}" >> "$log_path" 2>&1 &
  echo "started v2 runner-loop: market=${market} pid=$!"
  echo "  wrapper log: $log_path"
done

if [[ "$ENABLE_AUTO_REDEEM" == "1" ]]; then
  echo ""
  echo "Starting v2 auto-redeem loops ..."
  for market in "${MARKETS_LIST[@]}"; do
    V2_AUTO_REDEEM_MARKET="$market" \
    V2_AUTO_REDEEM_PROFILE="$PROFILE" \
    V2_AUTO_REDEEM_CYCLE_MINUTES="$CYCLE_MINUTES" \
    V2_AUTO_REDEEM_ADAPTER="$ADAPTER" \
    bash "$SCRIPT_DIR/start_v2_auto_redeem.sh"
  done
fi

echo ""
echo "Canonical operator checks:"
echo "  PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter ${ADAPTER}"
echo "  PYTHONPATH=src python -m pm15min live show-latest-runner --market sol --profile deep_otm --risk-only"
