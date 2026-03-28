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
  PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC  default: 60
  PM15MIN_RUNNER_ENABLE_CANCEL_POLICY       default: 0
  PM15MIN_RUNNER_ENABLE_REDEEM_POLICY       default: 0
  PM15MIN_RUNNER_LIQUIDITY_SYNC_INTERVAL_SEC default: 0
  PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC    default: 60
  PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC      default: 0
  PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS      default: 384
  PM15MIN_LIVE_FEATURE_TAIL_CYCLES          default: 2
  PM15MIN_LIVE_BUNDLE_CACHE_SEC             default: 0
  PM15MIN_LIVE_WINDOW_SIGNAL_CACHE          default: 1
  PM15MIN_LIVE_DECISION_DEPTH_ENFORCED      default: 0
  PM15MIN_RUNNER_PERSIST_PIPELINE           default: 1
  PM15MIN_RUNNER_PERSIST_SIDE_EFFECTS       default: 1
  PM15MIN_RUNNER_SUMMARY_PERSIST_INTERVAL_SEC default: 0
  PM15MIN_RUNNER_SUMMARY_PERSIST_ON_CHANGE  default: 1
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

acquire_script_lock() {
  local lock_root="$1"
  local lock_dir="$2"
  mkdir -p "$lock_root"
  if mkdir "$lock_dir" 2>/dev/null; then
    printf '%s\n' "$$" > "$lock_dir/pid"
    return 0
  fi
  local holder_pid=""
  if [[ -f "$lock_dir/pid" ]]; then
    holder_pid="$(cat "$lock_dir/pid" 2>/dev/null || true)"
  fi
  if [[ -n "$holder_pid" ]] && kill -0 "$holder_pid" 2>/dev/null; then
    echo "Another start_v2_live_trading.sh is already running."
    echo "  holder pid: $holder_pid"
    echo "  lock: $lock_dir"
    return 1
  fi
  echo "Removing stale script lock: $lock_dir"
  rm -rf "$lock_dir"
  if mkdir "$lock_dir" 2>/dev/null; then
    printf '%s\n' "$$" > "$lock_dir/pid"
    return 0
  fi
  echo "Another start_v2_live_trading.sh is already running."
  [[ -n "$holder_pid" ]] && echo "  holder pid: $holder_pid"
  echo "  lock: $lock_dir"
  return 1
}

release_script_lock() {
  local lock_dir="$1"
  local holder_pid=""
  if [[ -f "$lock_dir/pid" ]]; then
    holder_pid="$(cat "$lock_dir/pid" 2>/dev/null || true)"
  fi
  if [[ -z "$holder_pid" || "$holder_pid" == "$$" ]]; then
    rm -rf "$lock_dir"
  fi
}

runner_loop_pattern() {
  local market="$1"
  printf '%s' "pm15min live runner-loop --market ${market} --profile ${PROFILE} --cycle-minutes ${CYCLE_MINUTES} --target direction --adapter ${ADAPTER}"
}

runner_loop_pids() {
  local market="$1"
  local pattern
  pattern="$(runner_loop_pattern "$market")"
  pgrep -f -- "$pattern" || true
}

stop_runner_loop_for_market() {
  local market="$1"
  local pids=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && pids+=("$pid")
  done < <(runner_loop_pids "$market")
  if [[ ${#pids[@]} -eq 0 ]]; then
    echo "No existing runner-loop found for market=${market}"
    return 0
  fi
  echo "Stopping runner-loop market=${market} pids=${pids[*]}"
  kill "${pids[@]}" 2>/dev/null || true
  sleep 1
  local remaining=()
  while IFS= read -r pid; do
    [[ -n "$pid" ]] && remaining+=("$pid")
  done < <(runner_loop_pids "$market")
  if [[ ${#remaining[@]} -gt 0 ]]; then
    echo "Force stopping runner-loop market=${market} pids=${remaining[*]}"
    kill -9 "${remaining[@]}" 2>/dev/null || true
  fi
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
    btc|eth|sol|xrp)
      if [[ ! " ${MARKETS_LIST[*]} " =~ [[:space:]]${market}[[:space:]] ]]; then
        MARKETS_LIST+=("$market")
      fi
      ;;
    "")
      ;;
    *)
      echo "❌ Unsupported v2 canonical live market: $market"
      echo "   Canonical v2 live scope only supports: btc,eth,sol,xrp"
      exit 1
      ;;
  esac
done
if [[ ${#MARKETS_LIST[@]} -eq 0 ]]; then
  MARKETS_LIST=("sol" "xrp")
fi
MARKETS_CSV="$(IFS=,; echo "${MARKETS_LIST[*]}")"

PROFILE="${V2_LIVE_PROFILE:-deep_otm}"
if [[ "$PROFILE" != "deep_otm" && "$PROFILE" != "deep_otm_baseline" ]]; then
  echo "❌ Unsupported v2 canonical live profile: $PROFILE"
  echo "   Canonical v2 live scope only supports: deep_otm, deep_otm_baseline"
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
export PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC="${PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC:-60}"
export PM15MIN_RUNNER_ENABLE_CANCEL_POLICY="${PM15MIN_RUNNER_ENABLE_CANCEL_POLICY:-0}"
export PM15MIN_RUNNER_ENABLE_REDEEM_POLICY="${PM15MIN_RUNNER_ENABLE_REDEEM_POLICY:-0}"
export PM15MIN_RUNNER_LIQUIDITY_SYNC_INTERVAL_SEC="${PM15MIN_RUNNER_LIQUIDITY_SYNC_INTERVAL_SEC:-0}"
export PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC="${PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC:-60}"
export PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC="${PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC:-0}"
export PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS="${PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS:-384}"
export PM15MIN_LIVE_FEATURE_TAIL_CYCLES="${PM15MIN_LIVE_FEATURE_TAIL_CYCLES:-2}"
export PM15MIN_LIVE_BUNDLE_CACHE_SEC="${PM15MIN_LIVE_BUNDLE_CACHE_SEC:-0}"
export PM15MIN_LIVE_WINDOW_SIGNAL_CACHE="${PM15MIN_LIVE_WINDOW_SIGNAL_CACHE:-1}"
export PM15MIN_LIVE_DECISION_DEPTH_ENFORCED="${PM15MIN_LIVE_DECISION_DEPTH_ENFORCED:-0}"
export PM15MIN_RUNNER_PERSIST_PIPELINE="${PM15MIN_RUNNER_PERSIST_PIPELINE:-1}"
export PM15MIN_RUNNER_PERSIST_SIDE_EFFECTS="${PM15MIN_RUNNER_PERSIST_SIDE_EFFECTS:-1}"
export PM15MIN_RUNNER_SUMMARY_PERSIST_INTERVAL_SEC="${PM15MIN_RUNNER_SUMMARY_PERSIST_INTERVAL_SEC:-0}"
export PM15MIN_RUNNER_SUMMARY_PERSIST_ON_CHANGE="${PM15MIN_RUNNER_SUMMARY_PERSIST_ON_CHANGE:-1}"
LOCK_ROOT="${PROJECT_DIR}/var/live/locks"
SCRIPT_LOCK_DIR="${LOCK_ROOT}/start_v2_live_trading.profile=${PROFILE}.cycle=${CYCLE_MINUTES}.adapter=${ADAPTER}"

mkdir -p "$LOG_DIR"
acquire_script_lock "$LOCK_ROOT" "$SCRIPT_LOCK_DIR"
trap 'release_script_lock "$SCRIPT_LOCK_DIR"' EXIT

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
echo "Account Sync Interval Sec: $PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC"
echo "Enable Cancel Policy: $PM15MIN_RUNNER_ENABLE_CANCEL_POLICY"
echo "Enable Redeem Policy: $PM15MIN_RUNNER_ENABLE_REDEEM_POLICY"
echo "Liquidity Sync Interval Sec: $PM15MIN_RUNNER_LIQUIDITY_SYNC_INTERVAL_SEC"
echo "Live Account Context Cache Sec: $PM15MIN_LIVE_ACCOUNT_CONTEXT_CACHE_SEC"
echo "Live Feature Frame Cache Sec: $PM15MIN_LIVE_FEATURE_FRAME_CACHE_SEC"
echo "Live Feature Build Tail Bars: $PM15MIN_LIVE_FEATURE_BUILD_TAIL_BARS"
echo "Live Feature Tail Cycles: $PM15MIN_LIVE_FEATURE_TAIL_CYCLES"
echo "Live Bundle Cache Sec: $PM15MIN_LIVE_BUNDLE_CACHE_SEC"
echo "Live Window Signal Cache: $PM15MIN_LIVE_WINDOW_SIGNAL_CACHE"
echo "Live Decision Depth Enforced: $PM15MIN_LIVE_DECISION_DEPTH_ENFORCED"
echo "Runner Persist Pipeline: $PM15MIN_RUNNER_PERSIST_PIPELINE"
echo "Runner Persist Side Effects: $PM15MIN_RUNNER_PERSIST_SIDE_EFFECTS"
echo "Runner Summary Persist Interval Sec: $PM15MIN_RUNNER_SUMMARY_PERSIST_INTERVAL_SEC"
echo "Runner Summary Persist On Change: $PM15MIN_RUNNER_SUMMARY_PERSIST_ON_CHANGE"
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

  runner_once_cmd=(
    "$PYTHON_BIN" -m pm15min live runner-once
    --market "$market"
    --profile "$PROFILE"
    --cycle-minutes "$CYCLE_MINUTES"
    --target direction
    --adapter "$ADAPTER"
    --dry-run-side-effects
    --no-persist
  )
  if is_truthy "$NO_FOUNDATION"; then
    runner_once_cmd+=(--no-foundation)
  fi
  if is_truthy "$NO_DIRECT_ORACLE"; then
    runner_once_cmd+=(--no-direct-oracle)
  fi
  if is_truthy "$NO_ORDERBOOKS"; then
    runner_once_cmd+=(--no-orderbooks)
  fi
  if is_truthy "$NO_SIDE_EFFECTS"; then
    runner_once_cmd+=(--no-side-effects)
  fi
  "${runner_once_cmd[@]}"
  echo ""
done

echo "Stopping previous v2 runner loops ..."
for market in "${MARKETS_LIST[@]}"; do
  stop_runner_loop_for_market "$market"
done

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
  pid="$!"
  sleep 1
  active_pids=()
  while IFS= read -r active_pid; do
    [[ -n "$active_pid" ]] && active_pids+=("$active_pid")
  done < <(runner_loop_pids "$market")
  if [[ ${#active_pids[@]} -eq 0 ]]; then
    echo "failed to start v2 runner-loop: market=${market}"
    exit 1
  fi
  if [[ ${#active_pids[@]} -gt 1 ]]; then
    echo "duplicate runner-loop detected after start: market=${market} pids=${active_pids[*]}"
    exit 1
  fi
  echo "started v2 runner-loop: market=${market} pid=${active_pids[0]} requested_pid=${pid}"
  echo "  wrapper log: $log_path"
  echo "  match pattern: $(runner_loop_pattern "$market")"
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
echo "  preflight runner-once used --no-persist, so show-ready/show-latest-runner will reflect the new daemon after its first persisted iteration"
echo "  PYTHONPATH=src python -m pm15min live show-ready --market sol --profile deep_otm --adapter ${ADAPTER}"
echo "  PYTHONPATH=src python -m pm15min live show-latest-runner --market sol --profile deep_otm --risk-only"
