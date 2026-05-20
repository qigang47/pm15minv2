#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ORDERBOOK_SCRIPT="$ROOT_DIR/scripts/entrypoints/start_v2_orderbook_fleet.sh"
PREWARM_SCRIPT="$ROOT_DIR/auto_research/prewarm_profitable_offset_pools.sh"
QUEUE_SCRIPT="$ROOT_DIR/auto_research/experiment_queue_supervisor.sh"
DIRECTION_SCRIPT="$ROOT_DIR/auto_research/start_direction_dense_sol_xrp.sh"
REVERSAL_SCRIPT="$ROOT_DIR/auto_research/start_reversal_dense_sol_xrp.sh"

MARKETS="${MARKETS:-btc,eth,sol,xrp}"
QUEUE_MARKETS="${PM15MIN_ALLOWED_QUEUE_MARKETS:-sol,xrp}"
ORDERBOOK_SURFACE="${ORDERBOOK_SURFACE:-live}"
ORDERBOOK_CYCLES="${ORDERBOOK_CYCLES:-15m,5m}"
DENSE_STACK_PREWARM_ENABLE="${DENSE_STACK_PREWARM_ENABLE:-0}"
MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-10}"
MAX_LAUNCHES_PER_PASS="${MAX_LAUNCHES_PER_PASS:-10}"
PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE="${PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE:-10}"
PM15MIN_QUICK_SCREEN_USE_POOL="${PM15MIN_QUICK_SCREEN_USE_POOL:-1}"
PM15MIN_QUICK_SCREEN_POOL_WORKERS="${PM15MIN_QUICK_SCREEN_POOL_WORKERS:-10}"
PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS="${PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS:-10}"
PM15MIN_MIN_AVAILABLE_MEM_GB="${PM15MIN_MIN_AVAILABLE_MEM_GB:-1}"

export MAX_LIVE_RUNS
export MAX_LAUNCHES_PER_PASS
export PM15MIN_QUEUE_QUICK_SCREEN_BATCH_SIZE
export PM15MIN_QUICK_SCREEN_USE_POOL
export PM15MIN_QUICK_SCREEN_POOL_WORKERS
export PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS
export PM15MIN_MIN_AVAILABLE_MEM_GB

start_orderbooks() {
  local cycle=""
  IFS=',' read -r -a cycles <<< "$ORDERBOOK_CYCLES"
  for cycle in "${cycles[@]}"; do
    cycle="$(echo "$cycle" | xargs)"
    [[ -z "$cycle" ]] && continue
    V2_ORDERBOOK_FLEET_MARKETS="$MARKETS" \
    V2_ORDERBOOK_FLEET_CYCLE="$cycle" \
    V2_ORDERBOOK_FLEET_SURFACE="$ORDERBOOK_SURFACE" \
      bash "$ORDERBOOK_SCRIPT"
  done
}

stop_orderbooks() {
  pkill -f "pm15min data run orderbook-fleet" || true
  pkill -f "run_orderbook_recorder; cfg = DataConfig.build" || true
}

stop_experiment_workers() {
  pkill -f "$ROOT_DIR/auto_research/run_one_experiment_background.sh" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/auto_research/run_one_experiment.sh" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/scripts/research/run_quick_screen_suite.py" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/scripts/research/run_quick_screen_queue_batch.py" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/scripts/research/run_quick_screen_pool.py" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/auto_research/run_quick_screen_queue_batch.sh" >/dev/null 2>&1 || true
  pkill -f "$ROOT_DIR/auto_research/run_quick_screen_pool.sh" >/dev/null 2>&1 || true
  pkill -f "research experiment run-suite" >/dev/null 2>&1 || true
  sleep 1
  pkill -9 -f "$ROOT_DIR/auto_research/run_one_experiment_background.sh" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/auto_research/run_one_experiment.sh" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/scripts/research/run_quick_screen_suite.py" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/scripts/research/run_quick_screen_queue_batch.py" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/scripts/research/run_quick_screen_pool.py" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/auto_research/run_quick_screen_queue_batch.sh" >/dev/null 2>&1 || true
  pkill -9 -f "$ROOT_DIR/auto_research/run_quick_screen_pool.sh" >/dev/null 2>&1 || true
  pkill -9 -f "research experiment run-suite" >/dev/null 2>&1 || true
}

status_orderbooks() {
  pgrep -af "run_orderbook_recorder; cfg = DataConfig.build" || true
}

start_optional_prewarm() {
  if [[ "$DENSE_STACK_PREWARM_ENABLE" == "1" ]]; then
    MARKETS="$MARKETS" "$PREWARM_SCRIPT" ensure &
  fi
}

ACTION="${1:-start}"

case "$ACTION" in
  start)
    start_orderbooks
    PM15MIN_ALLOWED_QUEUE_MARKETS="$QUEUE_MARKETS" "$QUEUE_SCRIPT" start
    "$DIRECTION_SCRIPT" start
    "$REVERSAL_SCRIPT" start
    start_optional_prewarm
    ;;
  restart)
    "$0" stop || true
    sleep 2
    "$0" start
    ;;
  stop)
    "$DIRECTION_SCRIPT" stop || true
    "$REVERSAL_SCRIPT" stop || true
    PM15MIN_ALLOWED_QUEUE_MARKETS="$QUEUE_MARKETS" "$QUEUE_SCRIPT" stop || true
    stop_experiment_workers
    stop_orderbooks
    ;;
  status)
    echo "===ORDERBOOKS==="
    status_orderbooks
    echo "===PREWARM==="
    "$PREWARM_SCRIPT" status || true
    echo "===QUEUE==="
    PM15MIN_ALLOWED_QUEUE_MARKETS="$QUEUE_MARKETS" "$QUEUE_SCRIPT" status || true
    echo "===DIRECTION==="
    AUTORUN_DIR="$ROOT_DIR/var/research/autorun/direction_dense_sol_xrp" "$ROOT_DIR/auto_research/status_autorun.sh" || true
    echo "===REVERSAL==="
    AUTORUN_DIR="$ROOT_DIR/var/research/autorun/reversal_dense_sol_xrp" "$ROOT_DIR/auto_research/status_autorun.sh" || true
    ;;
  *)
    echo "usage: start_dense_stack.sh {start|restart|stop|status}" >&2
    exit 2
    ;;
esac
