#!/bin/bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage: start_v2_auto_redeem.sh

Env overrides:
  CONDA_ENV                              default: pm15min
  V2_AUTO_REDEEM_MARKET                  default: sol
  V2_AUTO_REDEEM_PROFILE                 default: deep_otm
  V2_AUTO_REDEEM_CYCLE_MINUTES           default: 15
  V2_AUTO_REDEEM_ADAPTER                 default: direct
  V2_AUTO_REDEEM_SLEEP_SEC               default: 7200
  V2_AUTO_REDEEM_ITERATIONS              default: 0
  V2_AUTO_REDEEM_MAX_CONDITIONS          default: <unset>
  V2_AUTO_REDEEM_DRY_RUN                 default: 0
  V2_AUTO_REDEEM_REFRESH_ACCOUNT_STATE   default: 1
  V2_AUTO_REDEEM_PERSIST                 default: 1
  V2_AUTO_REDEEM_LOG_PATH                default: var/live/logs/entrypoints/redeem_loop_<market>_<profile>.out
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
MARKET="${V2_AUTO_REDEEM_MARKET:-sol}"
PROFILE="${V2_AUTO_REDEEM_PROFILE:-deep_otm}"
CYCLE_MINUTES="${V2_AUTO_REDEEM_CYCLE_MINUTES:-15}"
ADAPTER="${V2_AUTO_REDEEM_ADAPTER:-direct}"
SLEEP_SEC="${V2_AUTO_REDEEM_SLEEP_SEC:-7200}"
ITERATIONS="${V2_AUTO_REDEEM_ITERATIONS:-0}"
MAX_CONDITIONS="${V2_AUTO_REDEEM_MAX_CONDITIONS:-}"
DRY_RUN="${V2_AUTO_REDEEM_DRY_RUN:-0}"
REFRESH_ACCOUNT_STATE="${V2_AUTO_REDEEM_REFRESH_ACCOUNT_STATE:-1}"
PERSIST="${V2_AUTO_REDEEM_PERSIST:-1}"
LOG_PATH="${V2_AUTO_REDEEM_LOG_PATH:-$PROJECT_DIR/var/live/logs/entrypoints/redeem_loop_${MARKET}_${PROFILE}.out}"

pm15min_activate_python

mkdir -p "$(dirname "$LOG_PATH")"
touch "$LOG_PATH"

cmd=(
  "$PYTHON_BIN" -m pm15min live redeem-loop
  --market "$MARKET"
  --profile "$PROFILE"
  --cycle-minutes "$CYCLE_MINUTES"
  --adapter "$ADAPTER"
  --loop
  --iterations "$ITERATIONS"
  --sleep-sec "$SLEEP_SEC"
)

if [[ -n "$MAX_CONDITIONS" ]]; then
  cmd+=(--max-conditions "$MAX_CONDITIONS")
fi
if is_truthy "$DRY_RUN"; then
  cmd+=(--dry-run)
fi
if ! is_truthy "$REFRESH_ACCOUNT_STATE"; then
  cmd+=(--no-refresh-account-state)
fi
if ! is_truthy "$PERSIST"; then
  cmd+=(--no-persist)
fi

echo "============================================="
echo "Starting v2 auto redeem"
echo "Project: $PROJECT_DIR"
echo "Market: $MARKET"
echo "Profile: $PROFILE"
echo "Cycle Minutes: $CYCLE_MINUTES"
echo "Adapter: $ADAPTER"
echo "Iterations: $ITERATIONS"
echo "Sleep Sec: $SLEEP_SEC"
echo "Dry Run: $DRY_RUN"
echo "Wrapper Log: $LOG_PATH"
echo "============================================="

echo "Stopping previous v2 auto-redeem loops for ${MARKET}/${PROFILE}..."
pkill -f "pm15min live redeem-loop --market ${MARKET} --profile ${PROFILE}" || true
sleep 1

nohup "${cmd[@]}" >> "$LOG_PATH" 2>&1 &

echo "started v2 auto redeem: pid=$!"
echo "  wrapper log: $LOG_PATH"
echo "  canonical redeem state/log: var/live/state/redeem_runner/... and var/live/logs/redeem_runner/..."
