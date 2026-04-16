#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

SUITE_NAME=""
RUN_LABEL=""
MARKET=""
TIMEOUT_SEC=""
LOG_PATH=""
STDOUT_PATH=""
PID_PATH=""

usage() {
  echo "usage: run_one_experiment_background.sh --suite <suite> --run-label <run-label> [--market <market>] [--timeout-sec <sec>] [--log-path <path>] [--stdout-path <path>] [--pid-path <path>]" >&2
}

find_existing_pid() {
  local suite="$1"
  local run_label="$2"
  local pid=""

  if pid="$(pgrep -f "pm15min research experiment run-suite --suite ${suite} --run-label ${run_label}" | head -n 1)"; then
    if [[ -n "$pid" ]]; then
      printf '%s' "$pid"
      return 0
    fi
  fi
  if pid="$(pgrep -f "run_one_experiment.sh --suite ${suite} --run-label ${run_label}" | head -n 1)"; then
    if [[ -n "$pid" ]]; then
      printf '%s' "$pid"
      return 0
    fi
  fi
  return 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    --suite)
      SUITE_NAME="$2"
      shift 2
      ;;
    --run-label)
      RUN_LABEL="$2"
      shift 2
      ;;
    --market)
      MARKET="$2"
      shift 2
      ;;
    --timeout-sec)
      TIMEOUT_SEC="$2"
      shift 2
      ;;
    --log-path)
      LOG_PATH="$2"
      shift 2
      ;;
    --stdout-path)
      STDOUT_PATH="$2"
      shift 2
      ;;
    --pid-path)
      PID_PATH="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "$SUITE_NAME" || -z "$RUN_LABEL" ]]; then
  usage
  exit 2
fi

if [[ -z "$LOG_PATH" ]]; then
  LOG_PATH="$ROOT_DIR/var/research/logs/autorun/${RUN_LABEL}.log"
fi
if [[ -z "$STDOUT_PATH" ]]; then
  STDOUT_PATH="$ROOT_DIR/var/research/logs/autorun/${RUN_LABEL}.stdout.log"
fi
if [[ -z "$PID_PATH" ]]; then
  PID_PATH="$ROOT_DIR/var/research/logs/autorun/${RUN_LABEL}.pid"
fi

mkdir -p "$(dirname "$LOG_PATH")" "$(dirname "$STDOUT_PATH")" "$(dirname "$PID_PATH")"

if existing_pid="$(find_existing_pid "$SUITE_NAME" "$RUN_LABEL")"; then
  printf '%s\n' "$existing_pid" > "$PID_PATH"
  echo "[run_one_experiment_background] reusing pid=$existing_pid suite=$SUITE_NAME run_label=$RUN_LABEL"
  echo "$existing_pid"
  exit 0
fi

cmd=("$ROOT_DIR/auto_research/run_one_experiment.sh" --suite "$SUITE_NAME" --run-label "$RUN_LABEL")
if [[ -n "$MARKET" ]]; then
  cmd+=(--market "$MARKET")
fi
if [[ -n "$TIMEOUT_SEC" ]]; then
  cmd+=(--timeout-sec "$TIMEOUT_SEC")
fi
cmd+=(--log-path "$LOG_PATH")

nohup "${cmd[@]}" >"$STDOUT_PATH" 2>&1 &
pid="$!"
printf '%s\n' "$pid" > "$PID_PATH"
echo "[run_one_experiment_background] launched pid=$pid suite=$SUITE_NAME run_label=$RUN_LABEL"
echo "$pid"
