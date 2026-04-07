#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
source "$ROOT_DIR/scripts/entrypoints/_python_env.sh"

SUITE_NAME=""
RUN_LABEL=""
MARKET=""
TIMEOUT_SEC=""
LOG_PATH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      echo "usage: run_one_experiment.sh --suite <suite> --run-label <run-label> [--market <market>] [--timeout-sec <sec>] [--log-path <path>]"
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
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$SUITE_NAME" || -z "$RUN_LABEL" ]]; then
  echo "usage: run_one_experiment.sh --suite <suite> --run-label <run-label> [--market <market>] [--timeout-sec <sec>] [--log-path <path>]" >&2
  exit 2
fi

export PM15MIN_PROJECT_DIR="$ROOT_DIR"
pm15min_load_project_env
pm15min_activate_python

export PYTHONPATH="$ROOT_DIR/src"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/pm15min-mpl}"
export PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES:-1}"
mkdir -p "$MPLCONFIGDIR"

if [[ -z "$LOG_PATH" ]]; then
  LOG_PATH="$ROOT_DIR/var/research/logs/autorun/${RUN_LABEL}.log"
fi
mkdir -p "$(dirname "$LOG_PATH")"

CMD=("$PYTHON_BIN" -m pm15min research experiment run-suite --suite "$SUITE_NAME" --run-label "$RUN_LABEL")
if [[ -n "$MARKET" ]]; then
  CMD+=(--market "$MARKET")
fi

TIMEOUT_PREFIX=()
if [[ -n "$TIMEOUT_SEC" ]]; then
  if command -v gtimeout >/dev/null 2>&1; then
    TIMEOUT_PREFIX=(gtimeout "$TIMEOUT_SEC")
  elif command -v timeout >/dev/null 2>&1; then
    TIMEOUT_PREFIX=(timeout "$TIMEOUT_SEC")
  else
    echo "WARN: timeout command not found; running without timeout" | tee -a "$LOG_PATH"
  fi
fi

{
  echo "[run_one_experiment] suite=$SUITE_NAME run_label=$RUN_LABEL market=${MARKET:-all}"
  if [[ ${#TIMEOUT_PREFIX[@]} -gt 0 ]]; then
    "${TIMEOUT_PREFIX[@]}" "${CMD[@]}"
  else
    "${CMD[@]}"
  fi
} 2>&1 | tee "$LOG_PATH"

RUN_DIR="$("$PYTHON_BIN" - <<'PY' "$ROOT_DIR" "$SUITE_NAME" "$RUN_LABEL"
from pathlib import Path
import sys
from pm15min.research.layout import ResearchLayout

root = Path(sys.argv[1]).resolve()
suite = sys.argv[2]
run_label = sys.argv[3]
layout = ResearchLayout.discover(root)
print(layout.experiment_run_dir(suite, run_label))
PY
)"

echo "$RUN_DIR"
