#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
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
export PM15MIN_MANAGED_PROXY_ENABLE="${PM15MIN_MANAGED_PROXY_ENABLE:-1}"
pm15min_load_project_env
pm15min_load_managed_proxy_env
pm15min_activate_python

export PYTHONPATH="$ROOT_DIR/src"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/pm15min-mpl}"
export PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES="${PM15MIN_BACKTEST_RUNTIME_CACHE_MAX_ENTRIES:-1}"
export PM15MIN_EXPERIMENT_LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-formal}"
export PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"
export PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-3}"
export PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-16}"
export PM15MIN_EXPERIMENT_CPU_THREADS="${PM15MIN_EXPERIMENT_CPU_THREADS:-}"
mkdir -p "$MPLCONFIGDIR"

if [[ -z "$PM15MIN_EXPERIMENT_CPU_THREADS" ]]; then
  cpu_count=""
  if command -v nproc >/dev/null 2>&1; then
    cpu_count="$(nproc)"
  elif command -v getconf >/dev/null 2>&1; then
    cpu_count="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  fi
  if [[ ! "$cpu_count" =~ ^[0-9]+$ ]] || [[ "$cpu_count" -lt 1 ]]; then
    cpu_count=1
  fi
  expected_concurrency="$PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY"
  if [[ ! "$expected_concurrency" =~ ^[0-9]+$ ]] || [[ "$expected_concurrency" -lt 1 ]]; then
    expected_concurrency=4
  fi
  PM15MIN_EXPERIMENT_CPU_THREADS="$(( cpu_count / expected_concurrency ))"
  if [[ "$PM15MIN_EXPERIMENT_CPU_THREADS" -lt 1 ]]; then
    PM15MIN_EXPERIMENT_CPU_THREADS=1
  fi
  export PM15MIN_EXPERIMENT_CPU_THREADS
fi

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export GOTO_NUM_THREADS="${GOTO_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export BLIS_NUM_THREADS="${BLIS_NUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-$PM15MIN_EXPERIMENT_CPU_THREADS}"

if [[ -z "$LOG_PATH" ]]; then
  LOG_PATH="$ROOT_DIR/var/research/logs/autorun/${RUN_LABEL}.log"
fi
mkdir -p "$(dirname "$LOG_PATH")"

LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-formal}"
case "$LAUNCH_MODE" in
  formal)
    CMD=("$PYTHON_BIN" -m pm15min research experiment run-suite --suite "$SUITE_NAME" --run-label "$RUN_LABEL")
    if [[ -n "$MARKET" ]]; then
      CMD+=(--market "$MARKET")
    fi
    ;;
  quick_screen)
    CMD=(
      "$PYTHON_BIN"
      "$ROOT_DIR/scripts/research/run_quick_screen_suite.py"
      --suite "$SUITE_NAME"
      --run-label "$RUN_LABEL"
      --top-k "${PM15MIN_QUICK_SCREEN_TOP_K}"
    )
    ;;
  *)
    echo "unknown PM15MIN_EXPERIMENT_LAUNCH_MODE: $LAUNCH_MODE" >&2
    exit 2
    ;;
esac

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
  echo "[run_one_experiment] mode=$LAUNCH_MODE suite=$SUITE_NAME run_label=$RUN_LABEL market=${MARKET:-all}"
  echo "[run_one_experiment] cpu_threads=$PM15MIN_EXPERIMENT_CPU_THREADS expected_concurrency=$PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY"
  echo "[run_one_experiment] quick_screen_train_parallel_workers=$PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS"
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
