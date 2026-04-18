#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
STATE_DIR="$ROOT_DIR/var/research/autorun"
PID_PATH="$STATE_DIR/experiment-queue-supervisor.pid"
STATUS_PATH="$STATE_DIR/experiment-queue-supervisor.status.json"
LOG_PATH="$STATE_DIR/experiment-queue-supervisor.log"
STOP_FLAG="$STATE_DIR/experiment-queue-supervisor.stop.flag"
LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-15}"
MAX_LIVE_RUNS="${MAX_LIVE_RUNS:-16}"
MAX_QUEUED_ITEMS="${MAX_QUEUED_ITEMS:-24}"
MAX_REPAIR_ATTEMPTS="${MAX_REPAIR_ATTEMPTS:-3}"
TRACK_SLOT_CAPS_JSON="${TRACK_SLOT_CAPS_JSON:-{\"direction_dense\":8,\"reversal_dense\":8}}"
PM15MIN_EXPERIMENT_LAUNCH_MODE="${PM15MIN_EXPERIMENT_LAUNCH_MODE:-quick_screen}"
PM15MIN_QUICK_SCREEN_TOP_K="${PM15MIN_QUICK_SCREEN_TOP_K:-1}"
PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS="${PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS:-3}"
PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY="${PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY:-$MAX_LIVE_RUNS}"

export PM15MIN_EXPERIMENT_LAUNCH_MODE
export PM15MIN_QUICK_SCREEN_TOP_K
export PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS
export PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY

mkdir -p "$STATE_DIR"

write_status() {
  python3 - <<'PY' "$STATUS_PATH" "$PID_PATH" "$LOG_PATH" "$MAX_LIVE_RUNS" "$MAX_QUEUED_ITEMS" "$TRACK_SLOT_CAPS_JSON" "$1"
from __future__ import annotations
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

status_path = Path(sys.argv[1])
pid_path = Path(sys.argv[2])
log_path = Path(sys.argv[3])
max_live_runs = int(sys.argv[4])
max_queued_items = int(sys.argv[5])
track_slot_caps = json.loads(sys.argv[6])
state = sys.argv[7]

payload = {
    "state": state,
    "pid": int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else None,
    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "log_path": str(log_path),
    "max_live_runs": max_live_runs,
    "max_queued_items": max_queued_items,
    "track_slot_caps": track_slot_caps,
}
status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
PY
}

run_once() {
  PYTHONPATH="$ROOT_DIR/src" python3 "$ROOT_DIR/auto_research/experiment_queue.py" \
    --root "$ROOT_DIR" \
    supervise-once \
    --max-live-runs "$MAX_LIVE_RUNS" \
    --max-queued-items "$MAX_QUEUED_ITEMS" \
    --max-repair-attempts "$MAX_REPAIR_ATTEMPTS" \
    --track-slot-caps "$TRACK_SLOT_CAPS_JSON"
}

loop_body() {
  echo "$$" > "$PID_PATH"
  write_status "running"
  while true; do
    if [[ -f "$STOP_FLAG" ]]; then
      rm -f "$STOP_FLAG" "$PID_PATH"
      write_status "stopped"
      exit 0
    fi
    run_once >> "$LOG_PATH" 2>&1
    write_status "running"
    sleep "$LOOP_SLEEP_SEC"
  done
}

ACTION="${1:-start}"

case "$ACTION" in
  start)
    if [[ -f "$PID_PATH" ]]; then
      existing_pid="$(cat "$PID_PATH")"
      if kill -0 "$existing_pid" >/dev/null 2>&1; then
        echo "Queue supervisor already running with pid=$existing_pid"
        exit 0
      fi
      rm -f "$PID_PATH"
    fi
    rm -f "$STOP_FLAG"
    nohup "$SCRIPT_PATH" __run_loop >> "$LOG_PATH" 2>&1 &
    echo "Started queue supervisor"
    ;;
  stop)
    touch "$STOP_FLAG"
    if [[ -f "$PID_PATH" ]]; then
      pid="$(cat "$PID_PATH")"
      kill "$pid" >/dev/null 2>&1 || true
    fi
    echo "Stop requested"
    ;;
  restart)
    "$SCRIPT_PATH" stop || true
    sleep 1
    "$SCRIPT_PATH" start
    ;;
  once)
    run_once
    ;;
  status)
    cat "$STATUS_PATH"
    ;;
  __run_loop)
    loop_body
    ;;
  *)
    echo "usage: experiment_queue_supervisor.sh {start|stop|restart|once|status}" >&2
    exit 2
    ;;
esac
