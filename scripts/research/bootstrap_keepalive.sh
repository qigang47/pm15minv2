#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_PATH="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
STATE_DIR="$ROOT_DIR/var/research/bootstrap-keepalive"
PID_PATH="$STATE_DIR/supervisor.pid"
STATUS_PATH="$STATE_DIR/status.json"
LOG_PATH="$STATE_DIR/supervisor.log"
STOP_FLAG="$STATE_DIR/stop.flag"
LOOP_SLEEP_SEC="${LOOP_SLEEP_SEC:-30}"

mkdir -p "$STATE_DIR"

markets=(btc eth sol xrp)

slot_suite() {
  case "$1" in
    btc) printf '%s' 'baseline_focus_feature_search_btc_reversal_40plus_2usd_5max_20260409' ;;
    eth) printf '%s' 'baseline_focus_feature_search_eth_reversal_40plus_2usd_5max_20260409' ;;
    sol) printf '%s' 'baseline_focus_feature_search_sol_reversal_40main_2usd_5max_20260412' ;;
    xrp) printf '%s' 'baseline_focus_feature_search_xrp_reversal_40plus_2usd_5max_20260409' ;;
    *) return 1 ;;
  esac
}

slot_run_label() {
  case "$1" in
    btc) printf '%s' 'auto_btc_40plus_2usd_5max_reset_20260412' ;;
    eth) printf '%s' 'auto_eth_40plus_2usd_5max_reset_20260412' ;;
    sol) printf '%s' 'auto_sol_40main_2usd_5max_frozen_20260412_cycle002' ;;
    xrp) printf '%s' 'auto_xrp_40plus_2usd_5max_reset_20260412' ;;
    *) return 1 ;;
  esac
}

slot_log_path() {
  local market="$1"
  printf '%s' "$ROOT_DIR/sessions/deep_otm_baseline_40factor_2usd_max5_autoresearch/bootstrap/$(slot_run_label "$market").log"
}

slot_pid_path() {
  local market="$1"
  printf '%s' "$STATE_DIR/${market}.pid"
}

slot_stdout_path() {
  local market="$1"
  printf '%s' "$STATE_DIR/${market}.stdout.log"
}

slot_run_dir() {
  local market="$1"
  printf '%s' "$ROOT_DIR/research/experiments/runs/suite=$(slot_suite "$market")/run=$(slot_run_label "$market")"
}

slot_total_cases() {
  local market="$1"
  python3 - <<'PY' "$ROOT_DIR" "$(slot_suite "$market")"
from __future__ import annotations
import json
import sys
from pathlib import Path

root = Path(sys.argv[1])
suite_name = sys.argv[2]
path = root / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
payload = json.loads(path.read_text(encoding="utf-8"))
markets = payload.get("markets") or []
print(len(markets))
PY
}

slot_is_complete() {
  local market="$1"
  local run_dir summary_path total_cases
  run_dir="$(slot_run_dir "$market")"
  summary_path="$run_dir/summary.json"
  if [[ ! -f "$summary_path" ]]; then
    return 1
  fi
  total_cases="$(slot_total_cases "$market")"
  python3 - <<'PY' "$summary_path" "$total_cases"
from __future__ import annotations
import json
import sys
from pathlib import Path

summary_path = Path(sys.argv[1])
expected = int(sys.argv[2])
payload = json.loads(summary_path.read_text(encoding="utf-8"))
completed = int(payload.get("completed_cases") or 0)
failed = int(payload.get("failed_cases") or 0)
raise SystemExit(0 if completed + failed >= expected and expected > 0 else 1)
PY
}

slot_existing_pid() {
  local market="$1"
  local suite run_label pid
  suite="$(slot_suite "$market")"
  run_label="$(slot_run_label "$market")"

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

slot_is_active() {
  local market="$1"
  local pidfile pid
  pidfile="$(slot_pid_path "$market")"
  if [[ -f "$pidfile" ]]; then
    pid="$(cat "$pidfile")"
    if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
      return 0
    fi
  fi
  if pid="$(slot_existing_pid "$market")"; then
    printf '%s\n' "$pid" > "$pidfile"
    return 0
  fi
  rm -f "$pidfile"
  return 1
}

launch_slot() {
  local market="$1"
  local suite run_label bootstrap_log stdout_log pid
  suite="$(slot_suite "$market")"
  run_label="$(slot_run_label "$market")"
  bootstrap_log="$(slot_log_path "$market")"
  stdout_log="$(slot_stdout_path "$market")"
  mkdir -p "$(dirname "$bootstrap_log")"
  : > "$stdout_log"
  nohup "$ROOT_DIR/scripts/research/run_one_experiment.sh" \
    --suite "$suite" \
    --run-label "$run_label" \
    --market "$market" \
    --log-path "$bootstrap_log" \
    >"$stdout_log" 2>&1 &
  pid="$!"
  printf '%s\n' "$pid" > "$(slot_pid_path "$market")"
  printf '[bootstrap_keepalive] launched market=%s suite=%s run=%s pid=%s\n' \
    "$market" "$suite" "$run_label" "$pid" >> "$LOG_PATH"
}

write_status() {
  python3 - <<'PY' "$STATUS_PATH" "$PID_PATH" "$LOG_PATH" "$STATE_DIR" "${markets[@]}"
from __future__ import annotations
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

status_path = Path(sys.argv[1])
supervisor_pid_path = Path(sys.argv[2])
log_path = Path(sys.argv[3])
state_dir = Path(sys.argv[4])
markets = sys.argv[5:]

payload: dict[str, object] = {
    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "supervisor_pid": None,
    "supervisor_alive": False,
    "log_path": str(log_path),
    "slots": [],
}

if supervisor_pid_path.exists():
    try:
      payload["supervisor_pid"] = int(supervisor_pid_path.read_text(encoding="utf-8").strip())
    except Exception:
      payload["supervisor_pid"] = None

supervisor_pid = payload.get("supervisor_pid")
if isinstance(supervisor_pid, int):
    alive = subprocess.run(["bash", "-lc", f"kill -0 {supervisor_pid}"], capture_output=True)
    payload["supervisor_alive"] = alive.returncode == 0

for market in markets:
    pid_path = state_dir / f"{market}.pid"
    stdout_path = state_dir / f"{market}.stdout.log"
    slot_pid = None
    slot_alive = False
    if pid_path.exists():
        try:
            slot_pid = int(pid_path.read_text(encoding="utf-8").strip())
        except Exception:
            slot_pid = None
    if isinstance(slot_pid, int):
        alive = subprocess.run(["bash", "-lc", f"kill -0 {slot_pid}"], capture_output=True)
        slot_alive = alive.returncode == 0
    payload["slots"].append(
        {
            "market": market,
            "pid": slot_pid,
            "alive": slot_alive,
            "stdout_path": str(stdout_path),
        }
    )

status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
PY
}

print_status() {
  write_status
  python3 - <<'PY' "$STATUS_PATH"
from __future__ import annotations
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
print(f"updated_at: {payload.get('updated_at')}")
print(f"supervisor_pid: {payload.get('supervisor_pid')}")
print(f"supervisor_alive: {payload.get('supervisor_alive')}")
print(f"log_path: {payload.get('log_path')}")
for slot in payload.get("slots") or []:
    print(
        f"{slot.get('market')}: pid={slot.get('pid')} alive={slot.get('alive')} "
        f"stdout_path={slot.get('stdout_path')}"
    )
PY
}

sync_slots() {
  local market
  for market in "${markets[@]}"; do
    if slot_is_complete "$market"; then
      rm -f "$(slot_pid_path "$market")"
      continue
    fi
    if ! slot_is_active "$market"; then
      launch_slot "$market"
    fi
  done
}

loop_body() {
  echo "$$" > "$PID_PATH"
  write_status
  while true; do
    if [[ -f "$STOP_FLAG" ]]; then
      rm -f "$STOP_FLAG"
      rm -f "$PID_PATH"
      write_status
      exit 0
    fi
    sync_slots
    write_status
    sleep "$LOOP_SLEEP_SEC"
  done
}

ACTION="${1:-start}"

case "$ACTION" in
  start)
    if [[ -f "$PID_PATH" ]]; then
      existing_pid="$(cat "$PID_PATH")"
      if [[ -n "$existing_pid" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
        echo "Bootstrap keepalive already running with pid=$existing_pid"
        exit 0
      fi
      rm -f "$PID_PATH"
    fi
    rm -f "$STOP_FLAG"
    nohup "$SCRIPT_PATH" __run_loop >> "$LOG_PATH" 2>&1 &
    echo "Started bootstrap keepalive"
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
  status)
    print_status
    ;;
  once)
    sync_slots
    write_status
    print_status
    ;;
  __run_loop)
    loop_body
    ;;
  *)
    echo "usage: bootstrap_keepalive.sh {start|stop|restart|status|once}" >&2
    exit 2
    ;;
esac
