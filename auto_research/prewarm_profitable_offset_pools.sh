#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
source "$ROOT_DIR/scripts/entrypoints/_python_env.sh"
STATE_DIR="$ROOT_DIR/var/research/autorun"
PREWARM_STATUS_PATH="$STATE_DIR/profitable-offset-pool-prewarm.status.json"
PREWARM_LOG_PATH="$STATE_DIR/profitable-offset-pool-prewarm.log"
PREWARM_PID_PATH="$STATE_DIR/profitable-offset-pool-prewarm.pid"
PREWARM_STOP_FLAG="$STATE_DIR/profitable-offset-pool-prewarm.stop.flag"
MARKETS="${MARKETS:-btc,eth,sol,xrp}"
CYCLE="${CYCLE:-15m}"
PROFILE="${PROFILE:-deep_otm_baseline}"
FEATURE_SET="${FEATURE_SET:-bs_q_replace_direction}"
LABEL_SET="${LABEL_SET:-truth}"
TARGET="${TARGET:-direction}"
MODEL_FAMILY="${MODEL_FAMILY:-deep_otm}"
DECISION_START="${DECISION_START:-2026-04-15}"
DECISION_END="${DECISION_END:-2026-05-07}"
STAKE_LABEL="${STAKE_LABEL:-2usd}"
OFFSETS="${OFFSETS:-7,8,9}"
export PM15MIN_PROJECT_DIR="$ROOT_DIR"
export PM15MIN_MANAGED_PROXY_ENABLE="${PM15MIN_MANAGED_PROXY_ENABLE:-1}"
export MPLCONFIGDIR="${MPLCONFIGDIR:-/tmp/pm15min-mpl}"

mkdir -p "$STATE_DIR"

write_status() {
  python3 - <<'PY' "$PREWARM_STATUS_PATH" "$PREWARM_PID_PATH" "$1" "$MARKETS" "$CYCLE" "$PROFILE" "$FEATURE_SET" "$LABEL_SET" "$TARGET" "$MODEL_FAMILY" "$DECISION_START" "$DECISION_END" "$STAKE_LABEL" "$OFFSETS" "${2:-}"
from __future__ import annotations
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

status_path = Path(sys.argv[1])
pid_path = Path(sys.argv[2])
state = str(sys.argv[3])
markets = str(sys.argv[4])
cycle = str(sys.argv[5])
profile = str(sys.argv[6])
feature_set = str(sys.argv[7])
label_set = str(sys.argv[8])
target = str(sys.argv[9])
model_family = str(sys.argv[10])
decision_start = str(sys.argv[11])
decision_end = str(sys.argv[12])
stake_label = str(sys.argv[13])
offsets = str(sys.argv[14])
payload_text = str(sys.argv[15] or "").strip()

payload = {
    "state": state,
    "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "pid": int(pid_path.read_text(encoding="utf-8").strip()) if pid_path.exists() else None,
    "markets": [item for item in markets.split(",") if item],
    "cycle": cycle,
    "profile": profile,
    "feature_set": feature_set,
    "label_set": label_set,
    "target": target,
    "model_family": model_family,
    "decision_start": decision_start,
    "decision_end": decision_end,
    "stake_label": stake_label,
    "offsets": [item for item in offsets.split(",") if item],
}
if payload_text:
    try:
        payload["result"] = json.loads(payload_text)
    except json.JSONDecodeError:
        payload["result_text"] = payload_text
status_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
PY
}

run_once() {
  pm15min_load_project_env
  pm15min_load_managed_proxy_env
  pm15min_activate_python
  echo "$$" > "$PREWARM_PID_PATH"
  write_status "running"
  local payload
  payload="$(
    PYTHONPATH="$ROOT_DIR/src" "$PYTHON_BIN" "$ROOT_DIR/scripts/research/prewarm_profitable_offset_pools.py" \
      --root "$ROOT_DIR" \
      --markets "$MARKETS" \
      --cycle "$CYCLE" \
      --profile "$PROFILE" \
      --feature-set "$FEATURE_SET" \
      --label-set "$LABEL_SET" \
      --target "$TARGET" \
      --model-family "$MODEL_FAMILY" \
      --decision-start "$DECISION_START" \
      --decision-end "$DECISION_END" \
      --stake-label "$STAKE_LABEL" \
      --offsets "$OFFSETS"
  )"
  printf '%s\n' "$payload" >> "$PREWARM_LOG_PATH"
  rm -f "$PREWARM_PID_PATH" "$PREWARM_STOP_FLAG"
  write_status "completed" "$payload"
}

status_state() {
  python3 - <<'PY' "$PREWARM_STATUS_PATH" "$MARKETS" "$CYCLE" "$PROFILE" "$FEATURE_SET" "$LABEL_SET" "$TARGET" "$MODEL_FAMILY" "$DECISION_START" "$DECISION_END" "$STAKE_LABEL" "$OFFSETS"
from __future__ import annotations
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    print("")
    raise SystemExit(0)
payload = json.loads(path.read_text(encoding="utf-8"))
expected = {
    "markets": [item for item in str(sys.argv[2]).split(",") if item],
    "cycle": str(sys.argv[3]),
    "profile": str(sys.argv[4]),
    "feature_set": str(sys.argv[5]),
    "label_set": str(sys.argv[6]),
    "target": str(sys.argv[7]),
    "model_family": str(sys.argv[8]),
    "decision_start": str(sys.argv[9]),
    "decision_end": str(sys.argv[10]),
    "stake_label": str(sys.argv[11]),
    "offsets": [item for item in str(sys.argv[12]).split(",") if item],
}
for key, expected_value in expected.items():
    if payload.get(key) != expected_value:
        print("stale")
        raise SystemExit(0)
print(str(payload.get("state") or ""))
PY
}

ACTION="${1:-ensure}"

case "$ACTION" in
  ensure)
    current_state="$(status_state)"
    if [[ "$current_state" == "completed" ]]; then
      echo "Prewarm already completed"
      exit 0
    fi
    run_once
    ;;
  status)
    cat "$PREWARM_STATUS_PATH"
    ;;
  clear)
    rm -f "$PREWARM_STATUS_PATH" "$PREWARM_PID_PATH" "$PREWARM_STOP_FLAG"
    echo "Prewarm state cleared"
    ;;
  stop)
    touch "$PREWARM_STOP_FLAG"
    if [[ -f "$PREWARM_PID_PATH" ]]; then
      pid="$(cat "$PREWARM_PID_PATH")"
      kill "$pid" >/dev/null 2>&1 || true
    fi
    echo "Stop requested"
    ;;
  *)
    echo "usage: prewarm_profitable_offset_pools.sh {ensure|status|clear|stop}" >&2
    exit 2
    ;;
esac
