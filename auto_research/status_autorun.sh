#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
AUTORUN_DIR="${AUTORUN_DIR:-$ROOT_DIR/var/research/autorun}"
STATUS_PATH="${STATUS_PATH:-$AUTORUN_DIR/codex-background.status.json}"

PYTHONPATH="$ROOT_DIR/src" python3 - <<'PY' "$ROOT_DIR" "$STATUS_PATH"
from __future__ import annotations
import json
import sys
from pathlib import Path

from pm15min.research.automation import build_autorun_status_report

root = Path(sys.argv[1]).resolve()
status_path = Path(sys.argv[2]).resolve()
payload = build_autorun_status_report(root, log_tail_lines=20, max_incomplete_runs=10, status_path=status_path)
status = payload.get("status") or {}
if not status:
    print("No background autorun status file found.")
else:
    for key in [
        "state",
        "pid",
        "session_dir",
        "iteration",
        "failure_count",
        "started_at",
        "updated_at",
        "last_started_at",
        "last_finished_at",
        "last_exit_code",
        "last_prompt_path",
        "last_output_path",
    ]:
        print(f"{key}: {status.get(key)}")

incomplete_runs = payload.get("incomplete_runs") or []
if incomplete_runs:
    print()
    print("== Incomplete Experiment Runs ==")
    for item in incomplete_runs:
        print(
            f"{item.get('state')}: {item.get('suite_name')} / {item.get('run_label')} "
            f"(last_event={item.get('last_event')}, completed={item.get('completed_cases')}, failed={item.get('failed_cases')})"
        )

queue_payload = payload.get("queue") or {}
queue_items = queue_payload.get("items") or []
if queue_items:
    print()
    print("== Experiment Queue ==")
    print(f"queue_path: {queue_payload.get('queue_path')}")
    print(f"max_live_runs: {queue_payload.get('max_live_runs')}")
    for item in queue_items:
        print(
            f"{item.get('status')}: {item.get('market')} / {item.get('action')} / "
            f"{item.get('suite_name')} / {item.get('run_label')} "
            f"(retry={item.get('retry_count')}, reason={item.get('reason')})"
        )

log_tail = payload.get("log_tail") or []
if log_tail:
    print()
    print("== Last Log Lines ==")
    for line in log_tail:
        print(line)
PY
