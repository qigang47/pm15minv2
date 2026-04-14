#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


def _root_from_cwd() -> Path:
    return Path(__file__).resolve().parents[2]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage autoresearch formal experiment queue.")
    parser.add_argument("--root", default=str(_root_from_cwd()), help="Repository root.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    enqueue = subparsers.add_parser("enqueue", help="Queue one formal experiment action.")
    enqueue.add_argument("--suite", required=True)
    enqueue.add_argument("--run-label", required=True)
    enqueue.add_argument("--market", required=True)
    enqueue.add_argument("--action", choices=("launch", "resume", "repair"), required=True)
    enqueue.add_argument("--priority", type=int, default=100)
    enqueue.add_argument("--reason", default="")

    set_status = subparsers.add_parser("set-status", help="Update one queue item status.")
    set_status.add_argument("--item-id")
    set_status.add_argument("--suite")
    set_status.add_argument("--run-label")
    set_status.add_argument("--status", choices=("queued", "running", "repair", "done", "dead"), required=True)
    set_status.add_argument("--reason")

    subparsers.add_parser("show", help="Print queue JSON.")

    supervise = subparsers.add_parser("supervise-once", help="Reconcile queue state and fill empty slots once.")
    supervise.add_argument("--max-live-runs", type=int, default=4)
    supervise.add_argument("--max-repair-attempts", type=int, default=3)

    return parser


def _default_artifact_paths(root: Path, run_label: str) -> dict[str, str]:
    from pm15min.research.automation import resolve_autorun_session_dir

    session_dir = resolve_autorun_session_dir(root, program_path=root / "auto_research" / "program.md")
    bootstrap_dir = session_dir / "bootstrap"
    queue_dir = root / "var" / "research" / "autorun" / "queue"
    bootstrap_dir.mkdir(parents=True, exist_ok=True)
    queue_dir.mkdir(parents=True, exist_ok=True)
    return {
        "log_path": str((bootstrap_dir / f"{run_label}.log").resolve()),
        "stdout_path": str((queue_dir / f"{run_label}.stdout.log").resolve()),
        "pid_path": str((queue_dir / f"{run_label}.pid").resolve()),
    }


def _queue_launcher(root: Path):
    script = (root / "auto_research" / "run_one_experiment_background.sh").resolve()

    def launcher(item: dict[str, object]) -> dict[str, object]:
        artifact_paths = _default_artifact_paths(root, str(item["run_label"]))
        cmd = [
            str(script),
            "--suite",
            str(item["suite_name"]),
            "--run-label",
            str(item["run_label"]),
            "--market",
            str(item["market"]),
            "--log-path",
            artifact_paths["log_path"],
            "--stdout-path",
            artifact_paths["stdout_path"],
            "--pid-path",
            artifact_paths["pid_path"],
        ]
        result = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip() or "queue launch failed")
        pid_value = None
        pid_path = Path(artifact_paths["pid_path"])
        if pid_path.exists():
            try:
                pid_value = int(pid_path.read_text(encoding="utf-8").strip())
            except Exception:
                pid_value = None
        payload = dict(artifact_paths)
        if pid_value is not None:
            payload["pid"] = pid_value
        return payload

    return launcher


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    root = Path(args.root).resolve()

    from pm15min.research.automation import build_autorun_status_report
    from pm15min.research.automation.control_plane import find_live_formal_workers
    from pm15min.research.automation.queue_state import (
        build_queue_item,
        ensure_running_queue_items,
        launch_ready_queue_items,
        load_experiment_queue,
        reconcile_queue_with_live_workers,
        set_queue_item_status,
        upsert_queue_item,
    )

    if args.command == "enqueue":
        item = build_queue_item(
            market=args.market,
            suite_name=args.suite,
            run_label=args.run_label,
            action=args.action,
            priority=args.priority,
            reason=args.reason,
        )
        item.update(_default_artifact_paths(root, args.run_label))
        payload = upsert_queue_item(root, item)
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "set-status":
        if not args.item_id and not (args.suite and args.run_label):
            parser.error("set-status requires --item-id or both --suite and --run-label")
        payload = set_queue_item_status(
            root,
            item_id=args.item_id,
            suite_name=args.suite,
            run_label=args.run_label,
            status=args.status,
            reason=args.reason,
        )
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "show":
        print(json.dumps(load_experiment_queue(root), indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "supervise-once":
        live_workers = find_live_formal_workers(root)
        ensure_running_queue_items(root, live_workers=live_workers)
        reconciled = reconcile_queue_with_live_workers(
            root,
            live_workers=live_workers,
            max_repair_attempts=args.max_repair_attempts,
        )
        live_workers = find_live_formal_workers(root)
        queue_payload, launched_items = launch_ready_queue_items(
            root,
            live_workers=live_workers,
            launcher=_queue_launcher(root),
            max_live_runs=args.max_live_runs,
        )
        payload = {
            "queue_path": str((root / "var" / "research" / "autorun" / "experiment-queue.json").resolve()),
            "live_workers": len(live_workers),
            "launched": [
                {
                    "market": item.get("market"),
                    "suite_name": item.get("suite_name"),
                    "run_label": item.get("run_label"),
                    "action": item.get("action"),
                }
                for item in launched_items
            ],
            "queue_items": len(queue_payload.get("items") or []),
            "status_report": build_autorun_status_report(root, log_tail_lines=0, max_incomplete_runs=5).get("status") or {},
            "reconciled_queue_items": len(reconciled.get("items") or []),
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
