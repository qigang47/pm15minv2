#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def _root_from_cwd() -> Path:
    return Path(__file__).resolve().parents[1]


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
    enqueue.add_argument("--track", required=True)
    enqueue.add_argument("--session-dir", required=True)
    enqueue.add_argument("--program-path", required=True)

    set_status = subparsers.add_parser("set-status", help="Update one queue item status.")
    set_status.add_argument("--item-id")
    set_status.add_argument("--suite")
    set_status.add_argument("--run-label")
    set_status.add_argument("--track")
    set_status.add_argument("--status", choices=("queued", "running", "repair", "done", "dead"), required=True)
    set_status.add_argument("--reason")

    subparsers.add_parser("show", help="Print queue JSON.")

    supervise = subparsers.add_parser("supervise-once", help="Reconcile queue state and fill empty slots once.")
    supervise.add_argument("--max-live-runs", type=int, default=16)
    supervise.add_argument("--max-queued-items", type=int, default=24)
    supervise.add_argument("--max-repair-attempts", type=int, default=3)
    supervise.add_argument(
        "--track-slot-caps",
        default='{"direction_dense": 2, "reversal_dense": 2}',
        help="JSON object mapping track names to shared live slot caps.",
    )

    return parser


def _default_artifact_paths(root: Path, item: dict[str, object]) -> dict[str, str]:
    session_dir = _resolve_session_dir(root, item, required=True)
    run_label = str(item.get("run_label") or "").strip()
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
        if str(item.get("track") or "").strip().lower() in {"", "unknown"}:
            raise RuntimeError("queue item track metadata is required for launch")
        if not str(item.get("program_path") or "").strip():
            raise RuntimeError("queue item program_path metadata is required for launch")
        if not str(item.get("session_dir") or "").strip():
            raise RuntimeError("queue item session_dir metadata is required for launch")
        artifact_paths = _default_artifact_paths(root, item)
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
        env = dict(os.environ)
        session_dir = str(item.get("session_dir") or "").strip()
        program_path = str(item.get("program_path") or "").strip()
        track = str(item.get("track") or "").strip()
        if session_dir:
            env["SESSION_DIR"] = session_dir
        if program_path:
            env["PROGRAM_PATH"] = program_path
        if track:
            env["EXPERIMENT_TRACK"] = track
        result = subprocess.run(cmd, cwd=root, capture_output=True, text=True, check=False, env=env)
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


def _resolve_program_path(root: Path, value: str | None, *, required: bool = False) -> Path:
    if value is None or not str(value).strip():
        if required:
            raise ValueError("program_path is required")
        return Path()
    raw = Path(str(value).strip()).expanduser()
    return (raw if raw.is_absolute() else root / raw).resolve()


def _resolve_session_dir(root: Path, item: dict[str, object], *, required: bool = False) -> Path:
    explicit = str(item.get("session_dir") or "").strip() or None
    if explicit is None:
        if required:
            raise ValueError("session_dir is required")
        return Path()
    raw = Path(explicit).expanduser()
    return (raw if raw.is_absolute() else root / raw).resolve()


def _parse_track_slot_caps(raw: str) -> dict[str, int]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid track slot caps JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("track slot caps must be a JSON object")
    caps: dict[str, int] = {}
    for raw_key, raw_value in payload.items():
        key = str(raw_key or "").strip()
        if not key:
            continue
        caps[key] = int(raw_value)
    return caps


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
        reseed_empty_tracks_from_recent_done,
        save_experiment_queue,
        set_queue_item_status,
        upsert_queue_item,
    )

    if args.command == "enqueue":
        try:
            program_path = _resolve_program_path(root, args.program_path, required=True)
            session_dir = _resolve_session_dir(
                root,
                {
                    "session_dir": args.session_dir,
                    "program_path": str(program_path),
                },
                required=True,
            )
        except ValueError as exc:
            parser.error(str(exc))
        item = build_queue_item(
            market=args.market,
            suite_name=args.suite,
            run_label=args.run_label,
            action=args.action,
            priority=args.priority,
            reason=args.reason,
            track=args.track,
            session_dir=session_dir,
            program_path=program_path,
        )
        item.update(_default_artifact_paths(root, item))
        payload = upsert_queue_item(root, item)
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "set-status":
        if not args.item_id and not (args.suite and args.run_label):
            parser.error("set-status requires --item-id or both --suite and --run-label")
        try:
            payload = set_queue_item_status(
                root,
                item_id=args.item_id,
                suite_name=args.suite,
                run_label=args.run_label,
                track=args.track,
                status=args.status,
                reason=args.reason,
            )
        except (KeyError, ValueError) as exc:
            parser.error(str(exc))
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "show":
        print(json.dumps(load_experiment_queue(root), indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    if args.command == "supervise-once":
        try:
            track_slot_caps = _parse_track_slot_caps(args.track_slot_caps)
        except ValueError as exc:
            parser.error(str(exc))
        queue_payload = load_experiment_queue(root)
        queue_payload["max_live_runs"] = args.max_live_runs
        queue_payload["max_queued_items"] = args.max_queued_items
        queue_payload["track_slot_caps"] = track_slot_caps
        save_experiment_queue(root, queue_payload)
        live_workers = find_live_formal_workers(root)
        ensure_running_queue_items(root, live_workers=live_workers)
        reconciled = reconcile_queue_with_live_workers(
            root,
            live_workers=live_workers,
            max_repair_attempts=args.max_repair_attempts,
        )
        live_workers = find_live_formal_workers(root)
        queue_payload, reseeded_items = reseed_empty_tracks_from_recent_done(
            root,
            live_workers=live_workers,
        )
        queue_payload, launched_items = launch_ready_queue_items(
            root,
            live_workers=live_workers,
            launcher=_queue_launcher(root),
            max_live_runs=args.max_live_runs,
        )
        payload = {
            "queue_path": str((root / "var" / "research" / "autorun" / "experiment-queue.json").resolve()),
            "live_workers": len(live_workers),
            "max_queued_items": args.max_queued_items,
            "track_slot_caps": track_slot_caps,
            "launched": [
                {
                    "market": item.get("market"),
                    "track": item.get("track"),
                    "suite_name": item.get("suite_name"),
                    "run_label": item.get("run_label"),
                    "action": item.get("action"),
                }
                for item in launched_items
            ],
            "reseeded": [
                {
                    "market": item.get("market"),
                    "track": item.get("track"),
                    "suite_name": item.get("suite_name"),
                    "run_label": item.get("run_label"),
                    "action": item.get("action"),
                }
                for item in reseeded_items
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
