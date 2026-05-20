#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
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
    enqueue.add_argument("--primary-lever", default="")
    enqueue.add_argument("--feature-width", default="")
    enqueue.add_argument("--model-family", default="")
    enqueue.add_argument("--feature-set", default="")
    enqueue.add_argument("--factor-family-change", default="")
    enqueue.add_argument("--expected-trade-count-effect", default="")
    enqueue.add_argument("--difference-from-recent-failures", default="")

    set_status = subparsers.add_parser("set-status", help="Update one queue item status.")
    set_status.add_argument("--item-id")
    set_status.add_argument("--suite")
    set_status.add_argument("--run-label")
    set_status.add_argument("--track")
    set_status.add_argument("--status", choices=("queued", "running", "repair", "done", "dead"), required=True)
    set_status.add_argument("--reason")
    set_status.add_argument("--last-error")

    subparsers.add_parser("show", help="Print queue JSON.")

    supervise = subparsers.add_parser("supervise-once", help="Reconcile queue state and fill empty slots once.")
    supervise.add_argument("--max-live-runs", type=int, default=10)
    supervise.add_argument("--max-queued-items", type=int, default=24)
    supervise.add_argument("--max-launches-per-pass", type=int, default=None)
    supervise.add_argument("--max-repair-attempts", type=int, default=3)
    supervise.add_argument("--min-available-mem-gb", type=float, default=1.0)
    supervise.add_argument("--meminfo-path", default="/proc/meminfo")
    supervise.add_argument("--quick-screen-batch-size", type=int, default=1)
    supervise.add_argument("--quick-screen-worker-mem-gb", type=float, default=16.0)
    supervise.add_argument(
        "--track-slot-caps",
        default='{"direction_dense": 5, "reversal_dense": 5}',
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


def _queue_wake_flag_for_item(root: Path, item: dict[str, object]) -> Path | None:
    track_to_autorun = {
        "direction_dense": "direction_dense_sol_xrp",
        "reversal_dense": "reversal_dense_sol_xrp",
    }
    track = str(item.get("track") or "").strip().lower()
    autorun_name = track_to_autorun.get(track)
    if autorun_name is None:
        return None
    return root / "var" / "research" / "autorun" / autorun_name / "wake.flag"


def _queue_launcher(root: Path):
    script = (root / "auto_research" / "run_one_experiment_background.sh").resolve()
    launch_timeout_sec = max(5, int(os.environ.get("PM15MIN_QUEUE_LAUNCH_TIMEOUT_SEC") or 30))

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
        market = str(item.get("market") or "").strip().lower()
        if track in {"direction_dense", "reversal_dense"} and market not in {"sol", "xrp"}:
            raise RuntimeError(f"dense queue can only launch sol/xrp quick-screen items, got market={market!r}")
        launch_mode = str(env.get("PM15MIN_EXPERIMENT_LAUNCH_MODE") or "").strip()
        quick_screen_top_k = str(env.get("PM15MIN_QUICK_SCREEN_TOP_K") or "").strip()
        quick_screen_train_parallel_workers = str(env.get("PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS") or "").strip()
        expected_concurrency = str(env.get("PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY") or "").strip()
        if track in {"direction_dense", "reversal_dense"}:
            launch_mode = launch_mode or "quick_screen"
            quick_screen_top_k = quick_screen_top_k or "1"
            quick_screen_train_parallel_workers = quick_screen_train_parallel_workers or "2"
            expected_concurrency = expected_concurrency or str(
                max(1, int(os.environ.get("MAX_LIVE_RUNS") or 10))
            )
        if session_dir:
            env["SESSION_DIR"] = session_dir
        if program_path:
            env["PROGRAM_PATH"] = program_path
        if track:
            env["EXPERIMENT_TRACK"] = track
        wake_flag = _queue_wake_flag_for_item(root, item)
        if wake_flag is not None:
            env["PM15MIN_AUTORESEARCH_WAKE_FLAG"] = str(wake_flag)
        if launch_mode:
            cmd.extend(["--launch-mode", launch_mode])
        if quick_screen_top_k:
            cmd.extend(["--quick-screen-top-k", quick_screen_top_k])
        if quick_screen_train_parallel_workers:
            cmd.extend(["--quick-screen-train-parallel-workers", quick_screen_train_parallel_workers])
        if expected_concurrency:
            cmd.extend(["--expected-concurrency", expected_concurrency])
        try:
            result = subprocess.run(
                cmd,
                cwd=root,
                capture_output=True,
                text=True,
                check=False,
                env=env,
                timeout=launch_timeout_sec,
            )
        except subprocess.TimeoutExpired as exc:
            pid_path = Path(artifact_paths["pid_path"])
            if pid_path.exists():
                try:
                    pid_value = int(pid_path.read_text(encoding="utf-8").strip())
                    os.kill(pid_value, 0)
                except Exception:
                    pass
                else:
                    payload = dict(artifact_paths)
                    payload["pid"] = pid_value
                    return payload
            raise RuntimeError(f"queue launch timed out after {launch_timeout_sec}s") from exc
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


def _queue_batch_launcher(root: Path):
    script = (root / "auto_research" / "run_quick_screen_queue_batch.sh").resolve()
    python_script = (root / "scripts" / "research" / "run_quick_screen_queue_batch.py").resolve()
    queue_dir = root / "var" / "research" / "autorun" / "queue"

    def launcher(items: list[dict[str, object]]) -> dict[str, object]:
        batch_items = [dict(item) for item in items if isinstance(item, dict)]
        if not batch_items:
            raise RuntimeError("quick-screen batch requires at least one item")
        for item in batch_items:
            if str(item.get("track") or "").strip().lower() in {"", "unknown"}:
                raise RuntimeError("queue item track metadata is required for batch launch")
            if not str(item.get("program_path") or "").strip():
                raise RuntimeError("queue item program_path metadata is required for batch launch")
            if not str(item.get("session_dir") or "").strip():
                raise RuntimeError("queue item session_dir metadata is required for batch launch")
        batch_id = _quick_screen_batch_id(batch_items)
        stdout_path = queue_dir / f"{batch_id}.stdout.log"
        pid_path = queue_dir / f"{batch_id}.pid"
        manifest_path = queue_dir / f"{batch_id}.manifest.json"
        queue_dir.mkdir(parents=True, exist_ok=True)

        manifest_items = []
        for item in batch_items:
            artifact_paths = _default_artifact_paths(root, item)
            manifest_items.append(
                {
                    **item,
                    "log_path": artifact_paths["log_path"],
                    "stdout_path": artifact_paths["stdout_path"],
                    "pid_path": artifact_paths["pid_path"],
                }
            )
        manifest_payload = {
            "batch_id": batch_id,
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "root": str(root),
            "items": manifest_items,
        }
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

        env = dict(os.environ)
        first_item = batch_items[0]
        session_dir = str(first_item.get("session_dir") or "").strip()
        program_path = str(first_item.get("program_path") or "").strip()
        track = str(first_item.get("track") or "").strip()
        if session_dir:
            env["SESSION_DIR"] = session_dir
        if program_path:
            env["PROGRAM_PATH"] = program_path
        if track:
            env["EXPERIMENT_TRACK"] = track
        env["PM15MIN_PROJECT_DIR"] = str(root)
        env["PM15MIN_EXPERIMENT_LAUNCH_MODE"] = "quick_screen"
        env["PM15MIN_QUICK_SCREEN_TOP_K"] = str(env.get("PM15MIN_QUICK_SCREEN_TOP_K") or "1")
        env["PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS"] = str(
            env.get("PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS") or "1"
        )
        env["PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY"] = str(
            env.get("PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY")
            or max(1, int(os.environ.get("MAX_LIVE_RUNS") or 10))
        )
        wake_flag = _queue_wake_flag_for_item(root, first_item)
        if wake_flag is not None:
            env["PM15MIN_AUTORESEARCH_WAKE_FLAG"] = str(wake_flag)

        cmd = [
            str(script),
            "--root",
            str(root),
            "--manifest",
            str(manifest_path),
            "--batch-id",
            batch_id,
            "--top-k",
            env["PM15MIN_QUICK_SCREEN_TOP_K"],
        ]
        env["PM15MIN_QUICK_SCREEN_BATCH_SCRIPT"] = str(python_script)
        with stdout_path.open("ab") as stdout_file:
            process = subprocess.Popen(
                cmd,
                cwd=root,
                stdout=stdout_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
        try:
            process.wait(timeout=0)
        except subprocess.TimeoutExpired:
            pass
        else:
            if process.returncode not in (0, None):
                raise RuntimeError(f"quick-screen batch launch failed with exit={process.returncode}")
        return {
            "pid": process.pid,
            "batch_id": batch_id,
            "manifest_path": str(manifest_path),
            "stdout_path": str(stdout_path),
            "pid_path": str(pid_path),
        }

    return launcher


def _queue_pool_launcher(root: Path):
    script = (root / "auto_research" / "run_quick_screen_pool.sh").resolve()
    python_script = (root / "scripts" / "research" / "run_quick_screen_pool.py").resolve()
    queue_dir = root / "var" / "research" / "autorun" / "queue"

    def launcher(items: list[dict[str, object]]) -> dict[str, object]:
        batch_items = [dict(item) for item in items if isinstance(item, dict)]
        if not batch_items:
            raise RuntimeError("quick-screen pool requires at least one item")
        for item in batch_items:
            if str(item.get("track") or "").strip().lower() in {"", "unknown"}:
                raise RuntimeError("queue item track metadata is required for pool launch")
            if not str(item.get("program_path") or "").strip():
                raise RuntimeError("queue item program_path metadata is required for pool launch")
            if not str(item.get("session_dir") or "").strip():
                raise RuntimeError("queue item session_dir metadata is required for pool launch")
        batch_id = _quick_screen_pool_id(batch_items)
        stdout_path = queue_dir / f"{batch_id}.stdout.log"
        pid_path = queue_dir / f"{batch_id}.pid"
        manifest_path = queue_dir / f"{batch_id}.manifest.json"
        queue_dir.mkdir(parents=True, exist_ok=True)

        manifest_items = []
        for item in batch_items:
            artifact_paths = _default_artifact_paths(root, item)
            manifest_items.append(
                {
                    **item,
                    "log_path": artifact_paths["log_path"],
                    "stdout_path": artifact_paths["stdout_path"],
                    "pid_path": artifact_paths["pid_path"],
                }
            )
        manifest_payload = {
            "batch_id": batch_id,
            "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "launcher": "quick_screen_pool",
            "root": str(root),
            "items": manifest_items,
        }
        manifest_path.write_text(json.dumps(manifest_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

        env = dict(os.environ)
        first_item = batch_items[0]
        session_dir = str(first_item.get("session_dir") or "").strip()
        program_path = str(first_item.get("program_path") or "").strip()
        track = str(first_item.get("track") or "").strip()
        if session_dir:
            env["SESSION_DIR"] = session_dir
        if program_path:
            env["PROGRAM_PATH"] = program_path
        if track:
            env["EXPERIMENT_TRACK"] = track
        env["PM15MIN_PROJECT_DIR"] = str(root)
        env["PM15MIN_EXPERIMENT_LAUNCH_MODE"] = "quick_screen"
        env["PM15MIN_QUICK_SCREEN_SHARED_SURFACES"] = "1"
        env["PM15MIN_QUICK_SCREEN_TOP_K"] = str(env.get("PM15MIN_QUICK_SCREEN_TOP_K") or "1")
        env["PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS"] = str(
            env.get("PM15MIN_QUICK_SCREEN_TRAIN_PARALLEL_WORKERS") or "1"
        )
        env["PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY"] = str(
            env.get("PM15MIN_EXPECTED_EXPERIMENT_CONCURRENCY")
            or max(1, int(os.environ.get("MAX_LIVE_RUNS") or 10))
        )
        env["PM15MIN_QUICK_SCREEN_POOL_WORKERS"] = str(
            env.get("PM15MIN_QUICK_SCREEN_POOL_WORKERS") or max(1, len(batch_items))
        )
        env["PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS"] = str(
            env.get("PM15MIN_QUICK_SCREEN_POOL_MAX_ITEMS") or max(1, len(batch_items))
        )
        wake_flag = _queue_wake_flag_for_item(root, first_item)
        if wake_flag is not None:
            env["PM15MIN_AUTORESEARCH_WAKE_FLAG"] = str(wake_flag)

        cmd = [
            str(script),
            "--root",
            str(root),
            "--manifest",
            str(manifest_path),
            "--batch-id",
            batch_id,
            "--top-k",
            env["PM15MIN_QUICK_SCREEN_TOP_K"],
        ]
        env["PM15MIN_QUICK_SCREEN_POOL_SCRIPT"] = str(python_script)
        with stdout_path.open("ab") as stdout_file:
            process = subprocess.Popen(
                cmd,
                cwd=root,
                stdout=stdout_file,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        pid_path.write_text(f"{process.pid}\n", encoding="utf-8")
        try:
            process.wait(timeout=0)
        except subprocess.TimeoutExpired:
            pass
        else:
            if process.returncode not in (0, None):
                raise RuntimeError(f"quick-screen pool launch failed with exit={process.returncode}")
        return {
            "pid": process.pid,
            "batch_id": batch_id,
            "manifest_path": str(manifest_path),
            "stdout_path": str(stdout_path),
            "pid_path": str(pid_path),
            "launcher": "quick_screen_pool",
        }

    return launcher


def _quick_screen_batch_id(items: list[dict[str, object]]) -> str:
    first = items[0]
    track = str(first.get("track") or "quick_screen").strip().lower().replace("/", "_")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"quick_screen_batch_{track}_{stamp}_{os.getpid()}"


def _quick_screen_pool_id(items: list[dict[str, object]]) -> str:
    first = items[0]
    track = str(first.get("track") or "quick_screen").strip().lower().replace("/", "_")
    market = str(first.get("market") or "").strip().lower().replace("/", "_")
    market_part = f"_{market}" if market else ""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"quick_screen_pool_{track}{market_part}_{stamp}_{os.getpid()}"


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


def _read_mem_available_kb(meminfo_path: str) -> int | None:
    path = Path(meminfo_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        if not line.startswith("MemAvailable:"):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None
    return None


def _memory_gate_payload(
    *,
    min_available_mem_gb: float,
    meminfo_path: str,
    live_workers: list[dict[str, object]] | None = None,
    launch_mem_gb: float = 0.0,
) -> dict[str, object]:
    required_kb = max(0, int(float(min_available_mem_gb) * 1024 * 1024))
    available_kb = _read_mem_available_kb(meminfo_path)
    launch_budget_kb = max(0, int(float(launch_mem_gb or 0.0) * 1024 * 1024))
    budget_workers = _quick_screen_budget_workers(live_workers or [])
    live_worker_reservation_gap_kb = _live_worker_reservation_gap_kb(
        budget_workers,
        launch_budget_kb=launch_budget_kb,
    )
    effective_required_kb = required_kb + live_worker_reservation_gap_kb
    state = "open"
    required_with_next_launch_kb = effective_required_kb + launch_budget_kb
    if required_with_next_launch_kb > 0 and (available_kb is None or available_kb < required_with_next_launch_kb):
        state = "blocked"
    launch_capacity = None
    if available_kb is not None and launch_budget_kb > 0:
        launch_capacity = max(0, int((available_kb - effective_required_kb) // launch_budget_kb))
    return {
        "state": state,
        "available_kb": available_kb,
        "required_kb": required_kb,
        "effective_required_kb": effective_required_kb,
        "required_with_next_launch_kb": required_with_next_launch_kb,
        "launch_budget_kb": launch_budget_kb,
        "live_worker_reservation_gap_kb": live_worker_reservation_gap_kb,
        "live_worker_count_for_budget": len(_unique_live_worker_budget_entries(budget_workers)),
        "launch_capacity": launch_capacity,
        "meminfo_path": meminfo_path,
    }


def _quick_screen_budget_workers(live_workers: list[dict[str, object]]) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for raw_worker in live_workers:
        if not isinstance(raw_worker, dict):
            continue
        worker = dict(raw_worker)
        track = str(worker.get("track") or "").strip().lower()
        cmd = str(worker.get("cmd") or "").lower()
        if track in {"direction_dense", "reversal_dense"} or "run_quick_screen" in cmd:
            out.append(worker)
    return out


def _unique_live_worker_budget_entries(live_workers: list[dict[str, object]]) -> list[dict[str, object]]:
    entries: dict[str, dict[str, object]] = {}
    fallback_index = 0
    for raw_worker in live_workers:
        if not isinstance(raw_worker, dict):
            continue
        worker = dict(raw_worker)
        pid = str(worker.get("pid") or "").strip()
        batch_id = str(worker.get("batch_id") or "").strip()
        suite_name = str(worker.get("suite_name") or "").strip()
        run_label = str(worker.get("run_label") or "").strip()
        market = str(worker.get("market") or "").strip()
        if pid:
            key = f"pid:{pid}"
        elif batch_id:
            key = f"batch:{batch_id}"
        elif suite_name or run_label or market:
            key = f"worker:{suite_name}:{run_label}:{market}"
        else:
            fallback_index += 1
            key = f"anon:{fallback_index}"
        entries.setdefault(key, worker)
    return list(entries.values())


def _live_worker_reservation_gap_kb(
    live_workers: list[dict[str, object]],
    *,
    launch_budget_kb: int,
) -> int:
    if launch_budget_kb <= 0:
        return 0
    total_gap = 0
    for worker in _unique_live_worker_budget_entries(live_workers):
        rss_kb = _worker_rss_kb(worker)
        if rss_kb < launch_budget_kb:
            total_gap += launch_budget_kb - rss_kb
    return total_gap


def _worker_rss_kb(worker: dict[str, object]) -> int:
    for key in ("rss_kb", "rss"):
        raw = worker.get(key)
        try:
            return max(0, int(raw))
        except Exception:
            continue
    return 0


def _quick_screen_launch_preflight_error(root: Path, item: dict[str, object]) -> str | None:
    from pm15min.research.experiments.specs import load_suite_definition
    from pm15min.research.features.registry import feature_set_columns

    suite_name = str(item.get("suite_name") or "").strip()
    if not suite_name:
        return "missing suite_name"
    suite_path = root / "research" / "experiments" / "suite_specs" / f"{suite_name}.json"
    try:
        suite = load_suite_definition(suite_path)
        for market_spec in suite.markets:
            feature_set_columns(market_spec.feature_set, root=root)
    except Exception as exc:
        return str(exc)
    return None


def _mark_unlaunchable_quick_screen_items_dead(
    root: Path,
    payload: dict[str, object],
) -> dict[str, object]:
    updated_items: list[dict[str, object]] = []
    changed = False
    for raw_item in payload.get("items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = dict(raw_item)
        status = str(item.get("status") or "").strip().lower()
        track = str(item.get("track") or "").strip().lower()
        if status in {"queued", "repair"} and track in {"direction_dense", "reversal_dense"}:
            error = _quick_screen_launch_preflight_error(root, item)
            if error is not None:
                item["action"] = "blocked"
                item["status"] = "dead"
                item["reason"] = "launch_preflight_failed"
                item["last_error"] = error
                item["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                changed = True
        updated_items.append(item)
    if not changed:
        return payload
    updated_payload = dict(payload)
    updated_payload["items"] = updated_items
    return _save_queue_payload_without_revalidating_preflight(root, updated_payload)


def _save_queue_payload_without_revalidating_preflight(
    root: Path,
    payload: dict[str, object],
) -> dict[str, object]:
    from pm15min.research.automation.queue_state import save_experiment_queue

    return save_experiment_queue(root, payload)


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
        summarize_queue_items,
        upsert_queue_item,
    )
    from pm15min.research.automation.queue_state import launch_ready_queue_item_batches

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
        research_meta = {
            "primary_lever": args.primary_lever,
            "feature_width": args.feature_width,
            "model_family": args.model_family,
            "feature_set": args.feature_set,
            "factor_family_change": args.factor_family_change,
            "expected_trade_count_effect": args.expected_trade_count_effect,
            "difference_from_recent_failures": args.difference_from_recent_failures,
        }
        item["research_meta"] = {
            key: value
            for key, value in research_meta.items()
            if str(value or "").strip()
        }
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
                last_error=args.last_error,
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
        memory_gate = _memory_gate_payload(
            min_available_mem_gb=args.min_available_mem_gb,
            meminfo_path=args.meminfo_path,
            live_workers=live_workers,
            launch_mem_gb=args.quick_screen_worker_mem_gb
            if int(args.quick_screen_batch_size or 1) > 1
            else 0.0,
        )
        if memory_gate["state"] == "blocked":
            launched_items = []
        else:
            quick_screen_batch_size = max(1, int(args.quick_screen_batch_size or 1))
            max_launches_per_pass = args.max_launches_per_pass
            launch_capacity = memory_gate.get("launch_capacity")
            if launch_capacity is not None:
                max_launches_per_pass = (
                    int(launch_capacity)
                    if max_launches_per_pass is None
                    else min(int(max_launches_per_pass), int(launch_capacity))
                )
            queue_payload = _mark_unlaunchable_quick_screen_items_dead(root, queue_payload)
            quick_screen_use_pool = str(os.environ.get("PM15MIN_QUICK_SCREEN_USE_POOL") or "").strip() == "1"
            if quick_screen_use_pool:
                queue_payload, launched_items = launch_ready_queue_item_batches(
                    root,
                    live_workers=live_workers,
                    batch_launcher=_queue_pool_launcher(root),
                    max_live_runs=args.max_live_runs,
                    max_new_launches=max_launches_per_pass,
                    quick_screen_batch_size=max(1, quick_screen_batch_size),
                    single_pool_per_track=True,
                )
            elif quick_screen_batch_size > 1:
                queue_payload, launched_items = launch_ready_queue_item_batches(
                    root,
                    live_workers=live_workers,
                    batch_launcher=_queue_batch_launcher(root),
                    max_live_runs=args.max_live_runs,
                    max_new_launches=max_launches_per_pass,
                    quick_screen_batch_size=quick_screen_batch_size,
                )
            else:
                queue_payload, launched_items = launch_ready_queue_items(
                    root,
                    live_workers=live_workers,
                    launcher=_queue_launcher(root),
                    max_live_runs=args.max_live_runs,
                    max_new_launches=max_launches_per_pass,
                )
        queue_summary = summarize_queue_items(queue_payload.get("items") or [])
        reconciled_summary = summarize_queue_items(reconciled.get("items") or [])
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
            "memory_gate": memory_gate,
            "quick_screen_batch_size": int(args.quick_screen_batch_size or 1),
            "quick_screen_use_pool": str(os.environ.get("PM15MIN_QUICK_SCREEN_USE_POOL") or "").strip() == "1",
            "quick_screen_worker_mem_gb": float(args.quick_screen_worker_mem_gb),
            "queue_items": queue_summary["pending_items"],
            "pending_queue_items": queue_summary["pending_items"],
            "running_queue_items": queue_summary["running_items"],
            "done_queue_items": queue_summary["done_items"],
            "dead_queue_items": queue_summary["dead_items"],
            "total_queue_items": queue_summary["total_items"],
            "queue_status_counts": queue_summary["status_counts"],
            "status_report": build_autorun_status_report(root, log_tail_lines=0, max_incomplete_runs=5).get("status") or {},
            "reconciled_queue_items": reconciled_summary["pending_items"],
            "reconciled_total_queue_items": reconciled_summary["total_items"],
            "reconciled_queue_status_counts": reconciled_summary["status_counts"],
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
