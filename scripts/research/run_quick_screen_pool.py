#!/usr/bin/env python3
from __future__ import annotations

import argparse
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import traceback

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

SCRIPT_DIR = ROOT / "scripts" / "research"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from pm15min.core.process_memory import trim_process_memory
from pm15min.research.backtests.runtime_cache import clear_process_backtest_runtime_cache
from pm15min.research.inference.scorer import clear_process_scoring_runtime_cache
from pm15min.research.automation.queue_state import (
    experiment_queue_path,
    load_experiment_queue,
    save_experiment_queue,
)
from run_quick_screen_suite import run_quick_screen_suite

_CLEANUP_LOCK = threading.Lock()
_REFILL_POLL_SEC = 5.0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run quick-screen queue work through one shared in-process pool.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--top-k", type=int, default=1)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--max-items", type=int, default=0)
    parser.add_argument("--memory-report-interval-sec", type=int, default=60)
    args = parser.parse_args()

    return run_pool(
        root=Path(args.root).resolve(),
        manifest_path=Path(args.manifest).expanduser().resolve(),
        batch_id=str(args.batch_id),
        top_k=int(args.top_k),
        workers=int(args.workers),
        max_items=int(args.max_items),
        memory_report_interval_sec=int(args.memory_report_interval_sec),
    )


def run_pool(
    *,
    root: Path,
    manifest_path: Path,
    batch_id: str,
    top_k: int,
    workers: int,
    max_items: int,
    memory_report_interval_sec: int,
) -> int:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    if max_items > 0:
        items = items[:max_items]
    if not items:
        raise SystemExit("quick-screen pool manifest has no items")

    worker_count = max(1, int(workers or 1))
    print(
        "[quick_screen_pool] start "
        f"batch_id={batch_id} items={len(items)} workers={worker_count} manifest={manifest_path}",
        flush=True,
    )
    reporter = _MemoryReporter(
        batch_id=batch_id,
        interval_sec=max(0, int(memory_report_interval_sec or 0)),
    )
    reporter.maybe_report(force=True)
    exit_code = 0
    track = _pool_track(items)
    market = _pool_market(items)
    submitted_ids: set[str] = set()
    future_to_item = {}
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="quick-screen") as executor:
        for item in _append_refill_items(
            root,
            items,
                manifest_path=manifest_path,
                track=track,
                market=market,
                batch_id=batch_id,
                max_items=max_items,
        ):
            item_id = str(item.get("id") or "").strip()
            if item_id and item_id in submitted_ids:
                continue
            if item_id:
                submitted_ids.add(item_id)
            future_to_item[executor.submit(_run_pool_item, root, item, top_k)] = item

        while future_to_item:
            done_futures, _ = wait(
                future_to_item,
                timeout=_REFILL_POLL_SEC,
                return_when=FIRST_COMPLETED,
            )
            for done_future in done_futures:
                item = future_to_item.pop(done_future)
                try:
                    item_exit_code = int(done_future.result())
                except Exception:
                    item_exit_code = 1
                    traceback.print_exc()
                if item_exit_code != 0:
                    exit_code = 1
            for refill_item in _claim_refill_items(
                root,
                manifest_path=manifest_path,
                track=track,
                market=market,
                batch_id=batch_id,
                submitted_ids=submitted_ids,
                max_items=max_items,
                open_slots=max(0, worker_count - len(future_to_item)),
            ):
                refill_id = str(refill_item.get("id") or "").strip()
                if refill_id:
                    submitted_ids.add(refill_id)
                future_to_item[executor.submit(_run_pool_item, root, refill_item, top_k)] = refill_item
            reporter.maybe_report()

    _cleanup_after_item()
    reporter.maybe_report(force=True)
    print(f"[quick_screen_pool] finished batch_id={batch_id} exit_code={exit_code}", flush=True)
    return exit_code


def _pool_track(items: list[dict[str, object]]) -> str:
    tracks = {
        str(item.get("track") or "").strip().lower()
        for item in items
        if str(item.get("track") or "").strip()
    }
    if len(tracks) == 1:
        return next(iter(tracks))
    return ""


def _pool_market(items: list[dict[str, object]]) -> str:
    markets = {
        str(item.get("market") or "").strip().lower()
        for item in items
        if str(item.get("market") or "").strip()
    }
    if len(markets) == 1:
        return next(iter(markets))
    return ""


def _append_refill_items(
    root: Path,
    items: list[dict[str, object]],
    *,
    manifest_path: Path,
    track: str,
    market: str,
    batch_id: str,
    max_items: int,
) -> list[dict[str, object]]:
    limit = max(0, int(max_items or 0))
    if limit <= 0 or len(items) >= limit:
        return list(items)
    refill_items = _claim_refill_items(
        root,
        manifest_path=manifest_path,
        track=track,
        market=market,
        batch_id=batch_id,
        submitted_ids={str(item.get("id") or "").strip() for item in items},
        max_items=limit,
        open_slots=limit - len(items),
    )
    return [*items, *refill_items]


def _claim_refill_items(
    root: Path,
    *,
    manifest_path: Path | None = None,
    track: str,
    market: str,
    batch_id: str,
    submitted_ids: set[str],
    max_items: int,
    open_slots: int,
) -> list[dict[str, object]]:
    if not track or open_slots <= 0:
        return []
    queue_path = experiment_queue_path(root)
    lock_path = queue_path.with_suffix(queue_path.suffix + ".lock")
    with _exclusive_file_lock(lock_path):
        payload = load_experiment_queue(root)
        existing_batch_count = sum(
            1
            for item in payload.get("items") or []
            if isinstance(item, dict)
            and str(item.get("batch_id") or "").strip() == batch_id
            and str(item.get("status") or "").strip().lower() in {"running", "queued", "repair"}
        )
        remaining_capacity = _remaining_refill_capacity(
            existing_batch_count=existing_batch_count,
            submitted_count=len(submitted_ids),
            max_items=max_items,
            open_slots=open_slots,
        )
        if remaining_capacity <= 0:
            return []

        claimed_ids: set[str] = set()
        claimed_items: list[dict[str, object]] = []
        updated_items: list[dict[str, object]] = []
        for raw_item in payload.get("items") or []:
            if not isinstance(raw_item, dict):
                continue
            item = dict(raw_item)
            item_id = str(item.get("id") or "").strip()
            if (
                len(claimed_items) < remaining_capacity
                and _is_claimable_refill_item(item, track=track, market=market)
                and item_id
                and item_id not in submitted_ids
                and item_id not in claimed_ids
            ):
                item["status"] = "running"
                item["batch_id"] = batch_id
                item["batch_manifest_path"] = ""
                item["pid"] = os.getpid()
                item["updated_at"] = _utc_now()
                claimed_ids.add(item_id)
                claimed_items.append(dict(item))
            updated_items.append(item)
        if not claimed_items:
            return []
        payload["items"] = updated_items
        save_experiment_queue(root, payload)
        if manifest_path is not None:
            _append_manifest_items(manifest_path, claimed_items)
        print(
            "[quick_screen_pool] refill "
            f"batch_id={batch_id} claimed={len(claimed_items)} track={track}",
            flush=True,
        )
        return claimed_items


def _append_manifest_items(manifest_path: Path, claimed_items: list[dict[str, object]]) -> None:
    if not claimed_items:
        return
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    existing_items = [
        dict(item)
        for item in payload.get("items") or []
        if isinstance(item, dict)
    ]
    existing_ids = {
        str(item.get("id") or "").strip()
        for item in existing_items
        if str(item.get("id") or "").strip()
    }
    for item in claimed_items:
        item_id = str(item.get("id") or "").strip()
        if item_id and item_id in existing_ids:
            continue
        existing_items.append(dict(item))
        if item_id:
            existing_ids.add(item_id)
    payload["items"] = existing_items
    tmp_path = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    tmp_path.replace(manifest_path)


def _remaining_refill_capacity(
    *,
    existing_batch_count: int,
    submitted_count: int,
    max_items: int,
    open_slots: int,
) -> int:
    limit = max(0, int(max_items or 0))
    if limit > 0:
        limit_remaining = limit - max(int(existing_batch_count), int(submitted_count))
        if limit_remaining <= 0:
            return 0
        return min(int(open_slots), limit_remaining)
    return int(open_slots)


def _is_claimable_refill_item(item: dict[str, object], *, track: str, market: str) -> bool:
    status = str(item.get("status") or "").strip().lower()
    if status not in {"queued", "repair"}:
        return False
    if str(item.get("track") or "").strip().lower() != track:
        return False
    if market and str(item.get("market") or "").strip().lower() != market:
        return False
    if str(item.get("batch_id") or "").strip():
        return False
    if not str(item.get("id") or "").strip():
        return False
    if not str(item.get("suite_name") or "").strip():
        return False
    if not str(item.get("run_label") or "").strip():
        return False
    return True


class _exclusive_file_lock:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None

    def __enter__(self):
        import fcntl

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self._path.open("a+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        import fcntl

        if self._fh is None:
            return
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None


def _utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_pool_item(root: Path, item: dict[str, object], top_k: int) -> int:
    suite_name = str(item.get("suite_name") or "").strip()
    run_label = str(item.get("run_label") or "").strip()
    track = str(item.get("track") or "").strip()
    if not suite_name or not run_label:
        print(f"[quick_screen_pool] skip invalid item id={item.get('id')}", flush=True)
        return 1
    print(
        f"[quick_screen_pool] item start suite={suite_name} run_label={run_label} track={track}",
        flush=True,
    )
    try:
        run_quick_screen_suite(
            suite_name=suite_name,
            run_label=run_label,
            top_k=int(top_k),
            cleanup_between_cases=False,
        )
    except Exception as exc:
        traceback.print_exc()
        if _is_non_retryable_quick_screen_error(exc):
            _set_queue_status(
                root,
                item,
                status="dead",
                reason="quick_screen_pool_nonretryable_failure",
                last_error=str(exc),
            )
        else:
            _set_queue_status(
                root,
                item,
                status="repair",
                reason="quick_screen_pool_item_failed",
                last_error=str(exc),
            )
        return 1

    _set_queue_status(root, item, status="done", reason="quick_screen_pool_item_completed")
    _touch_wake_flag()
    print(
        f"[quick_screen_pool] item done suite={suite_name} run_label={run_label} track={track}",
        flush=True,
    )
    return 0


def _cleanup_after_item() -> None:
    with _CLEANUP_LOCK:
        clear_process_scoring_runtime_cache()
        clear_process_backtest_runtime_cache()
        gc.collect()
        trim_process_memory()


def _set_queue_status(
    root: Path,
    item: dict[str, object],
    *,
    status: str,
    reason: str,
    last_error: str | None = None,
) -> None:
    cmd = [
        sys.executable,
        str(root / "auto_research" / "experiment_queue.py"),
        "--root",
        str(root),
        "set-status",
        "--item-id",
        str(item.get("id") or ""),
        "--status",
        status,
        "--reason",
        reason,
    ]
    if last_error:
        cmd.extend(["--last-error", last_error])
    track = str(item.get("track") or "").strip()
    if track:
        cmd.extend(["--track", track])
    result = subprocess.run(
        cmd,
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(root / "src")},
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "queue status update failed"
        print(f"[quick_screen_pool] queue status update failed run_label={item.get('run_label')} error={message}", flush=True)


def _is_non_retryable_quick_screen_error(exc: BaseException) -> bool:
    if isinstance(exc, ValueError) and "Unsupported feature_set" in str(exc):
        return True
    cause = exc.__cause__
    while cause is not None:
        if isinstance(cause, ValueError) and "Unsupported feature_set" in str(cause):
            return True
        cause = cause.__cause__
    return False


def _touch_wake_flag() -> None:
    wake_flag = str(os.environ.get("PM15MIN_AUTORESEARCH_WAKE_FLAG") or "").strip()
    if not wake_flag:
        return
    path = Path(wake_flag).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch()


class _MemoryReporter:
    def __init__(self, *, batch_id: str, interval_sec: int) -> None:
        self._batch_id = batch_id
        self._interval_sec = max(0, int(interval_sec))
        self._next_report_at = 0.0

    def maybe_report(self, *, force: bool = False) -> None:
        if self._interval_sec <= 0 and not force:
            return
        now = time.monotonic()
        if not force and now < self._next_report_at:
            return
        self._next_report_at = now + float(self._interval_sec)
        payload = _process_memory_payload()
        print(
            "[quick_screen_pool] memory "
            f"batch_id={self._batch_id} "
            f"rss_mib={payload.get('rss_mib')} "
            f"available_mib={payload.get('available_mib')}",
            flush=True,
        )


def _process_memory_payload() -> dict[str, object]:
    rss_kb = _read_self_status_kb("VmRSS")
    available_kb = _read_meminfo_kb("MemAvailable")
    return {
        "rss_mib": None if rss_kb is None else round(rss_kb / 1024.0, 1),
        "available_mib": None if available_kb is None else round(available_kb / 1024.0, 1),
    }


def _read_self_status_kb(key: str) -> int | None:
    return _read_keyed_kb(Path("/proc/self/status"), key)


def _read_meminfo_kb(key: str) -> int | None:
    return _read_keyed_kb(Path("/proc/meminfo"), key)


def _read_keyed_kb(path: Path, key: str) -> int | None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    prefix = f"{key}:"
    for line in lines:
        if not line.startswith(prefix):
            continue
        parts = line.split()
        if len(parts) < 2:
            return None
        try:
            return int(parts[1])
        except ValueError:
            return None
    return None


if __name__ == "__main__":
    raise SystemExit(main())
