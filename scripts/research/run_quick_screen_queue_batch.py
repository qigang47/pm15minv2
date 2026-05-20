#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.backtests.runtime_cache import clear_process_backtest_runtime_cache
from pm15min.core.process_memory import trim_process_memory
from pm15min.research.inference.scorer import clear_process_scoring_runtime_cache

SCRIPT_DIR = ROOT / "scripts" / "research"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from run_quick_screen_suite import run_quick_screen_suite


def main() -> int:
    parser = argparse.ArgumentParser(description="Run queued quick-screen suites sequentially in one worker.")
    parser.add_argument("--root", default=str(ROOT))
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--top-k", type=int, default=1)
    args = parser.parse_args()

    root = Path(args.root).resolve()
    manifest_path = Path(args.manifest).expanduser().resolve()
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    items = [dict(item) for item in payload.get("items") or [] if isinstance(item, dict)]
    if not items:
        raise SystemExit("quick-screen batch manifest has no items")

    print(f"[quick_screen_batch] start batch_id={args.batch_id} items={len(items)} manifest={manifest_path}", flush=True)
    exit_code = 0
    for index, item in enumerate(items, start=1):
        suite_name = str(item.get("suite_name") or "").strip()
        run_label = str(item.get("run_label") or "").strip()
        track = str(item.get("track") or "").strip()
        if not suite_name or not run_label:
            print(f"[quick_screen_batch] skip invalid item index={index}", flush=True)
            exit_code = 1
            continue
        print(
            f"[quick_screen_batch] item {index}/{len(items)} suite={suite_name} run_label={run_label} track={track}",
            flush=True,
        )
        try:
            run_quick_screen_suite(suite_name=suite_name, run_label=run_label, top_k=int(args.top_k))
        except Exception as exc:
            exit_code = 1
            traceback.print_exc()
            if _is_non_retryable_quick_screen_error(exc):
                _set_queue_status(
                    root,
                    item,
                    status="dead",
                    reason="quick_screen_batch_nonretryable_failure",
                    last_error=str(exc),
                )
            else:
                _set_queue_status(
                    root,
                    item,
                    status="repair",
                    reason="quick_screen_batch_item_failed",
                    last_error=str(exc),
                )
            clear_process_scoring_runtime_cache()
            clear_process_backtest_runtime_cache()
            gc.collect()
            trim_process_memory()
            continue
        _set_queue_status(root, item, status="done", reason=f"quick_screen_batch_completed:{args.batch_id}")
        _touch_wake_flag()
        clear_process_scoring_runtime_cache()
        clear_process_backtest_runtime_cache()
        gc.collect()
        trim_process_memory()
    clear_process_scoring_runtime_cache()
    clear_process_backtest_runtime_cache()
    gc.collect()
    trim_process_memory()
    print(f"[quick_screen_batch] finished batch_id={args.batch_id} exit_code={exit_code}", flush=True)
    return exit_code


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
        print(f"[quick_screen_batch] queue status update failed run_label={item.get('run_label')} error={message}", flush=True)


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


if __name__ == "__main__":
    raise SystemExit(main())
