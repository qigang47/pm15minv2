from __future__ import annotations

from dataclasses import dataclass
import os
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Any

from ..config import DataConfig
from ..io.json_files import append_jsonl, write_json_atomic
from .orderbook_recording import (
    CapturedOrderbookBatch,
    capture_orderbooks_once,
    persist_captured_orderbooks_once,
    record_orderbooks_once,
    summarize_captured_orderbook_batch,
)
from ..sources.orderbook_provider import OrderbookProvider, build_orderbook_provider_from_env
from ..sources.polymarket_clob import PolymarketClobClient

_ASYNC_STOP = object()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_state(cfg: DataConfig, payload: dict[str, Any]) -> None:
    write_json_atomic(payload, cfg.layout.orderbook_state_path)


def _append_log(cfg: DataConfig, payload: dict[str, Any]) -> None:
    append_jsonl(cfg.layout.recorder_log_path, payload)


@dataclass
class _AsyncPersistStats:
    persisted_iterations: int = 0
    pending_batches: int = 0
    dropped_batches: int = 0
    dropped_snapshot_rows: int = 0
    write_errors: int = 0
    last_persisted_at: str | None = None
    last_persisted_summary: dict[str, Any] | None = None
    last_write_error: str | None = None


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, *, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return max(minimum, int(default))
    try:
        value = int(raw)
    except Exception:
        value = int(default)
    return max(minimum, value)


def _use_async_persist(cfg: DataConfig, *, loop: bool, async_persist: bool | None) -> bool:
    if async_persist is not None:
        return bool(async_persist)
    default_enabled = bool(loop and str(cfg.surface).lower() == "live")
    return _env_bool("PM15MIN_ORDERBOOK_ASYNC_PERSIST", default=default_enabled)


def _async_persistence_mode(*, pending_limit: int) -> str:
    return "async_unbounded" if int(pending_limit) <= 0 else "async_bounded"


def _async_state_payload(
    *,
    stats: _AsyncPersistStats,
    persistence_mode: str,
) -> dict[str, Any]:
    return {
        "persistence_mode": persistence_mode,
        "persisted_iterations": int(stats.persisted_iterations),
        "pending_batches": int(stats.pending_batches),
        "dropped_batches": int(stats.dropped_batches),
        "dropped_snapshot_rows": int(stats.dropped_snapshot_rows),
        "write_errors": int(stats.write_errors),
        "last_persisted_at": stats.last_persisted_at,
        "last_persisted_summary": stats.last_persisted_summary,
        "last_write_error": stats.last_write_error,
    }


def _combined_error_count(*, iteration_errors: int, stats: _AsyncPersistStats | None) -> int:
    return int(iteration_errors) + int(0 if stats is None else stats.write_errors)


def _async_last_error(stats: _AsyncPersistStats | None) -> str | None:
    if stats is None:
        return None
    if stats.last_write_error:
        return str(stats.last_write_error)
    if int(stats.write_errors) > 0:
        return f"persist_error_count={int(stats.write_errors)}"
    return None


def run_orderbook_recorder(
    cfg: DataConfig,
    *,
    client: PolymarketClobClient | None = None,
    provider: OrderbookProvider | None = None,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float | None = None,
    async_persist: bool | None = None,
    max_pending_batches: int | None = None,
    drop_oldest_when_full: bool | None = None,
    capture_orderbooks_once_fn=capture_orderbooks_once,
    persist_captured_orderbooks_once_fn=persist_captured_orderbooks_once,
) -> dict[str, object]:
    provider = provider or build_orderbook_provider_from_env(
        client=client,
        source_name=f"v2-recorder:{cfg.asset.slug}:{cfg.cycle}",
        subscribe_on_read=False,
    )
    sleep_sec = cfg.poll_interval_sec if sleep_sec is None else max(0.0, float(sleep_sec))
    iterations = int(iterations)
    if not loop:
        iterations = max(1, iterations)
    provider_name = str(getattr(provider, "__class__", type(provider)).__name__)
    ok_log_interval_sec = max(0.0, float(os.getenv("PM15MIN_ORDERBOOK_OK_LOG_INTERVAL_SEC", "60")))
    next_ok_log_at = 0.0
    async_enabled = _use_async_persist(cfg, loop=loop, async_persist=async_persist)
    pending_limit = max_pending_batches
    if pending_limit is None:
        pending_limit = _env_int("PM15MIN_ORDERBOOK_ASYNC_MAX_PENDING_BATCHES", default=2, minimum=0)
    pending_limit = max(0, int(pending_limit))
    async_persistence_mode = _async_persistence_mode(pending_limit=pending_limit)
    drop_oldest = bool(
        _env_bool("PM15MIN_ORDERBOOK_ASYNC_DROP_OLDEST_WHEN_FULL", default=True)
        if drop_oldest_when_full is None
        else drop_oldest_when_full
    )

    run_started_at = _utc_now_iso()
    completed = 0
    errors = 0
    last_summary: dict[str, Any] | None = None
    last_completed_at: str | None = None
    next_iteration_at = time.monotonic()
    async_stats = _AsyncPersistStats()
    stats_lock = threading.Lock()
    pending_batches: queue.Queue[object] | None = None
    writer_thread: threading.Thread | None = None
    writer_finalized = False

    if async_enabled:
        pending_batches = queue.Queue(maxsize=pending_limit)

        def _writer_loop() -> None:
            assert pending_batches is not None
            while True:
                try:
                    item = pending_batches.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    if item is _ASYNC_STOP:
                        return
                    batch = item
                    summary = persist_captured_orderbooks_once_fn(cfg, batch=batch)
                    with stats_lock:
                        async_stats.persisted_iterations += 1
                        async_stats.pending_batches = pending_batches.qsize()
                        async_stats.last_persisted_at = _utc_now_iso()
                        async_stats.last_persisted_summary = summary
                except Exception as exc:
                    error_text = f"{type(exc).__name__}: {exc}"
                    with stats_lock:
                        async_stats.write_errors += 1
                        async_stats.pending_batches = pending_batches.qsize()
                        async_stats.last_write_error = error_text
                    try:
                        _append_log(
                            cfg,
                            {
                                "ts": _utc_now_iso(),
                                "event": "persist_error",
                                "market": cfg.asset.slug,
                                "cycle": cfg.cycle,
                                "provider": provider_name,
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                            },
                        )
                    except Exception as log_exc:
                        with stats_lock:
                            async_stats.last_write_error = (
                                f"{error_text}; log_error={type(log_exc).__name__}: {log_exc}"
                            )
                finally:
                    pending_batches.task_done()

        writer_thread = threading.Thread(
            target=_writer_loop,
            name=f"orderbook-persist-{cfg.asset.slug}-{cfg.cycle}",
            daemon=False,
        )
        writer_thread.start()

    def _finalize_async_writer() -> None:
        nonlocal writer_finalized
        if writer_finalized or not async_enabled:
            return
        assert pending_batches is not None
        assert writer_thread is not None
        pending_batches.join()
        pending_batches.put(_ASYNC_STOP)
        pending_batches.join()
        writer_thread.join()
        with stats_lock:
            async_stats.pending_batches = pending_batches.qsize()
        writer_finalized = True

    _write_state(
        cfg,
        {
            "status": "running",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "run_started_at": run_started_at,
            "iteration": 0,
            "completed_iterations": 0,
            "errors": 0,
            "last_summary": None,
            "last_error": None,
            **(
                _async_state_payload(stats=async_stats, persistence_mode=async_persistence_mode)
                if async_enabled
                else {"persistence_mode": "sync"}
            ),
        },
    )

    try:
        while True:
            if loop:
                if iterations > 0 and completed >= iterations:
                    break
            else:
                if completed >= iterations:
                    break

            iteration_started_monotonic = time.monotonic()
            iteration_no = completed + errors + 1
            iteration_started_at = _utc_now_iso()
            try:
                if async_enabled:
                    batch = capture_orderbooks_once_fn(
                        cfg,
                        provider=provider,
                    )
                    summary = summarize_captured_orderbook_batch(cfg, batch=batch)
                    assert pending_batches is not None
                    try:
                        pending_batches.put_nowait(batch)
                    except queue.Full:
                        if drop_oldest:
                            try:
                                dropped = pending_batches.get_nowait()
                            except queue.Empty:
                                dropped = None
                            if dropped is not None:
                                pending_batches.task_done()
                                with stats_lock:
                                    async_stats.dropped_batches += 1
                                    async_stats.dropped_snapshot_rows += len(dropped.snapshot_rows)
                            pending_batches.put_nowait(batch)
                            _append_log(
                                cfg,
                                {
                                    "ts": _utc_now_iso(),
                                    "event": "persist_backlog_drop_oldest",
                                    "iteration": iteration_no,
                                    "market": cfg.asset.slug,
                                    "cycle": cfg.cycle,
                                    "provider": provider_name,
                                    "dropped_captured_ts_ms": None if dropped is None else dropped.captured_ts_ms,
                                    "pending_limit": pending_batches.maxsize,
                                },
                            )
                        else:
                            pending_batches.put(batch)
                    with stats_lock:
                        async_stats.pending_batches = pending_batches.qsize()
                else:
                    summary = record_orderbooks_once(
                        cfg,
                        provider=provider,
                    )
                completed += 1
                last_summary = summary
                last_completed_at = _utc_now_iso()
                state_payload = {
                    "status": "running" if loop and (iterations <= 0 or completed < iterations) else "ok",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "run_started_at": run_started_at,
                    "iteration": iteration_no,
                    "completed_iterations": completed,
                    "errors": _combined_error_count(
                        iteration_errors=errors,
                        stats=async_stats if async_enabled else None,
                    ),
                    "last_summary": summary,
                    "last_error": _async_last_error(async_stats if async_enabled else None),
                    "last_completed_at": last_completed_at,
                }
                if async_enabled:
                    with stats_lock:
                        state_payload.update(
                            _async_state_payload(stats=async_stats, persistence_mode=async_persistence_mode)
                        )
                else:
                    state_payload["persistence_mode"] = "sync"
                _write_state(cfg, state_payload)
                now_ts = time.time()
                if ok_log_interval_sec <= 0 or now_ts >= next_ok_log_at or iteration_no == 1:
                    next_ok_log_at = now_ts + ok_log_interval_sec
                    log_payload = {
                        "ts": iteration_started_at,
                        "event": "iteration_ok",
                        "iteration": iteration_no,
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "provider": provider_name,
                        "captured_ts_ms": summary.get("captured_ts_ms"),
                        "selected_markets": summary.get("selected_markets"),
                        "market_start_offset": summary.get("market_start_offset"),
                        "snapshot_rows": summary.get("snapshot_rows"),
                        "recent_window_minutes": summary.get("recent_window_minutes"),
                    }
                    if async_enabled:
                        with stats_lock:
                            log_payload.update(
                                {
                                    "persistence_mode": async_persistence_mode,
                                    "pending_batches": async_stats.pending_batches,
                                    "persisted_iterations": async_stats.persisted_iterations,
                                    "dropped_batches": async_stats.dropped_batches,
                                }
                            )
                    else:
                        log_payload["recent_rows"] = summary.get("recent_rows")
                        log_payload["persistence_mode"] = "sync"
                    _append_log(cfg, log_payload)
            except KeyboardInterrupt:
                if async_enabled:
                    _finalize_async_writer()
                _write_state(
                    cfg,
                    {
                        "status": "stopped",
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "provider": provider_name,
                        "run_started_at": run_started_at,
                        "iteration": iteration_no,
                        "completed_iterations": completed,
                        "errors": _combined_error_count(
                            iteration_errors=errors,
                            stats=async_stats if async_enabled else None,
                        ),
                        "last_summary": last_summary,
                        "last_error": "KeyboardInterrupt",
                        "last_completed_at": last_completed_at,
                        "stopped_at": _utc_now_iso(),
                        **(
                            _async_state_payload(stats=async_stats, persistence_mode=async_persistence_mode)
                            if async_enabled
                            else {"persistence_mode": "sync"}
                        ),
                    },
                )
                _append_log(
                    cfg,
                    {
                        "ts": _utc_now_iso(),
                        "event": "stopped",
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "provider": provider_name,
                        "completed_iterations": completed,
                        "errors": _combined_error_count(
                            iteration_errors=errors,
                            stats=async_stats if async_enabled else None,
                        ),
                    },
                )
                return {
                    "status": "stopped",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "completed_iterations": completed,
                    "persisted_iterations": async_stats.persisted_iterations if async_enabled else completed,
                    "dropped_batches": async_stats.dropped_batches if async_enabled else 0,
                    "errors": _combined_error_count(
                        iteration_errors=errors,
                        stats=async_stats if async_enabled else None,
                    ),
                    "state_path": str(cfg.layout.orderbook_state_path),
                    "log_path": str(cfg.layout.recorder_log_path),
                }
            except Exception as exc:
                errors += 1
                error_payload = {
                    "status": "error" if not loop else "running",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "run_started_at": run_started_at,
                    "iteration": iteration_no,
                    "completed_iterations": completed,
                    "errors": _combined_error_count(
                        iteration_errors=errors,
                        stats=async_stats if async_enabled else None,
                    ),
                    "last_summary": last_summary,
                    "last_error": f"{type(exc).__name__}: {exc}",
                    "last_completed_at": last_completed_at,
                    "last_error_at": _utc_now_iso(),
                    **(
                        _async_state_payload(stats=async_stats, persistence_mode=async_persistence_mode)
                        if async_enabled
                        else {"persistence_mode": "sync"}
                    ),
                }
                _write_state(cfg, error_payload)
                _append_log(
                    cfg,
                    {
                        "ts": _utc_now_iso(),
                        "event": "iteration_error",
                        "iteration": iteration_no,
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "provider": provider_name,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
                if not loop:
                    if async_enabled:
                        _finalize_async_writer()
                    raise

            if loop and iterations > 0 and completed >= iterations:
                continue
            if loop:
                next_iteration_at = max(next_iteration_at + sleep_sec, iteration_started_monotonic)
                remaining_sleep = next_iteration_at - time.monotonic()
                if remaining_sleep > 0:
                    time.sleep(remaining_sleep)

        if async_enabled:
            _finalize_async_writer()

        total_errors = _combined_error_count(
            iteration_errors=errors,
            stats=async_stats if async_enabled else None,
        )
        final_last_error = _async_last_error(async_stats if async_enabled else None)
        final_status = "error" if total_errors > 0 else "ok"

        final_payload = {
            "status": final_status,
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "run_started_at": run_started_at,
            "completed_iterations": completed,
            "errors": total_errors,
            "last_summary": last_summary,
            "last_error": final_last_error,
            "last_completed_at": last_completed_at,
            "finished_at": _utc_now_iso(),
            **(
                _async_state_payload(stats=async_stats, persistence_mode=async_persistence_mode)
                if async_enabled
                else {"persistence_mode": "sync"}
            ),
        }
        _write_state(cfg, final_payload)
        _append_log(
            cfg,
            {
                "ts": _utc_now_iso(),
                "event": "run_finished",
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "provider": provider_name,
                "completed_iterations": completed,
                "errors": total_errors,
            },
        )
        return {
            "status": final_status,
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "completed_iterations": completed,
            "persisted_iterations": async_stats.persisted_iterations if async_enabled else completed,
            "dropped_batches": async_stats.dropped_batches if async_enabled else 0,
            "errors": total_errors,
            "state_path": str(cfg.layout.orderbook_state_path),
            "log_path": str(cfg.layout.recorder_log_path),
            "last_summary": last_summary,
            "last_completed_at": last_completed_at,
        }
    except Exception:
        raise
