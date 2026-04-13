from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

from pmshared.io.json_files import append_jsonl, write_json_atomic

from ..config import DataConfig
from ..sources.orderbook_provider import OrderbookProvider, build_orderbook_provider_from_env
from ..sources.polymarket_clob import PolymarketClobClient
from .orderbook_recording import record_orderbooks_once


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_state(cfg: DataConfig, payload: dict[str, Any]) -> None:
    write_json_atomic(payload, cfg.layout.orderbook_state_path)


def _append_log(cfg: DataConfig, payload: dict[str, Any]) -> None:
    append_jsonl(cfg.layout.recorder_log_path, payload)


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
) -> dict[str, object]:
    del async_persist, max_pending_batches, drop_oldest_when_full
    provider = provider or build_orderbook_provider_from_env(
        client=client,
        source_name=f"v2-recorder:{cfg.asset.slug}:{cfg.cycle}",
        subscribe_on_read=False,
    )
    sleep_sec = cfg.poll_interval_sec if sleep_sec is None else max(0.0, float(sleep_sec))
    iterations = max(1, int(iterations))
    provider_name = str(getattr(provider, "__class__", type(provider)).__name__)
    run_started_at = _utc_now_iso()
    completed = 0
    errors = 0
    last_summary: dict[str, Any] | None = None
    last_error: str | None = None
    last_completed_at: str | None = None

    _write_state(
        cfg,
        {
            "status": "running",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "run_started_at": run_started_at,
            "completed_iterations": 0,
            "errors": 0,
            "last_summary": None,
            "last_error": None,
            "persistence_mode": "sync",
        },
    )

    while True:
        if not loop and completed >= iterations:
            break
        if loop and iterations > 0 and completed >= iterations:
            break
        iteration_no = completed + errors + 1
        try:
            summary = record_orderbooks_once(
                cfg,
                provider=provider,
            )
            completed += 1
            last_summary = summary
            last_completed_at = _utc_now_iso()
            _write_state(
                cfg,
                {
                    "status": "running" if loop and completed < iterations else "ok",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "run_started_at": run_started_at,
                    "completed_iterations": completed,
                    "errors": errors,
                    "last_summary": summary,
                    "last_error": last_error,
                    "last_completed_at": last_completed_at,
                    "persistence_mode": "sync",
                },
            )
            _append_log(
                cfg,
                {
                    "ts": _utc_now_iso(),
                    "event": "iteration_ok",
                    "iteration": iteration_no,
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "snapshot_rows": summary.get("snapshot_rows"),
                },
            )
        except Exception as exc:
            errors += 1
            last_error = f"{type(exc).__name__}: {exc}"
            _write_state(
                cfg,
                {
                    "status": "error",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "run_started_at": run_started_at,
                    "completed_iterations": completed,
                    "errors": errors,
                    "last_summary": last_summary,
                    "last_error": last_error,
                    "last_completed_at": last_completed_at,
                    "persistence_mode": "sync",
                },
            )
            _append_log(
                cfg,
                {
                    "ts": _utc_now_iso(),
                    "event": "iteration_error",
                    "iteration": iteration_no,
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "error": str(exc),
                },
            )
            if not loop:
                raise
        if loop and sleep_sec > 0:
            time.sleep(sleep_sec)

    finished_at = _utc_now_iso()
    final_status = "error" if errors > 0 else "ok"
    _write_state(
        cfg,
        {
            "status": final_status,
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "run_started_at": run_started_at,
            "completed_iterations": completed,
            "errors": errors,
            "last_summary": last_summary,
            "last_error": last_error,
            "last_completed_at": last_completed_at,
            "finished_at": finished_at,
            "persistence_mode": "sync",
        },
    )
    _append_log(
        cfg,
        {
            "ts": finished_at,
            "event": "run_finished",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "completed_iterations": completed,
            "errors": errors,
        },
    )
    return {
        "status": final_status,
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "provider": provider_name,
        "completed_iterations": completed,
        "persisted_iterations": completed,
        "dropped_batches": 0,
        "errors": errors,
        "state_path": str(cfg.layout.orderbook_state_path),
        "log_path": str(cfg.layout.recorder_log_path),
        "last_summary": last_summary,
        "last_completed_at": last_completed_at,
    }
