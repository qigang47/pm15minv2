from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

from ..config import DataConfig
from ..io.json_files import append_jsonl, write_json_atomic
from .orderbook_recording import record_orderbooks_once
from ..sources.orderbook_provider import OrderbookProvider, build_orderbook_provider_from_env
from ..sources.polymarket_clob import PolymarketClobClient


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

    run_started_at = _utc_now_iso()
    completed = 0
    errors = 0
    last_summary: dict[str, Any] | None = None

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

            iteration_no = completed + errors + 1
            iteration_started_at = _utc_now_iso()
            try:
                summary = record_orderbooks_once(
                    cfg,
                    provider=provider,
                )
                completed += 1
                last_summary = summary
                state_payload = {
                    "status": "running" if loop and (iterations <= 0 or completed < iterations) else "ok",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "provider": provider_name,
                    "run_started_at": run_started_at,
                    "iteration": iteration_no,
                    "completed_iterations": completed,
                    "errors": errors,
                    "last_summary": summary,
                    "last_error": None,
                    "last_completed_at": _utc_now_iso(),
                }
                _write_state(cfg, state_payload)
                now_ts = time.time()
                if ok_log_interval_sec <= 0 or now_ts >= next_ok_log_at or iteration_no == 1:
                    next_ok_log_at = now_ts + ok_log_interval_sec
                    _append_log(
                        cfg,
                        {
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
                            "recent_rows": summary.get("recent_rows"),
                            "recent_window_minutes": summary.get("recent_window_minutes"),
                        },
                    )
            except KeyboardInterrupt:
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
                        "errors": errors,
                        "last_summary": last_summary,
                        "last_error": "KeyboardInterrupt",
                        "stopped_at": _utc_now_iso(),
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
                        "errors": errors,
                    },
                )
                return {
                    "status": "stopped",
                    "market": cfg.asset.slug,
                    "cycle": cfg.cycle,
                    "completed_iterations": completed,
                    "errors": errors,
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
                    "errors": errors,
                    "last_summary": last_summary,
                    "last_error": f"{type(exc).__name__}: {exc}",
                    "last_error_at": _utc_now_iso(),
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
                    raise

            if loop and sleep_sec > 0:
                time.sleep(sleep_sec)

        final_payload = {
            "status": "ok",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "run_started_at": run_started_at,
            "completed_iterations": completed,
            "errors": errors,
            "last_summary": last_summary,
            "last_error": None if errors == 0 else None,
            "finished_at": _utc_now_iso(),
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
                "errors": errors,
            },
        )
        return {
            "status": "ok",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "provider": provider_name,
            "completed_iterations": completed,
            "errors": errors,
            "state_path": str(cfg.layout.orderbook_state_path),
            "log_path": str(cfg.layout.recorder_log_path),
            "last_summary": last_summary,
        }
    except Exception:
        raise
