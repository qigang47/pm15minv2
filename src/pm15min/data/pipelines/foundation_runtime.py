from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from ..config import DataConfig
from ..io.json_files import append_jsonl, write_json_atomic
from .binance_klines import sync_binance_klines_1m
from .direct_sync import sync_streams_from_rpc
from .direct_oracle_prices import sync_polymarket_oracle_prices_direct
from .market_catalog import sync_market_catalog
from .oracle_prices import build_oracle_prices_15m
from .orderbook_runtime import run_orderbook_recorder
from ..sources.binance_spot import BinanceSpotKlinesClient
from ..sources.polymarket_clob import PolymarketClobClient
from ..sources.polymarket_gamma import GammaEventsClient
from ..sources.polymarket_oracle_api import PolymarketOracleApiClient


def _utc_now(now_provider: Callable[[], datetime] | None = None) -> datetime:
    now = now_provider() if now_provider is not None else datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _utc_now_iso(now_provider: Callable[[], datetime] | None = None) -> str:
    return _utc_now(now_provider).strftime("%Y-%m-%dT%H:%M:%SZ")


def _write_state(cfg: DataConfig, payload: dict[str, Any]) -> None:
    write_json_atomic(payload, cfg.layout.foundation_state_path)


def _append_log(cfg: DataConfig, payload: dict[str, Any]) -> None:
    append_jsonl(cfg.layout.foundation_log_path, payload)


def _binance_markets_for_foundation(cfg: DataConfig) -> list[str]:
    markets = [cfg.asset.slug]
    if cfg.asset.slug != "btc":
        markets.append("btc")
    return markets


def _run_market_catalog_step(
    cfg: DataConfig,
    *,
    now: datetime,
    client: GammaEventsClient | None,
    lookback_hours: int,
    lookahead_hours: int,
) -> dict[str, object]:
    start_ts = int((now - timedelta(hours=max(1, int(lookback_hours)))).timestamp())
    end_ts = int((now + timedelta(hours=max(1, int(lookahead_hours)))).timestamp())
    return sync_market_catalog(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        client=client,
        now=now,
    )


def _run_binance_step(
    cfg: DataConfig,
    *,
    now: datetime,
    client: BinanceSpotKlinesClient | None,
    lookback_minutes: int,
    batch_limit: int,
) -> dict[str, object]:
    summaries: list[dict[str, object]] = []
    for market in _binance_markets_for_foundation(cfg):
        market_cfg = DataConfig.build(
            market=market,
            cycle=cfg.cycle,
            surface=cfg.surface,
            root=cfg.layout.storage.rewrite_root,
        )
        summaries.append(
            sync_binance_klines_1m(
                market_cfg,
                client=client,
                now=now,
                lookback_minutes=lookback_minutes,
                batch_limit=batch_limit,
            )
        )
    return {
        "dataset": "foundation_binance_klines_1m",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "summaries": summaries,
    }


def _run_oracle_step(
    cfg: DataConfig,
    *,
    now: datetime,
    client: PolymarketOracleApiClient | None,
    lookback_days: int,
    lookahead_hours: int,
) -> dict[str, object]:
    start_ts = int((now - timedelta(days=max(1, int(lookback_days)))).timestamp())
    end_ts = int((now + timedelta(hours=max(1, int(lookahead_hours)))).timestamp())
    direct_summary = sync_polymarket_oracle_prices_direct(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
        lookback_days=lookback_days,
        client=client,
    )
    table_summary = build_oracle_prices_15m(cfg)
    return {
        "dataset": "foundation_oracle_prices",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "direct_summary": direct_summary,
        "table_summary": table_summary,
    }


def _run_streams_step(
    cfg: DataConfig,
    *,
    now: datetime,
    lookback_days: int,
    lookahead_hours: int,
) -> dict[str, object]:
    start_ts = int((now - timedelta(days=max(1, int(lookback_days)))).timestamp())
    end_ts = int((now + timedelta(hours=max(1, int(lookahead_hours)))).timestamp())
    return sync_streams_from_rpc(
        cfg,
        start_ts=start_ts,
        end_ts=end_ts,
    )


def _run_orderbook_step(
    cfg: DataConfig,
    *,
    client: PolymarketClobClient | None,
) -> dict[str, object]:
    return run_orderbook_recorder(
        cfg,
        client=client,
        iterations=1,
        loop=False,
        sleep_sec=0.0,
    )


def _task_can_fail_open(cfg: DataConfig, *, task_name: str) -> bool:
    if task_name in {"oracle", "streams"} and cfg.layout.oracle_prices_table_path.exists():
        return True
    return False


def _classify_foundation_task_issue(*, task_name: str, error_type: str | None, error: str | None, status: str) -> str:
    task = str(task_name or "").strip().lower()
    error_type_token = str(error_type or "").strip().lower()
    error_text = str(error or "").strip().lower()
    if task == "oracle":
        if "too many requests" in error_text or "429" in error_text:
            return "oracle_direct_rate_limited"
        return "oracle_refresh_failed"
    if task == "streams":
        return "streams_refresh_failed"
    if task == "market_catalog":
        return "market_catalog_refresh_failed"
    if task == "binance":
        return "binance_refresh_failed"
    if task == "orderbooks":
        return "orderbook_refresh_failed"
    return f"{task}_failed" if task else f"foundation_task_{status}"


def _build_foundation_reason(*, degraded_tasks: list[dict[str, object]], error_tasks: list[dict[str, object]]) -> str | None:
    issues = [*degraded_tasks, *error_tasks]
    if not issues:
        return None
    parts: list[str] = []
    for row in issues:
        task = str(row.get("task") or "unknown")
        code = str(row.get("issue_code") or "unknown_issue")
        error = str(row.get("error") or "").strip()
        if error:
            parts.append(f"{task}:{code}:{error}")
        else:
            parts.append(f"{task}:{code}")
    return "; ".join(parts)


def _summarize_task_results(last_task_results: dict[str, Any]) -> tuple[list[dict[str, object]], list[dict[str, object]], list[str], str | None]:
    degraded_tasks = [
        {
            "task": task_name,
            "issue_code": row.get("issue_code"),
            "error_type": row.get("error_type"),
            "error": row.get("error"),
            "fail_open": bool(row.get("fail_open")),
            "fallback_path": row.get("fallback_path"),
        }
        for task_name, row in last_task_results.items()
        if isinstance(row, dict) and str(row.get("status") or "") == "degraded"
    ]
    error_tasks = [
        {
            "task": task_name,
            "issue_code": row.get("issue_code"),
            "error_type": row.get("error_type"),
            "error": row.get("error"),
        }
        for task_name, row in last_task_results.items()
        if isinstance(row, dict) and str(row.get("status") or "") == "error"
    ]
    issue_codes = [
        str(item.get("issue_code") or "")
        for item in [*degraded_tasks, *error_tasks]
        if str(item.get("issue_code") or "")
    ]
    reason = _build_foundation_reason(degraded_tasks=degraded_tasks, error_tasks=error_tasks)
    return degraded_tasks, error_tasks, issue_codes, reason


def run_live_data_foundation(
    cfg: DataConfig,
    *,
    gamma_client: GammaEventsClient | None = None,
    binance_client: BinanceSpotKlinesClient | None = None,
    oracle_client: PolymarketOracleApiClient | None = None,
    orderbook_client: PolymarketClobClient | None = None,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 1.0,
    market_catalog_refresh_sec: float = 300.0,
    binance_refresh_sec: float = 30.0,
    oracle_refresh_sec: float = 60.0,
    streams_refresh_sec: float = 60.0,
    orderbook_refresh_sec: float = 0.35,
    market_catalog_lookback_hours: int = 24,
    market_catalog_lookahead_hours: int = 24,
    binance_lookback_minutes: int = 2880,
    binance_batch_limit: int = 1000,
    oracle_lookback_days: int = 2,
    oracle_lookahead_hours: int = 24,
    include_direct_oracle: bool = True,
    include_streams: bool = True,
    include_orderbooks: bool = True,
    now_provider: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    if cfg.surface != "live":
        raise ValueError("live foundation runtime currently requires surface=live.")
    if cfg.cycle != "15m":
        raise ValueError("live foundation runtime currently requires cycle=15m.")

    iterations = max(1, int(iterations))
    sleep_sec = max(0.0, float(sleep_sec))
    run_started_at = _utc_now_iso(now_provider)
    completed = 0
    errors = 0
    last_results: dict[str, Any] = {}
    last_task_results: dict[str, Any] = {}
    next_due_at = {
        "market_catalog": 0.0,
        "binance": 0.0,
        "streams": 0.0,
        "oracle": 0.0,
        "orderbooks": 0.0,
    }

    _write_state(
        cfg,
        {
            "status": "running",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "run_started_at": run_started_at,
            "completed_iterations": 0,
            "errors": 0,
            "last_results": {},
            "last_error": None,
        },
    )

    while True:
        if completed >= iterations:
            break

        now = _utc_now(now_provider)
        now_ts = now.timestamp()
        completed += 1
        iteration_no = completed
        iteration_payload: dict[str, Any] = {
            "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "event": "foundation_iteration",
            "iteration": iteration_no,
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "tasks": {},
        }

        task_specs = [
            (
                "market_catalog",
                float(market_catalog_refresh_sec),
                lambda: _run_market_catalog_step(
                    cfg,
                    now=now,
                    client=gamma_client,
                    lookback_hours=market_catalog_lookback_hours,
                    lookahead_hours=market_catalog_lookahead_hours,
                ),
            ),
            (
                "binance",
                float(binance_refresh_sec),
                lambda: _run_binance_step(
                    cfg,
                    now=now,
                    client=binance_client,
                    lookback_minutes=binance_lookback_minutes,
                    batch_limit=binance_batch_limit,
                ),
            ),
        ]
        if include_streams:
            task_specs.append(
                (
                    "streams",
                    float(streams_refresh_sec),
                    lambda: _run_streams_step(
                        cfg,
                        now=now,
                        lookback_days=oracle_lookback_days,
                        lookahead_hours=oracle_lookahead_hours,
                    ),
                )
            )
        if include_direct_oracle:
            task_specs.append(
                (
                    "oracle",
                    float(oracle_refresh_sec),
                    lambda: _run_oracle_step(
                        cfg,
                        now=now,
                        client=oracle_client,
                        lookback_days=oracle_lookback_days,
                        lookahead_hours=oracle_lookahead_hours,
                    ),
                )
            )
        if include_orderbooks:
            task_specs.append(
                (
                    "orderbooks",
                    float(orderbook_refresh_sec),
                    lambda: _run_orderbook_step(cfg, client=orderbook_client),
                )
            )

        for task_name, interval_sec, task_fn in task_specs:
            if loop and now_ts < next_due_at[task_name]:
                iteration_payload["tasks"][task_name] = {"status": "skipped", "reason": "not_due"}
                continue
            try:
                summary = task_fn()
            except Exception as exc:
                errors += 1
                error_payload = {
                    "status": "error",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                if _task_can_fail_open(cfg, task_name=task_name):
                    error_payload["status"] = "degraded"
                    error_payload["fail_open"] = True
                    error_payload["fallback_path"] = str(cfg.layout.oracle_prices_table_path)
                    error_payload["issue_code"] = _classify_foundation_task_issue(
                        task_name=task_name,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        status="degraded",
                    )
                    iteration_payload["tasks"][task_name] = error_payload
                    last_task_results[task_name] = error_payload
                    _append_log(
                        cfg,
                        {
                            "ts": _utc_now_iso(now_provider),
                            "event": "foundation_task_degraded",
                            "iteration": iteration_no,
                            "task": task_name,
                            "market": cfg.asset.slug,
                            "cycle": cfg.cycle,
                            "surface": cfg.surface,
                            **error_payload,
                        },
                    )
                    continue
                iteration_payload["tasks"][task_name] = error_payload
                error_payload["issue_code"] = _classify_foundation_task_issue(
                    task_name=task_name,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    status="error",
                )
                last_task_results[task_name] = error_payload
                _append_log(
                    cfg,
                    {
                        "ts": _utc_now_iso(now_provider),
                        "event": "foundation_task_error",
                        "iteration": iteration_no,
                        "task": task_name,
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "surface": cfg.surface,
                        **error_payload,
                    },
                )
                degraded_tasks, error_tasks, issue_codes, reason = _summarize_task_results(last_task_results)
                error_state_ts = _utc_now_iso(now_provider)
                _write_state(
                    cfg,
                    {
                        "status": "running" if loop else "error",
                        "market": cfg.asset.slug,
                        "cycle": cfg.cycle,
                        "surface": cfg.surface,
                        "run_started_at": run_started_at,
                        "completed_iterations": completed,
                        "errors": errors,
                        "last_results": last_results,
                        "last_task_results": last_task_results,
                        "degraded_tasks": degraded_tasks,
                        "error_tasks": error_tasks,
                        "issue_codes": issue_codes,
                        "reason": reason,
                        "last_error": f"{task_name}: {type(exc).__name__}: {exc}",
                        "last_error_at": error_state_ts,
                        "finished_at": None if loop else error_state_ts,
                    },
                )
                if not loop:
                    raise
            else:
                last_results[task_name] = summary
                iteration_payload["tasks"][task_name] = {"status": "ok", "summary": summary}
                last_task_results[task_name] = iteration_payload["tasks"][task_name]
                next_due_at[task_name] = now_ts + max(0.0, interval_sec)

        _append_log(cfg, iteration_payload)
        _write_state(
            cfg,
            {
                "status": "running" if loop and completed < iterations else "ok",
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "surface": cfg.surface,
                "run_started_at": run_started_at,
                "completed_iterations": completed,
                "errors": errors,
                "last_results": last_results,
                "last_error": None if errors == 0 else None,
                "last_completed_at": _utc_now_iso(now_provider),
            },
        )

        if loop and completed < iterations and sleep_sec > 0:
            time.sleep(sleep_sec)

    _append_log(
        cfg,
        {
            "ts": _utc_now_iso(now_provider),
            "event": "foundation_finished",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "completed_iterations": completed,
            "errors": errors,
        },
    )
    finished_at = _utc_now_iso(now_provider)
    degraded_tasks, error_tasks, issue_codes, reason = _summarize_task_results(last_task_results)
    _write_state(
        cfg,
        {
            "status": "ok" if errors == 0 else "ok_with_errors",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "run_started_at": run_started_at,
            "completed_iterations": completed,
            "errors": errors,
            "last_results": last_results,
            "last_task_results": last_task_results,
            "degraded_tasks": degraded_tasks,
            "error_tasks": error_tasks,
            "issue_codes": issue_codes,
            "reason": reason,
            "last_error": None,
            "last_completed_at": finished_at,
            "finished_at": finished_at,
        },
    )
    return {
        "status": "ok" if errors == 0 else "ok_with_errors",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "run_started_at": run_started_at,
        "completed_iterations": completed,
        "errors": errors,
        "degraded_tasks": degraded_tasks,
        "error_tasks": error_tasks,
        "issue_codes": issue_codes,
        "reason": reason,
        "last_completed_at": finished_at,
        "finished_at": finished_at,
        "state_path": str(cfg.layout.foundation_state_path),
        "log_path": str(cfg.layout.foundation_log_path),
        "last_results": last_results,
        "last_task_results": last_task_results,
    }
