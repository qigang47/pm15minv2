from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import pandas as pd

from pmshared.io.json_files import append_jsonl, write_json_atomic

from ..config import DataConfig
from .binance_klines import sync_binance_klines_1m
from .direct_sync import sync_streams_from_rpc
from ..queries.loaders import load_direct_oracle_source
from .direct_oracle_prices import sync_polymarket_oracle_price_window
from .market_catalog import sync_market_catalog
from .oracle_prices import build_oracle_prices_15m
from .orderbook_runtime import run_orderbook_recorder
from ..sources.binance_spot import BinanceSpotKlinesClient
from ..sources.polymarket_clob import PolymarketClobClient
from ..sources.polymarket_gamma import GammaEventsClient
from ..sources.polymarket_oracle_api import PolymarketOracleApiClient

_CURRENT_CYCLE_DIRECT_ORACLE_LAST_ATTEMPT: dict[tuple[str, int], float] = {}
_SHARED_FOUNDATION_TASK_PRIORITY = ("binance", "oracle", "streams", "market_catalog", "orderbooks")


def _default_entry_offsets(*, cycle_minutes: int) -> tuple[int, ...]:
    if int(cycle_minutes) == 5:
        return (2, 3, 4)
    return (7, 8, 9)


def _utc_now(now_provider: Callable[[], datetime] | None = None) -> datetime:
    now = now_provider() if now_provider is not None else datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _utc_now_iso(now_provider: Callable[[], datetime] | None = None) -> str:
    return _utc_now(now_provider).strftime("%Y-%m-%dT%H:%M:%SZ")


def _env_float(name: str, *, default: float) -> float:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int_list(name: str, *, default: tuple[int, ...] = ()) -> tuple[int, ...]:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return tuple(default)
    values: list[int] = []
    for token in raw.split(","):
        text = token.strip()
        if not text:
            continue
        try:
            values.append(int(text))
        except Exception:
            continue
    if not values:
        return tuple(default)
    return tuple(sorted(set(values)))


def _write_state(cfg: DataConfig, payload: dict[str, Any]) -> None:
    write_json_atomic(payload, cfg.layout.foundation_state_path)


def _append_log(cfg: DataConfig, payload: dict[str, Any]) -> None:
    append_jsonl(cfg.layout.foundation_log_path, payload)


def _binance_markets_for_foundation(cfg: DataConfig) -> list[str]:
    # Keep foundation focused on the market's primary 1m feed. Cross-asset BTC
    # klines are refreshed on demand in the live signal path when actually needed.
    return [cfg.asset.slug]


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
    min_retry_seconds: float,
) -> dict[str, object]:
    cycle_seconds = int(cfg.layout.cycle_seconds)
    current_cycle_start_ts = int(now.timestamp()) // cycle_seconds * cycle_seconds
    direct_summary = _sync_live_current_cycle_oracle_open_price(
        cfg,
        cycle_start_ts=current_cycle_start_ts,
        client=client,
        min_retry_seconds=min_retry_seconds,
    )
    table_summary = build_oracle_prices_15m(cfg)
    return {
        "dataset": "foundation_oracle_prices",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "direct_summary": direct_summary,
        "table_summary": table_summary,
    }


def _sync_live_current_cycle_oracle_open_price(
    cfg: DataConfig,
    *,
    cycle_start_ts: int,
    client: PolymarketOracleApiClient | None,
    min_retry_seconds: float,
) -> dict[str, object]:
    existing = load_direct_oracle_source(cfg)
    if _has_direct_open_price(existing, cycle_start_ts=cycle_start_ts):
        return {
            "dataset": "polymarket_direct_oracle_price_window",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "cycle_start_ts": int(cycle_start_ts),
            "rows_imported": 0,
            "canonical_rows": int(len(existing)),
            "target_path": str(cfg.layout.direct_oracle_source_path),
            "status": "cached",
            "reason": "current_cycle_open_price_reused",
        }

    retry_key = (str(cfg.layout.direct_oracle_source_path), int(cycle_start_ts))
    now_monotonic = time.monotonic()
    retry_window_seconds = max(60.0, float(min_retry_seconds))
    last_attempt = _CURRENT_CYCLE_DIRECT_ORACLE_LAST_ATTEMPT.get(retry_key, 0.0)
    if last_attempt > 0.0 and now_monotonic - last_attempt < retry_window_seconds:
        return {
            "dataset": "polymarket_direct_oracle_price_window",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "cycle_start_ts": int(cycle_start_ts),
            "rows_imported": 0,
            "canonical_rows": int(len(existing)),
            "target_path": str(cfg.layout.direct_oracle_source_path),
            "status": "deferred",
            "reason": "current_cycle_open_price_retry_deferred",
            "retry_in_seconds": max(0.0, retry_window_seconds - (now_monotonic - last_attempt)),
        }

    _CURRENT_CYCLE_DIRECT_ORACLE_LAST_ATTEMPT[retry_key] = now_monotonic
    summary = sync_polymarket_oracle_price_window(
        cfg,
        cycle_start_ts=int(cycle_start_ts),
        client=client,
        max_retries=1,
        sleep_sec=0.0,
    )
    rows_imported = int(summary.get("rows_imported") or 0)
    summary["status"] = "ok" if rows_imported > 0 else "missing"
    summary["reason"] = "current_cycle_open_price_fetched" if rows_imported > 0 else "current_cycle_open_price_missing"
    return summary


def _has_direct_open_price(frame: pd.DataFrame, *, cycle_start_ts: int) -> bool:
    if frame.empty:
        return False
    cycle_series = pd.to_numeric(frame.get("cycle_start_ts"), errors="coerce")
    price_series = pd.to_numeric(frame.get("price_to_beat"), errors="coerce")
    if cycle_series.empty or price_series.empty:
        return False
    mask = cycle_series.eq(int(cycle_start_ts)) & price_series.notna()
    return bool(mask.any())


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


def _binance_boundary_offsets(*, cycle_minutes: int) -> tuple[int, ...]:
    default_offsets = _default_entry_offsets(cycle_minutes=cycle_minutes)
    valid = tuple(
        offset
        for offset in _env_int_list(
            "PM5MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS",
            default=default_offsets,
        )
        if 0 <= int(offset) < max(1, int(cycle_minutes))
    )
    return valid


def _binance_boundary_initial_delay_sec() -> float:
    return _env_float("PM5MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC", default=0.0)


def _binance_boundary_retry_interval_sec() -> float:
    return _env_float("PM5MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC", default=0.2)


def _binance_boundary_retry_window_sec() -> float:
    return _env_float("PM5MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC", default=1.5)


def _iter_binance_boundary_targets(
    *,
    now: datetime,
    cycle_minutes: int,
    boundary_offsets: tuple[int, ...],
    boundary_delay_sec: float,
) -> list[dict[str, float]]:
    if int(cycle_minutes) <= 0 or not boundary_offsets:
        return []
    cycle_seconds = int(cycle_minutes) * 60
    now_ts = float(now.timestamp())
    cycle_start_ts = int(now_ts // cycle_seconds) * cycle_seconds
    targets: list[dict[str, float]] = []
    for cycle_shift in (-1, 0, 1):
        base_ts = float(cycle_start_ts + cycle_shift * cycle_seconds)
        for offset in boundary_offsets:
            closed_bar_open_ts = base_ts + float(offset - 1) * 60.0
            due_ts = closed_bar_open_ts + 60.0 + max(0.0, float(boundary_delay_sec))
            targets.append(
                {
                    "offset": float(offset),
                    "expected_open_ts": closed_bar_open_ts,
                    "due_ts": due_ts,
                }
            )
    targets.sort(key=lambda row: float(row["due_ts"]))
    return targets


def _active_binance_boundary_target(
    *,
    now: datetime,
    cycle_minutes: int,
    boundary_offsets: tuple[int, ...],
    boundary_delay_sec: float,
    retry_window_sec: float,
) -> dict[str, float] | None:
    now_ts = float(now.timestamp())
    active = [
        row
        for row in _iter_binance_boundary_targets(
            now=now,
            cycle_minutes=cycle_minutes,
            boundary_offsets=boundary_offsets,
            boundary_delay_sec=boundary_delay_sec,
        )
        if float(row["due_ts"]) <= now_ts < float(row["due_ts"]) + max(0.0, float(retry_window_sec))
    ]
    if not active:
        return None
    return max(active, key=lambda row: float(row["due_ts"]))


def _next_binance_boundary_due_ts(
    *,
    now: datetime,
    cycle_minutes: int,
    boundary_offsets: tuple[int, ...],
    boundary_delay_sec: float,
) -> float | None:
    now_ts = float(now.timestamp())
    candidates: list[float] = []
    for row in _iter_binance_boundary_targets(
        now=now,
        cycle_minutes=cycle_minutes,
        boundary_offsets=boundary_offsets,
        boundary_delay_sec=boundary_delay_sec,
    ):
        candidate = float(row["due_ts"])
        if candidate > now_ts:
            candidates.append(candidate)
    if not candidates:
        return None
    return min(candidates)


def _binance_summary_has_expected_closed_bar(*, summary: dict[str, object] | None, target: dict[str, float]) -> bool:
    if not isinstance(summary, dict):
        return False
    raw_latest_open_time = summary.get("latest_open_time")
    if raw_latest_open_time in (None, ""):
        return False
    try:
        latest_open_time = pd.Timestamp(raw_latest_open_time)
    except Exception:
        return False
    if latest_open_time.tzinfo is None:
        latest_open_time = latest_open_time.tz_localize("UTC")
    else:
        latest_open_time = latest_open_time.tz_convert("UTC")
    expected_open_ts = float(target.get("expected_open_ts") or 0.0)
    return float(latest_open_time.timestamp()) >= expected_open_ts


def _next_foundation_task_due_at(
    *,
    cfg: DataConfig,
    task_name: str,
    now: datetime,
    interval_sec: float,
    last_summary: dict[str, object] | None = None,
) -> float:
    if str(task_name) != "binance":
        return float(now.timestamp()) + max(0.0, float(interval_sec))
    cycle_minutes = int(cfg.layout.cycle_seconds // 60)
    boundary_offsets = _binance_boundary_offsets(cycle_minutes=cycle_minutes)
    boundary_delay_sec = _binance_boundary_initial_delay_sec()
    retry_window_sec = _binance_boundary_retry_window_sec()
    retry_interval_sec = _binance_boundary_retry_interval_sec()
    active_target = _active_binance_boundary_target(
        now=now,
        cycle_minutes=cycle_minutes,
        boundary_offsets=boundary_offsets,
        boundary_delay_sec=boundary_delay_sec,
        retry_window_sec=retry_window_sec,
    )
    boundary_due_ts = _next_binance_boundary_due_ts(
        now=now,
        cycle_minutes=cycle_minutes,
        boundary_offsets=boundary_offsets,
        boundary_delay_sec=boundary_delay_sec,
    )
    fallback_refresh_sec = _env_float(
        "PM5MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC",
        default=max(0.0, float(interval_sec)),
    )
    now_ts = float(now.timestamp())
    fallback_due_ts = float(now.timestamp()) + max(0.0, float(fallback_refresh_sec))
    if (
        active_target is not None
        and last_summary is not None
        and not _binance_summary_has_expected_closed_bar(summary=last_summary, target=active_target)
    ):
        retry_deadline_ts = float(active_target["due_ts"]) + max(0.0, float(retry_window_sec))
        if retry_interval_sec > 0.0 and now_ts < retry_deadline_ts:
            return min(fallback_due_ts, now_ts + max(0.0, float(retry_interval_sec)), retry_deadline_ts)
    if boundary_due_ts is None:
        return fallback_due_ts
    if fallback_refresh_sec <= 0.0:
        return fallback_due_ts
    return min(boundary_due_ts, fallback_due_ts)


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


def _build_foundation_task_specs(
    cfg: DataConfig,
    *,
    now: datetime,
    now_provider: Callable[[], datetime] | None,
    gamma_client: GammaEventsClient | None,
    binance_client: BinanceSpotKlinesClient | None,
    oracle_client: PolymarketOracleApiClient | None,
    orderbook_client: PolymarketClobClient | None,
    market_catalog_refresh_sec: float,
    binance_refresh_sec: float,
    oracle_refresh_sec: float,
    streams_refresh_sec: float,
    orderbook_refresh_sec: float,
    market_catalog_lookback_hours: int,
    market_catalog_lookahead_hours: int,
    binance_lookback_minutes: int,
    binance_batch_limit: int,
    oracle_lookback_days: int,
    oracle_lookahead_hours: int,
    include_direct_oracle: bool,
    include_streams: bool,
    include_orderbooks: bool,
    critical_first: bool = False,
) -> list[tuple[str, float, Callable[[], dict[str, object]]]]:
    task_specs: list[tuple[str, float, Callable[[], dict[str, object]]]] = [
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
                # Use execution-time now so a just-closed boundary bar is not
                # filtered out when earlier tasks delay the Binance sync.
                now=_utc_now(now_provider),
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
                    min_retry_seconds=max(60.0, float(oracle_refresh_sec)),
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
    if critical_first:
        priority = {name: idx for idx, name in enumerate(_SHARED_FOUNDATION_TASK_PRIORITY)}
        task_specs.sort(key=lambda item: priority.get(item[0], len(priority)))
    return task_specs


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
    binance_refresh_sec: float = 60.0,
    oracle_refresh_sec: float = 60.0,
    streams_refresh_sec: float = 300.0,
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

    requested_iterations = int(iterations)
    run_forever = loop and requested_iterations <= 0
    iterations = max(1, requested_iterations)
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
        if not run_forever and completed >= iterations:
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

        task_specs = _build_foundation_task_specs(
            cfg,
            now=now,
            now_provider=now_provider,
            gamma_client=gamma_client,
            binance_client=binance_client,
            oracle_client=oracle_client,
            orderbook_client=orderbook_client,
            market_catalog_refresh_sec=market_catalog_refresh_sec,
            binance_refresh_sec=binance_refresh_sec,
            oracle_refresh_sec=oracle_refresh_sec,
            streams_refresh_sec=streams_refresh_sec,
            orderbook_refresh_sec=orderbook_refresh_sec,
            market_catalog_lookback_hours=market_catalog_lookback_hours,
            market_catalog_lookahead_hours=market_catalog_lookahead_hours,
            binance_lookback_minutes=binance_lookback_minutes,
            binance_batch_limit=binance_batch_limit,
            oracle_lookback_days=oracle_lookback_days,
            oracle_lookahead_hours=oracle_lookahead_hours,
            include_direct_oracle=include_direct_oracle,
            include_streams=include_streams,
            include_orderbooks=include_orderbooks,
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
                next_due_at[task_name] = _next_foundation_task_due_at(
                    cfg=cfg,
                    task_name=task_name,
                    now=now,
                    interval_sec=interval_sec,
                    last_summary=summary,
                )

        _append_log(cfg, iteration_payload)
        _write_state(
            cfg,
            {
                "status": "running" if loop and (run_forever or completed < iterations) else "ok",
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

        if loop and (run_forever or completed < iterations) and sleep_sec > 0:
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


def run_live_data_foundation_shared(
    cfgs: list[DataConfig],
    *,
    gamma_client: GammaEventsClient | None = None,
    binance_client: BinanceSpotKlinesClient | None = None,
    oracle_client: PolymarketOracleApiClient | None = None,
    orderbook_client: PolymarketClobClient | None = None,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float = 1.0,
    market_catalog_refresh_sec: float = 300.0,
    binance_refresh_sec: float = 60.0,
    oracle_refresh_sec: float = 60.0,
    streams_refresh_sec: float = 300.0,
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
    if not cfgs:
        raise ValueError("shared live foundation requires at least one market config.")
    for cfg in cfgs:
        if cfg.surface != "live":
            raise ValueError("shared live foundation currently requires surface=live.")

    requested_iterations = int(iterations)
    run_forever = loop and requested_iterations <= 0
    iterations = max(1, requested_iterations)
    sleep_sec = max(0.0, float(sleep_sec))
    run_started_at = _utc_now_iso(now_provider)
    markets = [cfg.asset.slug for cfg in cfgs]
    state_by_market: dict[str, dict[str, Any]] = {}

    for cfg in cfgs:
        slug = cfg.asset.slug
        state_by_market[slug] = {
            "completed": 0,
            "errors": 0,
            "last_results": {},
            "last_task_results": {},
            "next_due_at": {
                "market_catalog": 0.0,
                "binance": 0.0,
                "streams": 0.0,
                "oracle": 0.0,
                "orderbooks": 0.0,
            },
        }
        _write_state(
            cfg,
            {
                "status": "running",
                "mode": "shared",
                "shared_markets": markets,
                "market": slug,
                "cycle": cfg.cycle,
                "surface": cfg.surface,
                "run_started_at": run_started_at,
                "completed_iterations": 0,
                "errors": 0,
                "last_results": {},
                "last_error": None,
            },
        )

    shared_completed = 0
    while True:
        if not run_forever and shared_completed >= iterations:
            break

        now = _utc_now(now_provider)
        now_ts = now.timestamp()
        shared_completed += 1
        heartbeat_ts = _utc_now_iso(now_provider)
        per_market_payloads: dict[str, dict[str, Any]] = {
            cfg.asset.slug: {
                "ts": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "event": "foundation_iteration",
                "mode": "shared",
                "shared_iteration": shared_completed,
                "iteration": shared_completed,
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "surface": cfg.surface,
                "tasks": {},
            }
            for cfg in cfgs
        }

        market_specs: dict[str, dict[str, tuple[float, Callable[[], dict[str, object]]]]] = {}
        for cfg in cfgs:
            market_specs[cfg.asset.slug] = {
                task_name: (interval_sec, task_fn)
                for task_name, interval_sec, task_fn in _build_foundation_task_specs(
                    cfg,
                    now=now,
                    now_provider=now_provider,
                    gamma_client=gamma_client,
                    binance_client=binance_client,
                    oracle_client=oracle_client,
                    orderbook_client=orderbook_client,
                    market_catalog_refresh_sec=market_catalog_refresh_sec,
                    binance_refresh_sec=binance_refresh_sec,
                    oracle_refresh_sec=oracle_refresh_sec,
                    streams_refresh_sec=streams_refresh_sec,
                    orderbook_refresh_sec=orderbook_refresh_sec,
                    market_catalog_lookback_hours=market_catalog_lookback_hours,
                    market_catalog_lookahead_hours=market_catalog_lookahead_hours,
                    binance_lookback_minutes=binance_lookback_minutes,
                    binance_batch_limit=binance_batch_limit,
                    oracle_lookback_days=oracle_lookback_days,
                    oracle_lookahead_hours=oracle_lookahead_hours,
                    include_direct_oracle=include_direct_oracle,
                    include_streams=include_streams,
                    include_orderbooks=include_orderbooks,
                    critical_first=True,
                )
            }

        for task_name in _SHARED_FOUNDATION_TASK_PRIORITY:
            for cfg in cfgs:
                slug = cfg.asset.slug
                spec = market_specs[slug].get(task_name)
                if spec is None:
                    continue
                interval_sec, task_fn = spec
                market_state = state_by_market[slug]
                payload = per_market_payloads[slug]
                if loop and now_ts < float(market_state["next_due_at"].get(task_name, 0.0)):
                    payload["tasks"][task_name] = {"status": "skipped", "reason": "not_due"}
                    continue
                try:
                    summary = task_fn()
                except Exception as exc:
                    market_state["errors"] = int(market_state["errors"]) + 1
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
                        payload["tasks"][task_name] = error_payload
                        market_state["last_task_results"][task_name] = error_payload
                        _append_log(
                            cfg,
                            {
                                "ts": _utc_now_iso(now_provider),
                                "event": "foundation_task_degraded",
                                "mode": "shared",
                                "shared_markets": markets,
                                "shared_iteration": shared_completed,
                                "iteration": shared_completed,
                                "task": task_name,
                                "market": slug,
                                "cycle": cfg.cycle,
                                "surface": cfg.surface,
                                **error_payload,
                            },
                        )
                        continue
                    error_payload["issue_code"] = _classify_foundation_task_issue(
                        task_name=task_name,
                        error_type=type(exc).__name__,
                        error=str(exc),
                        status="error",
                    )
                    payload["tasks"][task_name] = error_payload
                    market_state["last_task_results"][task_name] = error_payload
                    _append_log(
                        cfg,
                        {
                            "ts": _utc_now_iso(now_provider),
                            "event": "foundation_task_error",
                            "mode": "shared",
                            "shared_markets": markets,
                            "shared_iteration": shared_completed,
                            "iteration": shared_completed,
                            "task": task_name,
                            "market": slug,
                            "cycle": cfg.cycle,
                            "surface": cfg.surface,
                            **error_payload,
                        },
                    )
                    degraded_tasks, error_tasks, issue_codes, reason = _summarize_task_results(market_state["last_task_results"])
                    error_state_ts = _utc_now_iso(now_provider)
                    _write_state(
                        cfg,
                        {
                            "status": "running" if loop else "error",
                            "mode": "shared",
                            "shared_markets": markets,
                            "market": slug,
                            "cycle": cfg.cycle,
                            "surface": cfg.surface,
                            "run_started_at": run_started_at,
                            "completed_iterations": shared_completed,
                            "errors": market_state["errors"],
                            "last_results": market_state["last_results"],
                            "last_task_results": market_state["last_task_results"],
                            "degraded_tasks": degraded_tasks,
                            "error_tasks": error_tasks,
                            "issue_codes": issue_codes,
                            "reason": reason,
                            "last_error": f"{task_name}: {type(exc).__name__}: {exc}",
                            "last_error_at": error_state_ts,
                            "last_completed_at": error_state_ts,
                            "finished_at": None if loop else error_state_ts,
                        },
                    )
                    if not loop:
                        raise
                else:
                    market_state["last_results"][task_name] = summary
                    payload["tasks"][task_name] = {"status": "ok", "summary": summary}
                    market_state["last_task_results"][task_name] = payload["tasks"][task_name]
                    market_state["next_due_at"][task_name] = _next_foundation_task_due_at(
                        cfg=cfg,
                        task_name=task_name,
                        now=now,
                        interval_sec=interval_sec,
                        last_summary=summary,
                    )

        state_ts = _utc_now_iso(now_provider)
        for cfg in cfgs:
            slug = cfg.asset.slug
            market_state = state_by_market[slug]
            market_state["completed"] = int(market_state["completed"]) + 1
            _append_log(cfg, per_market_payloads[slug])
            _write_state(
                cfg,
                {
                    "status": "running" if loop and (run_forever or shared_completed < iterations) else "ok",
                    "mode": "shared",
                    "shared_markets": markets,
                    "market": slug,
                    "cycle": cfg.cycle,
                    "surface": cfg.surface,
                    "run_started_at": run_started_at,
                    "completed_iterations": market_state["completed"],
                    "errors": market_state["errors"],
                    "last_results": market_state["last_results"],
                    "last_task_results": market_state["last_task_results"],
                    "last_error": None,
                    "last_heartbeat_at": heartbeat_ts,
                    "last_completed_at": state_ts,
                },
            )

        if loop and (run_forever or shared_completed < iterations) and sleep_sec > 0:
            time.sleep(sleep_sec)

    finished_at = _utc_now_iso(now_provider)
    market_summaries: dict[str, dict[str, object]] = {}
    shared_errors = 0
    for cfg in cfgs:
        slug = cfg.asset.slug
        market_state = state_by_market[slug]
        shared_errors += int(market_state["errors"])
        degraded_tasks, error_tasks, issue_codes, reason = _summarize_task_results(market_state["last_task_results"])
        _append_log(
            cfg,
            {
                "ts": finished_at,
                "event": "foundation_finished",
                "mode": "shared",
                "shared_markets": markets,
                "market": slug,
                "cycle": cfg.cycle,
                "surface": cfg.surface,
                "completed_iterations": market_state["completed"],
                "errors": market_state["errors"],
            },
        )
        _write_state(
            cfg,
            {
                "status": "ok" if market_state["errors"] == 0 else "ok_with_errors",
                "mode": "shared",
                "shared_markets": markets,
                "market": slug,
                "cycle": cfg.cycle,
                "surface": cfg.surface,
                "run_started_at": run_started_at,
                "completed_iterations": market_state["completed"],
                "errors": market_state["errors"],
                "last_results": market_state["last_results"],
                "last_task_results": market_state["last_task_results"],
                "degraded_tasks": degraded_tasks,
                "error_tasks": error_tasks,
                "issue_codes": issue_codes,
                "reason": reason,
                "last_error": None,
                "last_completed_at": finished_at,
                "finished_at": finished_at,
            },
        )
        market_summaries[slug] = {
            "status": "ok" if market_state["errors"] == 0 else "ok_with_errors",
            "completed_iterations": market_state["completed"],
            "errors": market_state["errors"],
            "degraded_tasks": degraded_tasks,
            "error_tasks": error_tasks,
            "issue_codes": issue_codes,
            "reason": reason,
            "state_path": str(cfg.layout.foundation_state_path),
            "log_path": str(cfg.layout.foundation_log_path),
            "last_results": market_state["last_results"],
            "last_task_results": market_state["last_task_results"],
        }

    return {
        "status": "ok" if shared_errors == 0 else "ok_with_errors",
        "mode": "shared",
        "markets": markets,
        "cycle": cfgs[0].cycle,
        "surface": cfgs[0].surface,
        "run_started_at": run_started_at,
        "completed_iterations": shared_completed,
        "errors": shared_errors,
        "last_completed_at": finished_at,
        "finished_at": finished_at,
        "market_summaries": market_summaries,
    }
