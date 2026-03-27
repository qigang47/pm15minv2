from __future__ import annotations

import inspect
import time
from dataclasses import dataclass
from typing import Any, Callable

from ..config import DataConfig
from ..sources.orderbook_provider import build_orderbook_provider_from_env
from .orderbook_runtime import run_orderbook_recorder


DEFAULT_ORDERBOOK_FLEET_MARKETS = ("btc", "eth", "sol", "xrp")


@dataclass
class _FleetWorkerResult:
    market: str
    payload: dict[str, Any] | None = None
    error: str | None = None


def parse_orderbook_fleet_markets(markets: str | list[str] | tuple[str, ...] | None) -> list[str]:
    if markets is None:
        return list(DEFAULT_ORDERBOOK_FLEET_MARKETS)
    if isinstance(markets, str):
        raw_values = markets.split(",")
    else:
        raw_values = list(markets)
    out: list[str] = []
    for raw in raw_values:
        token = str(raw or "").strip().lower()
        if token not in {"btc", "eth", "sol", "xrp"}:
            continue
        if token not in out:
            out.append(token)
    return out or list(DEFAULT_ORDERBOOK_FLEET_MARKETS)


def run_orderbook_recorder_fleet(
    *,
    markets: str | list[str] | tuple[str, ...] | None = None,
    cycle: str | int = "15m",
    surface: str = "live",
    poll_interval_sec: float = 0.35,
    orderbook_timeout_sec: float = 1.2,
    recent_window_minutes: int = 15,
    market_depth: int = 1,
    market_start_offset: int = 0,
    iterations: int = 1,
    loop: bool = False,
    sleep_sec: float | None = None,
    root=None,
    run_orderbook_recorder_fn: Callable[..., dict[str, Any]] = run_orderbook_recorder,
) -> dict[str, Any]:
    market_list = parse_orderbook_fleet_markets(markets)
    results: dict[str, _FleetWorkerResult] = {
        market: _FleetWorkerResult(market=market) for market in market_list
    }
    cfgs = {
        market: DataConfig.build(
            market=market,
            cycle=cycle,
            surface=surface,
            poll_interval_sec=poll_interval_sec,
            orderbook_timeout_sec=orderbook_timeout_sec,
            recent_window_minutes=recent_window_minutes,
            market_depth=market_depth,
            market_start_offset=market_start_offset,
            root=root,
        )
        for market in market_list
    }
    recorder_signature = inspect.signature(run_orderbook_recorder_fn)
    supports_provider = "provider" in recorder_signature.parameters
    providers = {
        market: build_orderbook_provider_from_env(
            source_name=f"v2-recorder:{cfg.asset.slug}:{cfg.cycle}",
            subscribe_on_read=False,
        )
        for market, cfg in cfgs.items()
    } if supports_provider else {}

    iteration_limit: int | None
    if loop and int(iterations) <= 0:
        iteration_limit = None
    else:
        iteration_limit = max(1, int(iterations))
    sleep_sec_resolved = max(0.0, float(poll_interval_sec if sleep_sec is None else sleep_sec))
    completed_rounds = 0

    while True:
        if iteration_limit is not None and completed_rounds >= iteration_limit:
            break
        completed_rounds += 1
        for market in market_list:
            try:
                call_kwargs = {
                    "iterations": 1,
                    "loop": False,
                    "sleep_sec": 0.0,
                }
                if supports_provider:
                    call_kwargs["provider"] = providers[market]
                payload = run_orderbook_recorder_fn(
                    cfgs[market],
                    **call_kwargs,
                )
                results[market].payload = payload
                results[market].error = None
            except Exception as exc:
                results[market].error = f"{type(exc).__name__}: {exc}"
        if not loop:
            break
        if iteration_limit is not None and completed_rounds >= iteration_limit:
            break
        if sleep_sec_resolved > 0:
            time.sleep(sleep_sec_resolved)

    payloads = {market: row.payload for market, row in results.items() if row.payload is not None}
    errors = {market: row.error for market, row in results.items() if row.error is not None}
    status = "ok"
    if errors and payloads:
        status = "ok_with_errors"
    elif errors:
        status = "error"

    return {
        "domain": "data",
        "dataset": "orderbook_recorder_fleet",
        "status": status,
        "cycle": str(cycle),
        "surface": str(surface),
        "markets": market_list,
        "poll_interval_sec": float(poll_interval_sec),
        "recent_window_minutes": int(recent_window_minutes),
        "market_depth": int(market_depth),
        "market_start_offset": int(market_start_offset),
        "iterations": int(iterations),
        "loop": bool(loop),
        "scheduler_mode": "round_robin",
        "completed_rounds": int(completed_rounds),
        "results": payloads,
        "errors": errors,
    }
