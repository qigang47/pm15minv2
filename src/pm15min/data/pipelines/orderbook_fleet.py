from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Callable

from ..config import DataConfig
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

    def _worker(market: str) -> None:
        try:
            cfg = DataConfig.build(
                market=market,
                cycle=cycle,
                surface=surface,
                poll_interval_sec=poll_interval_sec,
                orderbook_timeout_sec=orderbook_timeout_sec,
                recent_window_minutes=recent_window_minutes,
                market_depth=market_depth,
                root=root,
            )
            payload = run_orderbook_recorder_fn(
                cfg,
                iterations=iterations,
                loop=loop,
                sleep_sec=sleep_sec,
            )
            results[market].payload = payload
        except Exception as exc:
            results[market].error = f"{type(exc).__name__}: {exc}"

    threads = [
        threading.Thread(target=_worker, args=(market,), daemon=False, name=f"orderbook-fleet-{market}")
        for market in market_list
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

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
        "iterations": int(iterations),
        "loop": bool(loop),
        "results": payloads,
        "errors": errors,
    }
