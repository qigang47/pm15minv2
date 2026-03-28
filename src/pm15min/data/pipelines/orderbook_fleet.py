from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import inspect
import os
from pathlib import Path
import subprocess
import sys
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


def _resolve_scheduler_mode(scheduler_mode: str | None) -> str:
    raw = str(
        scheduler_mode
        or os.getenv("PM15MIN_ORDERBOOK_FLEET_SCHEDULER_MODE")
        or "parallel_per_market"
    ).strip().lower()
    aliases = {
        "parallel_per_market": "parallel_per_market",
        "thread_per_market": "parallel_per_market",
        "threads": "parallel_per_market",
        "process_per_market": "process_per_market",
        "independent_processes": "process_per_market",
        "processes": "process_per_market",
    }
    return aliases.get(raw, "parallel_per_market")


def _project_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _process_recorder_command(
    *,
    market: str,
    cycle: str | int,
    surface: str,
    poll_interval_sec: float,
    orderbook_timeout_sec: float,
    recent_window_minutes: int,
    market_depth: int,
    market_start_offset: int,
    iterations: int,
    loop: bool,
    sleep_sec: float,
    root,
) -> list[str]:
    root_arg = "" if root is None else f", root={str(root)!r}"
    code = (
        "from pm15min.data.config import DataConfig; "
        "from pm15min.data.pipelines.orderbook_runtime import run_orderbook_recorder; "
        f"cfg = DataConfig.build(market={market!r}, cycle={str(cycle)!r}, surface={surface!r}, "
        f"poll_interval_sec={float(poll_interval_sec)!r}, orderbook_timeout_sec={float(orderbook_timeout_sec)!r}, "
        f"recent_window_minutes={int(recent_window_minutes)!r}, market_depth={int(market_depth)!r}, "
        f"market_start_offset={int(market_start_offset)!r}{root_arg}); "
        f"run_orderbook_recorder(cfg, iterations={int(iterations)!r}, loop={bool(loop)!r}, sleep_sec={float(sleep_sec)!r})"
    )
    return [str(sys.executable or "python3"), "-c", code]


def _launch_process_per_market_recorders(
    *,
    market_list: list[str],
    cycle: str | int,
    surface: str,
    poll_interval_sec: float,
    orderbook_timeout_sec: float,
    recent_window_minutes: int,
    market_depth: int,
    market_start_offset: int,
    iterations: int,
    loop: bool,
    sleep_sec: float,
    root,
) -> dict[str, int]:
    project_root = _project_root()
    env = os.environ.copy()
    pythonpath_parts = [str(project_root / "src"), str(project_root)]
    existing_pythonpath = str(env.get("PYTHONPATH") or "").strip()
    if existing_pythonpath:
        pythonpath_parts.append(existing_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath_parts)

    pids: dict[str, int] = {}
    for market in market_list:
        proc = subprocess.Popen(
            _process_recorder_command(
                market=market,
                cycle=cycle,
                surface=surface,
                poll_interval_sec=poll_interval_sec,
                orderbook_timeout_sec=orderbook_timeout_sec,
                recent_window_minutes=recent_window_minutes,
                market_depth=market_depth,
                market_start_offset=market_start_offset,
                iterations=iterations,
                loop=loop,
                sleep_sec=sleep_sec,
                root=root,
            ),
            cwd=str(project_root),
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        pids[market] = int(proc.pid)
    return pids


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
    scheduler_mode: str | None = None,
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
    iteration_limit = 0 if loop and int(iterations) <= 0 else max(1, int(iterations))
    sleep_sec_resolved = max(0.0, float(poll_interval_sec if sleep_sec is None else sleep_sec))
    completed_rounds = iteration_limit
    resolved_scheduler_mode = _resolve_scheduler_mode(scheduler_mode)

    if resolved_scheduler_mode == "process_per_market":
        if run_orderbook_recorder_fn is not run_orderbook_recorder:
            raise ValueError("process_per_market scheduler requires the default run_orderbook_recorder implementation.")
        if not loop or int(iterations) > 0:
            raise ValueError("process_per_market scheduler currently requires loop=True and iterations=0.")
        pids = _launch_process_per_market_recorders(
            market_list=market_list,
            cycle=cycle,
            surface=surface,
            poll_interval_sec=poll_interval_sec,
            orderbook_timeout_sec=orderbook_timeout_sec,
            recent_window_minutes=recent_window_minutes,
            market_depth=market_depth,
            market_start_offset=market_start_offset,
            iterations=iteration_limit,
            loop=loop,
            sleep_sec=sleep_sec_resolved,
            root=root,
        )
        return {
            "domain": "data",
            "dataset": "orderbook_recorder_fleet",
            "status": "ok",
            "cycle": str(cycle),
            "surface": str(surface),
            "markets": market_list,
            "poll_interval_sec": float(poll_interval_sec),
            "recent_window_minutes": int(recent_window_minutes),
            "market_depth": int(market_depth),
            "market_start_offset": int(market_start_offset),
            "iterations": int(iterations),
            "loop": bool(loop),
            "scheduler_mode": resolved_scheduler_mode,
            "completed_rounds": 0,
            "results": {},
            "errors": {},
            "pids": pids,
        }

    providers = {
        market: build_orderbook_provider_from_env(
            source_name=f"v2-recorder:{cfg.asset.slug}:{cfg.cycle}",
            subscribe_on_read=False,
        )
        for market, cfg in cfgs.items()
    } if supports_provider else {}

    def _run_market(market: str) -> dict[str, Any]:
        call_kwargs = {
            "iterations": iteration_limit,
            "loop": bool(loop),
            "sleep_sec": sleep_sec_resolved,
        }
        if supports_provider:
            call_kwargs["provider"] = providers[market]
        return run_orderbook_recorder_fn(
            cfgs[market],
            **call_kwargs,
        )

    with ThreadPoolExecutor(max_workers=len(market_list)) as executor:
        future_to_market = {executor.submit(_run_market, market): market for market in market_list}
        for future in as_completed(future_to_market):
            market = future_to_market[future]
            try:
                results[market].payload = future.result()
                results[market].error = None
            except Exception as exc:
                results[market].error = f"{type(exc).__name__}: {exc}"

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
        "scheduler_mode": resolved_scheduler_mode,
        "completed_rounds": int(completed_rounds),
        "results": payloads,
        "errors": errors,
    }
