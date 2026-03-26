from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm15min.data.pipelines.direct_oracle_prices import sync_polymarket_oracle_prices_direct
from pm15min.data.pipelines.direct_sync import sync_settlement_truth_from_rpc, sync_streams_from_rpc
from pm15min.data.pipelines.market_catalog import sync_market_catalog
from pm15min.data.pipelines.truth import build_truth_15m
from pm15min.data.pipelines.oracle_prices import build_oracle_prices_15m


BacktestRefreshStep = Callable[[DataConfig], dict[str, Any]]


@dataclass(frozen=True)
class BacktestRefreshOptions:
    cycle: str = "15m"
    lookback_days: int = 45
    market_catalog_lookahead_hours: int = 24
    binance_lookback_minutes: int = 90 * 24 * 60
    binance_batch_limit: int = 1000
    oracle_lookback_days: int = 45
    oracle_lookahead_hours: int = 24
    settlement_chunk_blocks: int = 3000
    settlement_sleep_sec: float = 0.01
    streams_chunk_blocks: int = 1000
    streams_sleep_sec: float = 0.02
    include_streams: bool = True
    include_direct_oracle: bool = True
    include_settlement_truth_rpc: bool = True


def run_backtest_data_refresh(
    *,
    markets: list[str],
    root: Path | None = None,
    options: BacktestRefreshOptions | None = None,
) -> dict[str, object]:
    opts = options or BacktestRefreshOptions()
    now = datetime.now(timezone.utc)
    start_ts = int((now - timedelta(days=max(1, int(opts.lookback_days)))).timestamp())
    end_ts = int((now + timedelta(hours=max(1, int(opts.market_catalog_lookahead_hours)))).timestamp())

    results: list[dict[str, object]] = []
    for market in markets:
        cfg = DataConfig.build(
            market=market,
            cycle=opts.cycle,
            surface="backtest",
            root=root,
        )
        market_result: dict[str, object] = {
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "steps": {},
        }

        market_result["steps"]["market_catalog"] = sync_market_catalog(
            cfg,
            start_ts=start_ts,
            end_ts=end_ts,
        )
        market_result["steps"]["binance_klines_1m"] = sync_binance_klines_1m(
            cfg,
            lookback_minutes=max(60, int(opts.binance_lookback_minutes)),
            batch_limit=max(100, int(opts.binance_batch_limit)),
        )
        if opts.include_streams:
            market_result["steps"]["streams_rpc"] = sync_streams_from_rpc(
                cfg,
                start_ts=start_ts,
                end_ts=end_ts,
                chunk_blocks=max(100, int(opts.streams_chunk_blocks)),
                sleep_sec=max(0.0, float(opts.streams_sleep_sec)),
            )
        if opts.include_direct_oracle:
            market_result["steps"]["direct_oracle_prices"] = sync_polymarket_oracle_prices_direct(
                cfg,
                start_ts=start_ts,
                end_ts=int((now + timedelta(hours=max(1, int(opts.oracle_lookahead_hours)))).timestamp()),
                lookback_days=max(1, int(opts.oracle_lookback_days)),
            )
        if opts.include_settlement_truth_rpc:
            market_result["steps"]["settlement_truth_rpc"] = sync_settlement_truth_from_rpc(
                cfg,
                start_ts=start_ts,
                end_ts=end_ts,
                chunk_blocks=max(100, int(opts.settlement_chunk_blocks)),
                sleep_sec=max(0.0, float(opts.settlement_sleep_sec)),
            )
        market_result["steps"]["oracle_prices_15m"] = build_oracle_prices_15m(cfg)
        market_result["steps"]["truth_15m"] = build_truth_15m(cfg)
        results.append(market_result)

    return {
        "dataset": "backtest_data_refresh",
        "markets": [str(market) for market in markets],
        "cycle": opts.cycle,
        "surface": "backtest",
        "lookback_days": int(opts.lookback_days),
        "generated_at": now.isoformat(),
        "results": results,
    }
