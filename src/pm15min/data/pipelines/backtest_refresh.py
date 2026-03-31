from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.pipelines.binance_klines import sync_binance_klines_1m
from pm15min.data.pipelines.direct_oracle_prices import sync_polymarket_oracle_prices_direct
from pm15min.data.pipelines.direct_sync import sync_settlement_truth_from_gamma, sync_settlement_truth_from_rpc, sync_streams_from_rpc
from pm15min.data.pipelines.market_catalog import backfill_market_catalog_from_closed_markets, sync_market_catalog
from pm15min.data.pipelines.truth import build_truth_15m, build_truth_table
from pm15min.data.pipelines.oracle_prices import build_oracle_prices_15m


BacktestRefreshStep = Callable[[DataConfig], dict[str, Any]]
LabelFrameRebuildFn = Callable[[DataConfig], dict[str, object]]


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


def backfill_cycle_labels_from_gamma(
    *,
    markets: list[str],
    cycle: str,
    root: Path | None = None,
    surface: str = "backtest",
    start_ts: int | None = None,
    end_ts: int | None = None,
    window_days: int = 7,
    workers: int = 4,
    refresh_market_catalog: bool = True,
    skip_freshness: bool = True,
    rebuild_label_frame_fn: LabelFrameRebuildFn | None = None,
) -> dict[str, object]:
    cfgs = [
        DataConfig.build(
            market=market,
            cycle=cycle,
            surface=surface,
            root=root,
        )
        for market in markets
    ]
    if not cfgs:
        return {
            "dataset": "backfill_cycle_labels_gamma",
            "markets": [],
            "cycle": cycle,
            "surface": surface,
            "results": [],
        }

    if start_ts is None:
        starts: list[int] = []
        for cfg in cfgs:
            if cfg.layout.market_catalog_table_path.exists():
                df = pd.read_parquet(cfg.layout.market_catalog_table_path, columns=["cycle_start_ts"])
                series = pd.to_numeric(df["cycle_start_ts"], errors="coerce").dropna()
                if not series.empty:
                    starts.append(int(series.min()))
        if not starts:
            raise FileNotFoundError("Cannot infer start_ts because market catalog tables are missing. Pass --start-date.")
        start_ts = min(starts)

    if end_ts is None:
        ends: list[int] = []
        for cfg in cfgs:
            if cfg.layout.market_catalog_table_path.exists():
                df = pd.read_parquet(cfg.layout.market_catalog_table_path, columns=["cycle_end_ts"])
                series = pd.to_numeric(df["cycle_end_ts"], errors="coerce").dropna()
                if not series.empty:
                    ends.append(int(series.max()))
        end_ts = max(ends) if ends else int(datetime.now(timezone.utc).timestamp())

    market_catalog_results: list[dict[str, object]] = []
    if refresh_market_catalog:
        for cfg in cfgs:
            market_catalog_results.append(
                backfill_market_catalog_from_closed_markets(
                    cfg,
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                    window_days=max(1, int(window_days)),
                )
            )

    results: list[dict[str, object]] = []
    for cfg in cfgs:
        settlement = sync_settlement_truth_from_gamma(
            cfg,
            start_ts=int(start_ts),
            end_ts=int(end_ts),
            workers=max(1, int(workers)),
        )
        truth = build_truth_table(cfg)
        label = _resolve_label_frame_rebuild_summary(
            cfg,
            rebuild_label_frame_fn=rebuild_label_frame_fn,
            skip_freshness=skip_freshness,
        )
        truth_df = pd.read_parquet(cfg.layout.truth_table_path, columns=["cycle_start_ts", "resolved", "full_truth"])
        truth_df = truth_df[
            (pd.to_numeric(truth_df["cycle_start_ts"], errors="coerce") >= int(start_ts))
            & (pd.to_numeric(truth_df["cycle_start_ts"], errors="coerce") <= int(end_ts))
        ].copy()
        ts = pd.to_datetime(pd.to_numeric(truth_df["cycle_start_ts"], errors="coerce"), unit="s", utc=True, errors="coerce").dropna()
        results.append(
            {
                "market": cfg.asset.slug,
                "cycle": cfg.cycle,
                "settlement": settlement,
                "truth": truth,
                "label": label,
                "truth_rows_in_range": int(len(truth_df)),
                "resolved_in_range": int(truth_df["resolved"].fillna(False).sum()),
                "full_truth_in_range": int(truth_df["full_truth"].fillna(False).sum()),
                "first_in_range": str(ts.min()) if not ts.empty else None,
                "last_in_range": str(ts.max()) if not ts.empty else None,
            }
        )

    return {
        "dataset": "backfill_cycle_labels_gamma",
        "markets": [cfg.asset.slug for cfg in cfgs],
        "cycle": cycle,
        "surface": surface,
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "window_days": int(window_days),
        "workers": int(workers),
        "refresh_market_catalog": bool(refresh_market_catalog),
        "market_catalog_results": market_catalog_results,
        "results": results,
    }


def _resolve_label_frame_rebuild_summary(
    cfg: DataConfig,
    *,
    rebuild_label_frame_fn: LabelFrameRebuildFn | None,
    skip_freshness: bool,
) -> dict[str, object]:
    if rebuild_label_frame_fn is None:
        return {
            "status": "skipped",
            "reason": "research_label_frame_rebuild_moved_out_of_data_domain",
            "market": cfg.asset.slug,
            "cycle": cfg.cycle,
            "surface": cfg.surface,
            "skip_freshness_requested": bool(skip_freshness),
        }
    return rebuild_label_frame_fn(cfg)
