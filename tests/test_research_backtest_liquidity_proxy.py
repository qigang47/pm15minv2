from __future__ import annotations

import pandas as pd
import pytest

from pm15min.research.backtests.liquidity_proxy import (
    BacktestLiquidityProxyConfig,
    BacktestLiquidityThresholds,
    build_backtest_liquidity_proxy,
    build_spot_kline_mirror_metrics,
)


def _sample_klines(*, start: str, quote_values: list[float], trade_values: list[float]) -> pd.DataFrame:
    ts = pd.date_range(start, periods=len(quote_values), freq="min", tz="UTC")
    return pd.DataFrame(
        {
            "open_time": ts,
            "quote_asset_volume": quote_values,
            "number_of_trades": trade_values,
        }
    )


def test_build_spot_kline_mirror_metrics_builds_ratio_and_window_columns() -> None:
    frame = build_spot_kline_mirror_metrics(
        _sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[100, 110, 120, 130, 140, 150],
            trade_values=[10, 11, 12, 13, 14, 15],
        ),
        lookback_minutes=2,
        baseline_minutes=3,
    )

    assert list(frame.columns) == [
        "open_time",
        "spot_quote_ratio",
        "spot_trades_ratio",
        "spot_quote_window",
        "spot_trades_window",
        "perp_quote_ratio",
        "perp_trades_ratio",
        "perp_quote_window",
        "perp_trades_window",
    ]
    assert float(frame.iloc[-1]["spot_quote_window"]) == 290.0
    assert float(frame.iloc[-1]["perp_trades_window"]) == 29.0
    assert frame.iloc[-1]["spot_quote_ratio"] > 1.0
    assert frame.iloc[-1]["perp_trades_ratio"] > 1.0


def test_backtest_liquidity_proxy_returns_degraded_status_when_soft_fails_hit_threshold() -> None:
    proxy = build_backtest_liquidity_proxy(
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[1000, 1000, 1000, 1000, 10, 10, 10, 10],
            trade_values=[100, 100, 100, 100, 1, 1, 1, 1],
        ),
        config=BacktestLiquidityProxyConfig(
            mode="spot_kline_mirror",
            lookback_minutes=2,
            baseline_minutes=4,
            soft_fail_min_count=2,
            thresholds=BacktestLiquidityThresholds(
                min_spot_quote_volume_ratio=0.5,
                min_spot_trades_ratio=0.5,
            ),
        ),
    )

    status = proxy.get_status(pd.Timestamp("2026-03-01T00:07:00Z"))

    assert status is not None
    assert status.blocked is False
    assert status.degraded is True
    assert set(status.reason_codes) == {"spot_quote_ratio", "spot_trades_ratio"}
    assert status.metrics["soft_fail_count"] == 2.0
    assert status.metrics["soft_fail_min_count"] == 2.0


def test_backtest_liquidity_proxy_returns_ok_when_thresholds_pass() -> None:
    proxy = build_backtest_liquidity_proxy(
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[100, 105, 110, 115, 120, 125],
            trade_values=[10, 10, 11, 11, 12, 12],
        ),
        config=BacktestLiquidityProxyConfig(
            mode="spot_kline_mirror",
            lookback_minutes=2,
            baseline_minutes=3,
            soft_fail_min_count=2,
            thresholds=BacktestLiquidityThresholds(
                min_spot_quote_volume_ratio=0.2,
                min_spot_trades_ratio=0.2,
            ),
        ),
    )

    status = proxy.get_status(pd.Timestamp("2026-03-01T00:05:00Z"))

    assert status is not None
    assert status.degraded is False
    assert status.reason_codes == ("ok",)


def test_backtest_liquidity_proxy_disabled_or_before_data_returns_none() -> None:
    disabled = build_backtest_liquidity_proxy(
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[100, 110, 120],
            trade_values=[10, 11, 12],
        ),
        config=BacktestLiquidityProxyConfig(mode="off"),
    )
    assert disabled.get_status(pd.Timestamp("2026-03-01T00:02:00Z")) is None

    enabled = build_backtest_liquidity_proxy(
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[100, 110, 120],
            trade_values=[10, 11, 12],
        ),
        config=BacktestLiquidityProxyConfig(mode="spot_kline_mirror"),
    )
    assert enabled.get_status(pd.Timestamp("2026-02-28T23:59:00Z")) is None


def test_backtest_liquidity_proxy_rejects_unknown_mode() -> None:
    with pytest.raises(ValueError):
        build_backtest_liquidity_proxy(
            raw_klines=_sample_klines(
                start="2026-03-01T00:00:00Z",
                quote_values=[100, 110, 120],
                trade_values=[10, 11, 12],
            ),
            config=BacktestLiquidityProxyConfig(mode="unsupported"),
        )
