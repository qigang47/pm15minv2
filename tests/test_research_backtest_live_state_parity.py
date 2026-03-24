from __future__ import annotations

import pandas as pd

from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.backtests.live_state_parity import attach_live_state_parity
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec


def _sample_klines(*, start: str, quote_values: list[float], trade_values: list[float]) -> pd.DataFrame:
    ts = pd.date_range(start, periods=len(quote_values), freq="min", tz="UTC")
    return pd.DataFrame(
        {
            "open_time": ts,
            "close": [100.0 + idx for idx in range(len(ts))],
            "quote_asset_volume": quote_values,
            "number_of_trades": trade_values,
        }
    )


def test_attach_live_state_parity_respects_parity_resolved_profile_spec() -> None:
    profile_spec = resolve_backtest_profile_spec(
        profile="deep_otm",
        parity=BacktestParitySpec(
            regime_enabled=False,
            liquidity_lookback_minutes=2,
            liquidity_baseline_minutes=4,
            liquidity_soft_fail_min_count=2,
        ),
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "offset": 7,
                "ret_15m": -0.0020,
                "ret_30m": -0.0030,
            },
            {
                "decision_ts": "2026-03-01T00:06:00Z",
                "offset": 7,
                "ret_15m": -0.0020,
                "ret_30m": -0.0030,
            },
        ]
    )

    out, summary = attach_live_state_parity(
        market="sol",
        profile=profile_spec,
        replay=replay,
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[1000, 1000, 1000, 1000, 10, 10, 10, 10],
            trade_values=[100, 100, 100, 100, 1, 1, 1, 1],
        ),
    )

    assert profile_spec.regime_controller_enabled is False
    assert profile_spec.liquidity_guard_lookback_minutes == 2
    assert profile_spec.liquidity_guard_baseline_minutes == 4
    assert summary.liquidity_available_rows == 2
    assert summary.liquidity_degraded_rows == 2
    assert summary.liquidity_status_counts == {"blocked": 1, "filtered_pending": 1}
    assert out.loc[0, "liquidity_status"] == "filtered_pending"
    assert bool(out.loc[1, "liquidity_degraded"]) is True
    assert out.loc[1, "liquidity_status"] == "blocked"
    assert out.loc[0, "regime_reason"] == "regime_controller_disabled"
    assert out.loc[1, "regime_state"] == "NORMAL"
