from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.research.backtests.fills import BacktestFillConfig, build_canonical_fills
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec


def _accepted_row(*, decision_ts: str, captured_ts_ms: int) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_ts": decision_ts,
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "decision_source": "primary",
                "token_up": "token-up",
                "token_down": "token-down",
                "winner_side": "UP",
                "p_up": 0.80,
                "p_down": 0.20,
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "quote_down_ask": 0.80,
                "quote_down_bid": 0.79,
                "quote_down_ask_size_1": 10.0,
                "quote_captured_ts_ms_up": captured_ts_ms,
                "quote_captured_ts_ms_down": captured_ts_ms,
            }
        ]
    )


def _write_depth(
    data_cfg: DataConfig,
    *,
    logged_at: str,
    asks: list[list[float]],
) -> Path:
    path = data_cfg.layout.orderbook_depth_path("2026-03-01")
    append_ndjson_zst(
        path,
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": logged_at,
                "asks": asks,
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    return path


def test_canonical_fills_use_profile_depth_constraints_before_quote_fallback(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-01T00:05:30Z").timestamp() * 1000)
    depth_path = _write_depth(
        data_cfg,
        logged_at="2026-03-01T00:05:30+00:00",
        asks=[[0.20, 2.0], [0.21, 10.0]],
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_max_slippage_bps=0.0,
        orderbook_min_fill_ratio=0.50,
    )

    filled, rejected = build_canonical_fills(
        _accepted_row(
            decision_ts="2026-03-01T00:05:00Z",
            captured_ts_ms=captured_ts_ms,
        ),
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.95,
            fill_model="canonical_quote_depth",
            min_fill_ratio=0.0,
        ),
        profile_spec=profile_spec,
    )

    assert rejected.empty
    assert len(filled) == 1
    row = filled.iloc[0]
    assert row["fill_model"] == "canonical_quote"
    assert row["depth_status"] == "blocked"
    assert row["depth_reason"] == "depth_fill_ratio_below_threshold"
    assert row["depth_source_path"] == str(depth_path)
    assert row["depth_fill_ratio"] == pytest.approx(0.4)
    assert row["depth_requested_notional"] == pytest.approx(1.0)
    assert row["depth_remaining_notional"] == pytest.approx(0.6)
    assert row["depth_levels_available"] == 2
    assert row["depth_levels_consumed"] == 1
    assert bool(row["depth_partial_fill"]) is True
    assert row["depth_stop_reason"] == "price_limit_reached"
    assert row["depth_price_limit"] == pytest.approx(0.20)
    assert row["depth_snapshot_ts_ms"] == captured_ts_ms
    assert row["depth_snapshot_age_ms"] == 0
    assert row["depth_snapshot_distance_ms"] == 0
    assert row["stake"] == pytest.approx(1.0)


def test_canonical_fills_surface_depth_diagnostics_on_depth_fill(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-01T00:05:30Z").timestamp() * 1000)
    depth_path = _write_depth(
        data_cfg,
        logged_at="2026-03-01T00:05:30+00:00",
        asks=[[0.20, 1.0], [0.201, 10.0]],
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_max_slippage_bps=60.0,
        orderbook_min_fill_ratio=0.80,
    )

    filled, rejected = build_canonical_fills(
        _accepted_row(
            decision_ts="2026-03-01T00:05:00Z",
            captured_ts_ms=captured_ts_ms,
        ),
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.95,
            fill_model="canonical_quote_depth",
        ),
        profile_spec=profile_spec,
    )

    assert rejected.empty
    assert len(filled) == 1
    row = filled.iloc[0]
    assert row["fill_model"] == "canonical_depth"
    assert row["depth_status"] == "ok"
    assert row["depth_reason"] == ""
    assert row["depth_source_path"] == str(depth_path)
    assert row["depth_fill_ratio"] == pytest.approx(1.0)
    assert row["depth_best_price"] == pytest.approx(0.20)
    assert row["depth_max_price"] == pytest.approx(0.201)
    assert row["depth_avg_price"] == pytest.approx(row["entry_price"])
    assert row["depth_requested_notional"] == pytest.approx(1.0)
    assert row["depth_remaining_notional"] == pytest.approx(0.0)
    assert row["depth_levels_available"] == 2
    assert row["depth_levels_consumed"] == 2
    assert bool(row["depth_partial_fill"]) is False
    assert row["depth_stop_reason"] == "filled_target"
    assert row["depth_snapshot_ts_ms"] == captured_ts_ms
    assert row["depth_snapshot_age_ms"] == 0
    assert row["entry_price"] > 0.20
