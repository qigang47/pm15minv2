from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.backtests.fills import BacktestFillConfig, build_canonical_fills, build_fill_plan_frame, summarize_fill_reasons
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec
from pm15min.research.backtests.settlement import build_equity_curve, settle_fill_frame, settlement_summary
from pm15min.research.backtests.taxonomy import build_reject_frame, summarize_reject_reasons


def test_build_fill_plan_frame_is_fee_and_stake_aware() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "offset": 7,
                "p_up": 0.82,
                "p_down": 0.18,
                "quote_prob_up": 0.70,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "offset": 7,
                "p_up": 0.60,
                "p_down": 0.40,
            },
            {
                "decision_ts": "2026-03-01T00:31:00Z",
                "offset": 7,
                "p_up": 0.51,
                "p_down": 0.49,
                "quote_prob_up": 0.52,
            },
        ]
    )

    out = build_fill_plan_frame(
        rows,
        base_stake=2.0,
        max_stake=3.0,
        min_edge=0.02,
        fee_bps=100.0,
        high_conf_threshold=0.8,
        high_conf_multiplier=2.0,
    )

    assert bool(out.iloc[0]["fill_valid"]) is True
    assert out.iloc[0]["entry_price_source"] == "quote_prob_up"
    assert float(out.iloc[0]["stake"]) == 3.0
    assert float(out.iloc[0]["fee_paid"]) == 0.03
    assert bool(out.iloc[1]["fill_valid"]) is True
    assert out.iloc[1]["entry_price_source"] == "p_up"
    assert out.iloc[2]["fill_reason"] == "below_min_edge"
    assert summarize_fill_reasons(out) == {"below_min_edge": 1}


def test_build_fill_plan_frame_applies_regime_stake_scale() -> None:
    profile_spec = resolve_backtest_profile_spec(
        profile="deep_otm",
        parity=BacktestParitySpec(
            regime_apply_stake_scale=True,
            regime_caution_stake_multiplier=0.5,
            regime_defense_stake_multiplier=0.25,
        ),
    )
    rows = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "offset": 7,
                "p_up": 0.82,
                "p_down": 0.18,
                "quote_prob_up": 0.70,
                "regime_state": "CAUTION",
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "offset": 7,
                "p_up": 0.82,
                "p_down": 0.18,
                "quote_prob_up": 0.70,
                "regime_state": "DEFENSE",
            },
            {
                "decision_ts": "2026-03-01T00:31:00Z",
                "offset": 7,
                "p_up": 0.82,
                "p_down": 0.18,
                "quote_prob_up": 0.70,
                "regime_state": "NORMAL",
            },
        ]
    )

    out = build_fill_plan_frame(
        rows,
        base_stake=2.0,
        max_stake=3.0,
        fee_bps=100.0,
        high_conf_threshold=0.8,
        high_conf_multiplier=2.0,
        profile_spec=profile_spec,
    )

    assert out["stake_base"].tolist() == [3.0, 3.0, 3.0]
    assert out["stake_multiplier"].tolist() == [0.5, 0.25, 1.0]
    assert out["stake_regime_state"].tolist() == ["CAUTION", "DEFENSE", "NORMAL"]
    assert out["stake"].tolist() == [1.5, 0.75, 3.0]
    assert out["fee_paid"].tolist() == [0.015, 0.0075, 0.03]


def test_build_canonical_fills_keeps_depth_diagnostics_when_depth_blocks_then_quote_fallbacks(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-01T00:08:30+00:00",
                "asks": [[0.20, 1.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.5,
        orderbook_max_slippage_bps=50.0,
    )
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.80,
                "p_down": 0.20,
                "winner_side": "UP",
            }
        ]
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=50.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
    )

    assert rejects.empty
    assert len(fills) == 1
    assert fills.loc[0, "fill_model"] == "canonical_quote"
    assert fills.loc[0, "depth_status"] == "blocked"
    assert fills.loc[0, "depth_reason"] == "depth_fill_ratio_below_threshold"
    assert fills.loc[0, "depth_source_path"].endswith("depth.ndjson.zst")
    assert float(fills.loc[0, "depth_fill_ratio"]) == 0.2
    assert float(fills.loc[0, "stake"]) == 1.0


def test_build_canonical_fills_uses_profile_depth_slippage_for_partial_depth_fill(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-01T00:08:30+00:00",
                "asks": [[0.20, 1.0], [0.202, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.80,
                "p_down": 0.20,
                "winner_side": "UP",
            }
        ]
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=50.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
    )

    assert rejects.empty
    assert len(fills) == 1
    assert fills.loc[0, "fill_model"] == "canonical_depth"
    assert fills.loc[0, "depth_status"] == "ok"
    assert fills.loc[0, "depth_reason"] == ""
    assert float(fills.loc[0, "depth_fill_ratio"]) == 1.0
    assert float(fills.loc[0, "depth_best_price"]) == 0.20
    assert float(fills.loc[0, "depth_max_price"]) == 0.202
    assert float(fills.loc[0, "entry_price"]) > 0.20


def test_build_canonical_fills_consumes_multi_snapshot_raw_depth_candidates(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.80,
                "p_down": 0.20,
                "predicted_side": "UP",
                "predicted_prob": 0.80,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.25, 1.0]],
                    "bids": [[0.24, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 5.0], [0.201, 5.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=50.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    assert fills.loc[0, "fill_model"] == "canonical_depth"
    assert fills.loc[0, "depth_status"] == "ok"
    assert fills.loc[0, "depth_reason"] == ""
    assert fills.loc[0, "depth_snapshot_ts_ms"] == int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000)
    assert float(fills.loc[0, "depth_best_price"]) == 0.20
    assert float(fills.loc[0, "depth_fill_ratio"]) == 1.0


def test_build_canonical_fills_accumulates_partial_raw_depth_candidates(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.22,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.21, 3.0]],
                    "bids": [[0.20, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth"
    assert row["depth_status"] == "ok"
    assert row["depth_reason"] == ""
    assert row["depth_snapshot_ts_ms"] == int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000)
    assert row["depth_fill_ratio"] == pytest.approx(1.0)
    assert row["depth_levels_consumed"] == 2
    assert row["depth_levels_available"] == 2
    assert bool(row["depth_partial_fill"]) is False
    assert row["depth_candidate_count"] == 2
    assert row["depth_candidate_progress_count"] == 2
    assert row["depth_chain_mode"] == "price_path"
    assert row["entry_price"] == pytest.approx(1.0 / (2.0 + (0.6 / 0.21)))
    assert row["stake"] == pytest.approx(1.0)


def test_build_canonical_fills_does_not_reuse_unchanged_raw_depth_queue(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.3,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth_quote"
    assert row["depth_status"] == "partial"
    assert row["depth_reason"] == "queue_path_stalled"
    assert row["depth_fill_ratio"] == pytest.approx(0.4)
    assert row["depth_candidate_count"] == 2
    assert row["depth_candidate_progress_count"] == 1
    assert row["depth_chain_mode"] == "single_snapshot"
    assert row["depth_queue_turnover_count"] == 0
    assert row["stake"] == pytest.approx(1.0)
    assert row["shares"] == pytest.approx(2.0 + (0.6 / 0.205))


def test_build_canonical_fills_reuses_same_price_only_after_queue_attrition(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 1.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 3,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:30Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.3,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth_quote"
    assert row["depth_status"] == "partial"
    assert row["depth_reason"] == "depth_exhausted"
    assert row["depth_fill_ratio"] == pytest.approx(0.6)
    assert row["depth_candidate_count"] == 3
    assert row["depth_candidate_progress_count"] == 2
    assert row["depth_chain_mode"] == "queue_growth"
    assert row["depth_queue_turnover_count"] == 1
    assert row["stake"] == pytest.approx(1.0)
    assert row["shares"] == pytest.approx(3.0 + (0.4 / 0.205))


def test_build_canonical_fills_uses_same_price_queue_growth_once(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 3.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.3,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth_quote"
    assert row["depth_status"] == "partial"
    assert row["depth_reason"] == "depth_exhausted"
    assert row["depth_fill_ratio"] == pytest.approx(0.6)
    assert row["depth_candidate_count"] == 2
    assert row["depth_candidate_progress_count"] == 2
    assert row["depth_chain_mode"] == "queue_growth"
    assert row["depth_queue_turnover_count"] == 0
    assert row["stake"] == pytest.approx(1.0)
    assert row["shares"] == pytest.approx(3.0 + (0.4 / 0.205))


def test_build_canonical_fills_allows_same_price_after_time_turnover_gap(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:09:00Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 0.8,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:50Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:50Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.3,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=0.8,
            max_stake=0.8,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            raw_depth_time_turnover_gap_ms=30_000,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth"
    assert row["depth_status"] == "ok"
    assert row["depth_reason"] == ""
    assert row["depth_fill_ratio"] == pytest.approx(1.0)
    assert row["depth_candidate_count"] == 2
    assert row["depth_candidate_progress_count"] == 2
    assert row["depth_chain_mode"] == "time_turnover"
    assert row["depth_queue_turnover_count"] == 0
    assert row["depth_time_turnover_count"] == 1
    assert row["stake"] == pytest.approx(0.8)
    assert row["shares"] == pytest.approx(4.0)


def test_build_canonical_fills_legacy_fak_refresh_uses_later_snapshot_after_block(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 5.0], [0.201, 5.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth"
    assert row["depth_status"] == "ok"
    assert row["depth_fill_ratio"] == pytest.approx(1.0)
    assert row["depth_candidate_count"] == 2
    assert row["depth_candidate_progress_count"] == 2
    assert row["depth_chain_mode"] == "refresh_retry"
    assert row["depth_retry_refresh_count"] == 1


def test_build_canonical_fills_default_uses_legacy_fak_refresh_mode(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 5.0], [0.201, 5.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["depth_chain_mode"] == "refresh_retry"
    assert row["depth_retry_refresh_count"] == 1


def test_build_canonical_fills_legacy_fak_refresh_caps_candidates_by_orderbook_retry_budget(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.50, 1.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.50, 1.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 3,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:30Z",
                    "asks": [[0.20, 5.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_fast_retry_max=2,
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    capped_row = fills.iloc[0]
    assert capped_row["fill_model"] == "canonical_quote"
    assert capped_row["depth_candidate_count"] == 2
    assert capped_row["depth_candidate_total_count"] == 3
    assert capped_row["depth_retry_budget"] == 2
    assert bool(capped_row["depth_retry_budget_exhausted"]) is True
    assert capped_row["depth_retry_budget_source"] == "orderbook_fast_retry_max"
    assert capped_row["depth_retry_stage"] == "pre_submit_orderbook_recheck"
    assert capped_row["depth_retry_exit_reason"] == "retry_budget_exhausted"

    fills_all, rejects_all = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=replace(profile_spec, orderbook_fast_retry_max=3),
        ),
        profile_spec=replace(profile_spec, orderbook_fast_retry_max=3),
        depth_replay=depth_replay,
    )

    assert rejects_all.empty
    assert len(fills_all) == 1
    row = fills_all.iloc[0]
    assert row["depth_candidate_count"] == 3
    assert row["depth_candidate_total_count"] == 3
    assert row["depth_retry_budget"] == 3
    assert bool(row["depth_retry_budget_exhausted"]) is False
    assert row["depth_retry_stage"] == "pre_submit_orderbook_recheck"


def test_build_canonical_fills_legacy_fak_refresh_captures_retry_trigger_reason(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.50, 1.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 10.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    row = fills.iloc[0]
    assert row["depth_retry_refresh_count"] == 1
    assert row["depth_retry_trigger_reason"] == "depth_fill_unavailable"
    assert row["depth_retry_stage"] == "pre_submit_orderbook_recheck"
    assert row["depth_retry_exit_reason"] == "filled_target"
    assert row["depth_retry_budget_source"] == "orderbook_fast_retry_max"


def test_build_canonical_fills_legacy_retry_stops_when_snapshot_marker_is_unchanged(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    repeated_ts_ms = int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000)
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.50, 1.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": repeated_ts_ms,
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 10.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": repeated_ts_ms,
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_fast_retry_max=3,
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_quote"
    assert row["depth_retry_stage"] == "pre_submit_orderbook_recheck"
    assert row["depth_retry_exit_reason"] == "orderbook_snapshot_unchanged"
    assert row["depth_retry_snapshot_unchanged_count"] == 1
    assert row["depth_retry_refresh_count"] == 0


def test_build_canonical_fills_legacy_fak_refresh_rejects_repriced_entry_price_max(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.31, 5.0]],
                    "bids": [[0.30, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        entry_price_max=0.30,
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert fills.empty
    assert rejects["reason"].tolist() == ["repriced_entry_price_max"]


def test_build_canonical_fills_legacy_fak_refresh_rejects_repriced_net_edge_below_threshold(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.65,
                "p_down": 0.35,
                "predicted_side": "UP",
                "predicted_prob": 0.65,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.61, 5.0]],
                    "bids": [[0.60, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        entry_price_max=None,
        min_net_edge_default=0.05,
        min_net_edge_by_offset={},
        min_net_edge_entry_price_le_0p10_bonus=0.0,
        min_net_edge_entry_price_le_0p05_bonus=0.0,
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert fills.empty
    assert rejects["reason"].tolist() == ["repriced_net_edge_below_threshold"]


def test_build_canonical_fills_legacy_fak_refresh_rejects_repriced_roi_below_threshold(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.61,
                "p_down": 0.39,
                "predicted_side": "UP",
                "predicted_prob": 0.61,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 2.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.58, 5.0]],
                    "bids": [[0.57, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        entry_price_max=None,
        min_net_edge_default=0.0,
        min_net_edge_by_offset={},
        roi_threshold_default=0.08,
        roi_threshold_by_offset={},
        orderbook_min_fill_ratio=0.9,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert fills.empty
    assert rejects["reason"].tolist() == ["repriced_roi_below_threshold"]


def test_build_canonical_fills_legacy_fak_refresh_keeps_partial_fill_without_quote_completion(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 3.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            },
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:20Z",
                    "asks": [[0.20, 8.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:20Z").timestamp() * 1000),
                "depth_down_record": None,
            },
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.3,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=True,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth"
    assert row["depth_status"] == "partial"
    assert row["depth_reason"] == "depth_exhausted"
    assert bool(row["depth_partial_fill"]) is True
    assert row["depth_fill_ratio"] == pytest.approx(0.6)
    assert row["depth_candidate_progress_count"] == 1
    assert row["depth_chain_mode"] == "single_snapshot"
    assert row["depth_retry_refresh_count"] == 0
    assert row["stake"] == pytest.approx(0.6)


def test_build_canonical_fills_completes_partial_depth_with_quote_fallback(tmp_path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-01T00:08:30Z").timestamp() * 1000),
                "quote_up_ask": 0.205,
                "quote_up_bid": 0.19,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.95,
                "p_down": 0.05,
                "predicted_side": "UP",
                "predicted_prob": 0.95,
                "winner_side": "UP",
                "stake": 1.0,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_source_path": str(data_cfg.layout.orderbook_depth_path("2026-03-01")),
                "depth_up_record": {
                    "logged_at": "2026-03-01T00:08:10Z",
                    "asks": [[0.20, 3.0]],
                    "bids": [[0.19, 1.0]],
                },
                "depth_up_snapshot_ts_ms": int(pd.Timestamp("2026-03-01T00:08:10Z").timestamp() * 1000),
                "depth_down_record": None,
            }
        ]
    )
    profile_spec = replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        orderbook_min_fill_ratio=0.5,
        orderbook_max_slippage_bps=150.0,
    )

    fills, rejects = build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            raw_depth_fak_refresh_enabled=False,
            profile_spec=profile_spec,
        ),
        profile_spec=profile_spec,
        depth_replay=depth_replay,
    )

    assert rejects.empty
    assert len(fills) == 1
    row = fills.iloc[0]
    assert row["fill_model"] == "canonical_depth_quote"
    assert row["depth_status"] == "partial"
    assert row["depth_reason"] == "depth_exhausted"
    assert row["depth_fill_ratio"] == pytest.approx(0.6)
    assert bool(row["depth_partial_fill"]) is True
    assert row["fill_ratio"] == pytest.approx(1.0)
    assert row["stake"] == pytest.approx(1.0)
    assert row["shares"] == pytest.approx(3.0 + (0.4 / 0.205))
    assert row["entry_price"] == pytest.approx(1.0 / (3.0 + (0.4 / 0.205)))


def test_settle_fill_frame_and_equity_curve() -> None:
    filled = build_fill_plan_frame(
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "p_up": 0.80,
                    "p_down": 0.20,
                    "quote_prob_up": 0.70,
                    "resolved": True,
                    "winner_side": "UP",
                },
                {
                    "decision_ts": "2026-03-01T00:16:00Z",
                    "cycle_start_ts": "2026-03-01T00:15:00Z",
                    "cycle_end_ts": "2026-03-01T00:30:00Z",
                    "offset": 8,
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "p_up": 0.25,
                    "p_down": 0.75,
                    "quote_prob_down": 0.65,
                    "resolved": True,
                    "winner_side": "UP",
                },
            ]
        ),
        base_stake=1.0,
        fee_bps=50.0,
    )

    trades = settle_fill_frame(filled)
    curve = build_equity_curve(trades)
    summary = settlement_summary(trades)

    assert len(trades) == 2
    assert bool(trades.iloc[0]["win"]) is True
    assert bool(trades.iloc[1]["win"]) is False
    assert summary["trades"] == 2
    assert summary["wins"] == 1
    assert "cumulative_roi_pct" in curve.columns
    assert curve["trade_number"].tolist() == [1, 2]


def test_build_reject_frame_uses_explicit_taxonomy() -> None:
    rows = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 4,
                "resolved": True,
                "score_valid": True,
                "fill_valid": False,
                "fill_reason": "quote_missing",
                "winner_side": "UP",
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "resolved": False,
                "score_valid": True,
                "fill_valid": False,
                "fill_reason": "",
                "winner_side": "",
            },
            {
                "decision_ts": "2026-03-01T00:31:00Z",
                "cycle_start_ts": "2026-03-01T00:30:00Z",
                "cycle_end_ts": "2026-03-01T00:45:00Z",
                "offset": 8,
                "resolved": True,
                "score_valid": False,
                "score_reason": "missing_reversal_anchor",
                "fill_valid": False,
                "fill_reason": "",
                "winner_side": "DOWN",
            },
        ]
    )

    rejects = build_reject_frame(rows, available_offsets=[7, 8])

    assert rejects["reason"].tolist() == [
        "bundle_offset_missing",
        "unresolved_label",
        "missing_reversal_anchor",
    ]
    assert summarize_reject_reasons(rejects) == {
        "bundle_offset_missing": 1,
        "missing_reversal_anchor": 1,
        "unresolved_label": 1,
    }
