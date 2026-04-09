from __future__ import annotations

import pandas as pd

from pm15min.research.backtests.decision_engine_parity import (
    DecisionEngineParityConfig,
    build_profile_decision_engine_parity_config,
)
from pm15min.research.backtests.decision_quote_surface import apply_initial_snapshot_decision_parity
from pm15min.research.backtests.fills import BacktestFillConfig
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec


def test_initial_snapshot_decision_parity_rejects_orderbook_missing_before_decision() -> None:
    profile_spec = resolve_backtest_profile_spec(market="sol", profile="deep_otm")
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.70,
                "p_down": 0.30,
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.10,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 0,
                "depth_up_record": {"asks": []},
                "depth_down_record": {"asks": [[0.10, 20.0]]},
            }
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=BacktestFillConfig(base_stake=1.0, max_stake=1.0, profile_spec=profile_spec),
        decision_config=build_profile_decision_engine_parity_config(market="sol", profile_spec=profile_spec),
    )

    assert summary.orderbook_missing_rows == 1
    assert bool(out.loc[0, "decision_quote_orderbook_missing"])
    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "orderbook_missing"


def test_initial_snapshot_decision_parity_rejects_when_first_snapshot_hits_limit_on_both_sides() -> None:
    profile_spec = resolve_backtest_profile_spec(market="sol", profile="deep_otm")
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.58,
                "p_down": 0.42,
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.18,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 0,
                "depth_up_record": {"asks": [[0.70, 20.0]]},
                "depth_down_record": {"asks": [[0.60, 20.0]]},
            }
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=BacktestFillConfig(base_stake=1.0, max_stake=1.0, profile_spec=profile_spec),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert summary.limit_reject_rows == 0
    assert bool(out.loc[0, "decision_quote_limit_reject"]) is False
    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "net_edge"


def test_initial_snapshot_decision_parity_scans_later_snapshot_for_repriced_surface() -> None:
    profile_spec = resolve_backtest_profile_spec(market="sol", profile="deep_otm")
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.58,
                "p_down": 0.42,
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.18,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 0,
                "depth_up_record": {"asks": [[0.70, 20.0]]},
                "depth_down_record": {"asks": [[0.15, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_up_record": {"asks": [[0.20, 20.0]]},
                "depth_down_record": {"asks": [[0.15, 20.0]]},
            },
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=BacktestFillConfig(base_stake=1.0, max_stake=1.0, profile_spec=profile_spec),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert summary.repriced_rows == 1
    assert summary.candidate_total_rows == 2
    assert summary.candidate_examined_rows == 2
    assert out.loc[0, "decision_quote_up_status"] == "filled_target"
    assert out.loc[0, "decision_quote_up_ask"] == 0.20
    assert out.loc[0, "decision_quote_down_ask"] == 0.15
    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "UP"


def test_initial_snapshot_decision_parity_tracks_signal_side_orderbook_reason_counts() -> None:
    profile_spec = resolve_backtest_profile_spec(market="sol", profile="deep_otm")
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.70,
                "p_down": 0.30,
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 0,
                "depth_up_record": {"asks": []},
                "depth_down_record": {"asks": [[0.29, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_up_record": {"asks": [[0.80, 20.0]]},
                "depth_down_record": {"asks": [[0.29, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_up_record": {"asks": [[0.20, 2.0]]},
                "depth_down_record": {"asks": [[0.29, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 3,
                "depth_up_record": {"asks": [[0.20, 20.0]]},
                "depth_down_record": {"asks": [[0.29, 20.0]]},
            },
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=BacktestFillConfig(base_stake=1.0, max_stake=1.0, profile_spec=profile_spec),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "UP"
    assert out.loc[0, "decision_quote_up_status"] == "filled_target"
    assert summary.signal_rows == 1
    assert summary.signal_candidate_total_rows == 4
    assert summary.signal_candidate_examined_rows == 4
    assert summary.signal_candidate_orderbook_missing_rows == 1
    assert summary.signal_candidate_price_reject_rows == 1
    assert summary.signal_candidate_depth_reject_rows == 1
    assert summary.signal_candidate_fillable_rows == 2


def test_initial_snapshot_decision_parity_scans_full_window_before_accepting_signal_side_price() -> None:
    profile_spec = resolve_backtest_profile_spec(market="sol", profile="deep_otm")
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "p_up": 0.82,
                "p_down": 0.18,
                "quote_up_ask": 0.34,
                "quote_down_ask": 0.10,
            }
        ]
    )
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 0,
                "depth_up_record": {"asks": [[0.34, 20.0]]},
                "depth_down_record": {"asks": [[0.10, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_up_record": {"asks": [[0.36, 20.0]]},
                "depth_down_record": {"asks": [[0.10, 20.0]]},
            },
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_up_record": {"asks": [[0.25, 20.0]]},
                "depth_down_record": {"asks": [[0.10, 20.0]]},
            },
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        replay,
        depth_replay=depth_replay,
        profile_spec=profile_spec,
        fill_config=BacktestFillConfig(base_stake=1.0, max_stake=1.0, profile_spec=profile_spec),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "UP"
    assert out.loc[0, "decision_quote_up_status"] == "filled_target"
    assert out.loc[0, "decision_quote_up_ask"] == 0.25
    assert summary.candidate_total_rows == 3
    assert summary.candidate_examined_rows == 3
    assert summary.signal_rows == 1
    assert summary.signal_candidate_total_rows == 3
    assert summary.signal_candidate_examined_rows == 3
    assert summary.signal_candidate_price_reject_rows == 2
    assert summary.signal_candidate_fillable_rows == 3
