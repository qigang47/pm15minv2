from __future__ import annotations

from dataclasses import replace

import pandas as pd
import pytest

from pm15min.research.backtests.decision_quote_surface import apply_initial_snapshot_decision_parity
from pm15min.research.backtests.decision_engine_parity import (
    DecisionEngineParityConfig,
    apply_decision_engine_parity,
    evaluate_decision_engine_side,
)
from pm15min.research.backtests.engine import _attach_decision_engine_surface
from pm15min.research.backtests.fills import BacktestFillConfig
from pm15min.research.backtests.regime_parity import resolve_backtest_profile_spec


def test_apply_decision_engine_parity_prefers_best_roi_net_side() -> None:
    rows = pd.DataFrame(
        [
            {
                "offset": 7,
                "p_up": 0.58,
                "p_down": 0.42,
                "quote_up_ask": 0.57,
                "quote_down_ask": 0.20,
            }
        ]
    )

    out = apply_decision_engine_parity(
        rows,
        config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "DOWN"
    assert out.loc[0, "decision_engine_rationale"] == "p_down_eff=0.4200 vs price=0.2000"
    assert out.loc[0, "decision_engine_entry_price"] == pytest.approx(0.20)
    assert out.loc[0, "decision_engine_prob"] == pytest.approx(0.42)
    assert out.loc[0, "decision_engine_probability_gap"] == pytest.approx(0.16)
    assert out.loc[0, "decision_engine_roi_net"] == pytest.approx(1.1)


def test_apply_decision_engine_parity_supports_custom_columns_and_probability_boost() -> None:
    rows = pd.DataFrame(
        [
            {
                "my_offset": 7,
                "prob_up": 0.56,
                "prob_down": 0.44,
                "up_ask": 0.30,
                "down_ask": 0.40,
                "boost": 0.05,
            }
        ]
    )

    out = apply_decision_engine_parity(
        rows,
        config=DecisionEngineParityConfig(min_dir_prob_default=0.52),
        offset_column="my_offset",
        p_up_column="prob_up",
        p_down_column="prob_down",
        up_price_columns=("up_ask",),
        down_price_columns=("down_ask",),
        min_dir_prob_boost_column="boost",
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "direction_prob"
    assert pd.isna(out.loc[0, "decision_engine_side"])
    assert out.loc[0, "decision_engine_probability_gap"] == pytest.approx(0.12)


def test_evaluate_decision_engine_side_breaks_roi_net_ties_on_probability() -> None:
    decision = evaluate_decision_engine_side(
        offset=7,
        p_up=0.50,
        p_down=0.25,
        up_price=0.25,
        down_price=0.125,
        config=DecisionEngineParityConfig(min_dir_prob_default=0.20),
    )

    assert decision.action == "trade"
    assert decision.side == "UP"
    assert decision.selected_prob == pytest.approx(0.50)
    assert decision.roi_net == pytest.approx(1.0)
    assert decision.rationale == "p_up_eff=0.5000 vs price=0.2500"


@pytest.mark.parametrize(
    ("config", "kwargs", "expected_reason"),
    [
        (
            DecisionEngineParityConfig(min_dir_prob_default=0.65),
            {"p_up": 0.60, "p_down": 0.40, "up_price": 0.30, "down_price": 0.20},
            "direction_prob",
        ),
        (
            DecisionEngineParityConfig(min_dir_prob_default=0.40, min_net_edge_default=0.05),
            {"p_up": 0.52, "p_down": 0.48, "up_price": 0.50, "down_price": 0.46},
            "net_edge",
        ),
        (
            DecisionEngineParityConfig(min_dir_prob_default=0.40, roi_threshold_default=0.10),
            {"p_up": 0.55, "p_down": 0.45, "up_price": 0.52, "down_price": 0.43},
            "roi_or_price",
        ),
        (
            DecisionEngineParityConfig(
                min_dir_prob_default=0.40,
                enforce_entry_price_band=True,
                entry_price_max=0.30,
            ),
            {"p_up": 0.70, "p_down": 0.30, "up_price": 0.35, "down_price": 0.40},
            "entry_price_max",
        ),
        (
            DecisionEngineParityConfig(
                min_dir_prob_default=0.40,
                enforce_price_bounds=True,
                price_floor=0.05,
                price_cap=0.95,
            ),
            {"p_up": 0.70, "p_down": 0.30, "up_price": 0.01, "down_price": 0.97},
            "price_bounds",
        ),
    ],
)
def test_evaluate_decision_engine_side_matches_reject_taxonomy(
    config: DecisionEngineParityConfig,
    kwargs: dict[str, float],
    expected_reason: str,
) -> None:
    decision = evaluate_decision_engine_side(
        offset=7,
        config=config,
        **kwargs,
    )

    assert decision.action == "reject"
    assert decision.reason == expected_reason
    assert decision.side is None
    assert decision.rationale == ""


def test_apply_initial_snapshot_decision_parity_prefers_repriced_first_snapshot_side() -> None:
    rows = _decision_replay_rows()
    depth_replay = pd.DataFrame(
        [
            _depth_snapshot_candidate(
                rank=0,
                up_asks=[[0.60, 10.0]],
                down_asks=[[0.20, 10.0]],
            )
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        rows,
        depth_replay=depth_replay,
        profile_spec=_decision_profile_spec(),
        fill_config=_decision_fill_config(),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "DOWN"
    assert out.loc[0, "decision_engine_entry_price"] == pytest.approx(0.20)
    assert out.loc[0, "decision_quote_up_status"] == "orderbook_limit_reject"
    assert out.loc[0, "decision_quote_down_status"] == "filled_target"
    assert summary.to_dict() == {
        "raw_depth_rows": 1,
        "repriced_rows": 1,
        "limit_reject_rows": 0,
        "orderbook_missing_rows": 0,
    }


def test_apply_initial_snapshot_decision_parity_rejects_when_first_snapshot_is_limit_blocked() -> None:
    rows = _decision_replay_rows()
    depth_replay = pd.DataFrame(
        [
            _depth_snapshot_candidate(
                rank=0,
                up_asks=[[0.60, 10.0]],
                down_asks=[[0.50, 10.0]],
            )
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        rows,
        depth_replay=depth_replay,
        profile_spec=_decision_profile_spec(),
        fill_config=_decision_fill_config(),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "orderbook_limit_reject"
    assert pd.isna(out.loc[0, "decision_engine_side"])
    assert summary.to_dict() == {
        "raw_depth_rows": 1,
        "repriced_rows": 0,
        "limit_reject_rows": 1,
        "orderbook_missing_rows": 0,
    }


def test_apply_initial_snapshot_decision_parity_rejects_partial_first_snapshot_without_mixing_later_snapshot() -> None:
    rows = _decision_replay_rows()
    depth_replay = pd.DataFrame(
        [
            _depth_snapshot_candidate(
                rank=0,
                up_asks=[[0.20, 10.0]],
                down_asks=None,
            ),
            _depth_snapshot_candidate(
                rank=1,
                up_asks=[[0.20, 10.0]],
                down_asks=[[0.20, 10.0]],
            ),
        ]
    )

    out, summary = apply_initial_snapshot_decision_parity(
        rows,
        depth_replay=depth_replay,
        profile_spec=_decision_profile_spec(),
        fill_config=_decision_fill_config(),
        decision_config=DecisionEngineParityConfig(min_dir_prob_default=0.40),
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "orderbook_missing"
    assert bool(out.loc[0, "decision_quote_orderbook_missing"]) is True
    assert out.loc[0, "decision_quote_up_status"] == "filled_target"
    assert out.loc[0, "decision_quote_down_status"] == "orderbook_missing"
    assert summary.to_dict() == {
        "raw_depth_rows": 1,
        "repriced_rows": 1,
        "limit_reject_rows": 0,
        "orderbook_missing_rows": 1,
    }


def test_attach_decision_engine_surface_uses_probability_threshold_only_when_window_scan_mode_enabled() -> None:
    rows = _decision_replay_rows(p_up=0.82, p_down=0.18, quote_up_ask=0.81, quote_down_ask=0.19)
    depth_replay = pd.DataFrame(
        [
            _depth_snapshot_candidate(
                rank=0,
                up_asks=[[0.60, 10.0]],
                down_asks=[[0.20, 10.0]],
            )
        ]
    )

    legacy_out, legacy_summary = _attach_decision_engine_surface(
        rows,
        market="sol",
        profile_spec=_decision_profile_spec(),
        depth_replay=depth_replay,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )
    research_out, research_summary = _attach_decision_engine_surface(
        rows,
        market="sol",
        profile_spec=_decision_profile_spec(),
        depth_replay=depth_replay,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=False),
    )

    assert legacy_out.loc[0, "decision_engine_action"] == "trade"
    assert legacy_out.loc[0, "decision_engine_reason"] == "trade"
    assert legacy_out.loc[0, "decision_engine_side"] == "UP"
    assert bool(legacy_out.loc[0, "decision_quote_limit_reject"]) is False
    assert bool(legacy_out.loc[0, "pre_submit_orderbook_retry_armed"]) is False
    assert legacy_summary.to_dict() == {
        "raw_depth_rows": 1,
        "repriced_rows": 1,
        "limit_reject_rows": 0,
        "orderbook_missing_rows": 0,
    }
    assert research_out.loc[0, "decision_engine_action"] == "trade"
    assert research_out.loc[0, "decision_engine_reason"] == "trade"
    assert research_out.loc[0, "decision_engine_side"] == "UP"
    assert research_summary.to_dict() == {
        "raw_depth_rows": 0,
        "repriced_rows": 0,
        "limit_reject_rows": 0,
        "orderbook_missing_rows": 0,
    }


def test_attach_decision_engine_surface_does_not_arm_pre_submit_retry_for_initial_snapshot_limit_reject() -> None:
    rows = _decision_replay_rows(p_up=0.58, p_down=0.42)
    depth_replay = pd.DataFrame(
        [
            _depth_snapshot_candidate(
                rank=0,
                up_asks=[[0.70, 10.0]],
                down_asks=[[0.60, 10.0]],
            )
        ]
    )

    out, _summary = _attach_decision_engine_surface(
        rows,
        market="sol",
        profile_spec=_decision_profile_spec(),
        depth_replay=depth_replay,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "direction_prob"
    assert bool(out.loc[0, "decision_quote_limit_reject"]) is True
    assert bool(out.loc[0, "pre_submit_orderbook_retry_armed"]) is False
    assert out.loc[0, "pre_submit_orderbook_retry_reason"] == ""
    assert out.loc[0, "pre_submit_orderbook_retry_state_key"] == ""


def test_attach_decision_engine_surface_missing_depth_fallback_uses_probability_threshold_only() -> None:
    rows = _decision_replay_rows(p_up=0.82, p_down=0.18, quote_up_ask=0.81, quote_down_ask=0.19)

    out, summary = _attach_decision_engine_surface(
        rows,
        market="eth",
        profile_spec=resolve_backtest_profile_spec(market="eth", profile="deep_otm_baseline"),
        depth_replay=None,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "UP"
    assert bool(out.loc[0, "decision_quote_has_raw_depth"]) is False
    assert bool(out.loc[0, "pre_submit_orderbook_retry_armed"]) is False
    assert summary.to_dict() == {
        "raw_depth_rows": 0,
        "repriced_rows": 0,
        "limit_reject_rows": 0,
        "orderbook_missing_rows": 0,
    }


def test_attach_decision_engine_surface_probability_only_keeps_up_side_when_up_interval_passes_even_if_down_is_cheaper() -> None:
    rows = _decision_replay_rows(
        signal_target="direction",
        p_up=0.65,
        p_down=0.30,
        p_up_raw=0.70,
        p_down_raw=0.30,
        p_eff_up=0.65,
        p_eff_down=0.30,
        p_up_lcb=0.65,
        p_up_ucb=0.70,
        quote_up_ask=0.29,
        quote_down_ask=0.05,
    )

    out, _summary = _attach_decision_engine_surface(
        rows,
        market="eth",
        profile_spec=resolve_backtest_profile_spec(market="eth", profile="deep_otm_baseline"),
        depth_replay=None,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "UP"


def test_attach_decision_engine_surface_probability_only_rejects_up_when_lcb_is_not_above_threshold() -> None:
    rows = _decision_replay_rows(
        signal_target="direction",
        p_up=0.60,
        p_down=0.32,
        p_up_raw=0.68,
        p_down_raw=0.32,
        p_eff_up=0.60,
        p_eff_down=0.32,
        p_up_lcb=0.60,
        p_up_ucb=0.68,
        quote_up_ask=0.29,
        quote_down_ask=0.05,
    )

    out, _summary = _attach_decision_engine_surface(
        rows,
        market="eth",
        profile_spec=resolve_backtest_profile_spec(market="eth", profile="deep_otm_baseline"),
        depth_replay=None,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "direction_prob"
    assert pd.isna(out.loc[0, "decision_engine_side"])


def test_attach_decision_engine_surface_probability_only_trades_down_when_down_interval_passes() -> None:
    rows = _decision_replay_rows(
        signal_target="direction",
        p_up=0.28,
        p_down=0.72,
        p_up_raw=0.28,
        p_down_raw=0.72,
        p_eff_up=0.28,
        p_eff_down=0.72,
        p_up_lcb=0.28,
        p_up_ucb=0.28,
        quote_up_ask=0.74,
        quote_down_ask=0.05,
    )

    out, _summary = _attach_decision_engine_surface(
        rows,
        market="eth",
        profile_spec=resolve_backtest_profile_spec(market="eth", profile="deep_otm_baseline"),
        depth_replay=None,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "trade"
    assert out.loc[0, "decision_engine_reason"] == "trade"
    assert out.loc[0, "decision_engine_side"] == "DOWN"


def test_attach_decision_engine_surface_probability_only_rejects_down_when_ucb_is_not_below_threshold() -> None:
    rows = _decision_replay_rows(
        signal_target="direction",
        p_up=0.41,
        p_down=0.59,
        p_up_raw=0.41,
        p_down_raw=0.59,
        p_eff_up=0.41,
        p_eff_down=0.59,
        p_up_lcb=0.41,
        p_up_ucb=0.41,
        quote_up_ask=0.20,
        quote_down_ask=0.29,
    )

    out, _summary = _attach_decision_engine_surface(
        rows,
        market="eth",
        profile_spec=resolve_backtest_profile_spec(market="eth", profile="deep_otm_baseline"),
        depth_replay=None,
        fill_config=_decision_fill_config(raw_depth_fak_refresh_enabled=True),
    )

    assert out.loc[0, "decision_engine_action"] == "reject"
    assert out.loc[0, "decision_engine_reason"] == "direction_prob"
    assert pd.isna(out.loc[0, "decision_engine_side"])


def _decision_replay_rows(**overrides: object) -> pd.DataFrame:
    row = {
        "decision_ts": "2026-03-01T00:08:00Z",
        "cycle_start_ts": "2026-03-01T00:00:00Z",
        "cycle_end_ts": "2026-03-01T00:15:00Z",
        "offset": 7,
        "p_up": 0.58,
        "p_down": 0.42,
        "quote_up_ask": 0.30,
        "quote_down_ask": 0.50,
    }
    row.update(overrides)
    return pd.DataFrame([row])


def _depth_snapshot_candidate(
    *,
    rank: int,
    up_asks: list[list[float]] | None,
    down_asks: list[list[float]] | None,
) -> dict[str, object]:
    return {
        "decision_ts": "2026-03-01T00:08:00Z",
        "cycle_start_ts": "2026-03-01T00:00:00Z",
        "cycle_end_ts": "2026-03-01T00:15:00Z",
        "offset": 7,
        "depth_snapshot_rank": rank,
        "depth_up_record": None if up_asks is None else {"asks": up_asks},
        "depth_down_record": None if down_asks is None else {"asks": down_asks},
    }


def _decision_profile_spec():
    return replace(
        resolve_backtest_profile_spec(profile="deep_otm"),
        roi_threshold_default=0.0,
        roi_threshold_by_offset={},
        slippage_bps=0.0,
        fee_model="flat_bps",
        fee_bps=0.0,
        orderbook_max_slippage_bps=0.0,
    )


def _decision_fill_config(*, raw_depth_fak_refresh_enabled: bool = True) -> BacktestFillConfig:
    return BacktestFillConfig(
        base_stake=1.0,
        max_stake=1.0,
        high_conf_threshold=0.99,
        high_conf_multiplier=1.0,
        depth_max_slippage_bps=0.0,
        raw_depth_fak_refresh_enabled=raw_depth_fak_refresh_enabled,
    )
