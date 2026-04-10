from __future__ import annotations

from dataclasses import replace

from dataclasses import replace

import pandas as pd

from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.live.profiles import resolve_live_profile_spec
from pm15min.research.backtests.guard_parity import apply_live_guard_parity
from pm15min.research.backtests.regime_parity import (
    attach_backtest_regime_parity,
    build_backtest_regime_liquidity_config,
    build_backtest_regime_liquidity_proxy,
    build_backtest_regime_state,
    resolve_backtest_profile_spec,
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


def _sample_features(*, ret_15m: float, ret_30m: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "offset": 7,
                "ret_15m": ret_15m,
                "ret_30m": ret_30m,
            }
        ]
    )


def test_build_backtest_regime_liquidity_config_uses_live_profile_thresholds() -> None:
    config = build_backtest_regime_liquidity_config(
        market="sol",
        profile="deep_otm",
    )

    assert config.mode == "spot_kline_mirror"
    assert config.lookback_minutes == 10
    assert config.baseline_minutes == 180
    assert config.soft_fail_min_count == 2
    assert config.thresholds.min_spot_quote_volume_window == 3.0e6
    assert config.thresholds.min_perp_quote_volume_window == 3.0e6
    assert config.thresholds.min_spot_trades_window == 2500.0
    assert config.thresholds.min_perp_trades_window == 2500.0


def test_resolve_backtest_profile_spec_applies_parity_overrides() -> None:
    spec = resolve_backtest_profile_spec(
        profile="deep_otm",
        parity=BacktestParitySpec(
            regime_enabled=False,
            regime_defense_force_with_pressure=False,
            regime_defense_max_trades_per_market=3,
            liquidity_lookback_minutes=6,
            liquidity_baseline_minutes=90,
            liquidity_soft_fail_min_count=4,
        ),
    )

    assert spec.regime_controller_enabled is False
    assert spec.regime_defense_force_with_pressure is False
    assert spec.regime_defense_max_trades_per_market == 3
    assert spec.liquidity_guard_lookback_minutes == 6
    assert spec.liquidity_guard_baseline_minutes == 90
    assert spec.liquidity_guard_soft_fail_min_count == 4


def test_resolve_backtest_profile_spec_can_disable_ret_30m_direction_guard() -> None:
    spec = resolve_backtest_profile_spec(
        market="xrp",
        profile="deep_otm",
        parity=BacktestParitySpec(disable_ret_30m_direction_guard=True),
    )

    assert spec.ret_30m_up_floor_for("xrp") == -1.0e9
    assert spec.ret_30m_down_ceiling_for("xrp") == 1.0e9
    assert spec.ret_30m_up_floor_for("sol") == 0.0
    assert spec.ret_30m_down_ceiling_for("sol") == 0.002


def test_build_backtest_regime_state_stays_normal_with_proxy_liquidity_status() -> None:
    spec = replace(
        resolve_live_profile_spec("deep_otm"),
        liquidity_guard_lookback_minutes=2,
        liquidity_guard_baseline_minutes=3,
        liquidity_guard_soft_fail_min_count=2,
        liquidity_min_spot_quote_volume_ratio_by_asset={"sol": 0.2},
        liquidity_min_perp_quote_volume_ratio_by_asset={"sol": 0.2},
        liquidity_min_spot_trades_ratio_by_asset={"sol": 0.2},
        liquidity_min_perp_trades_ratio_by_asset={"sol": 0.2},
        liquidity_min_spot_quote_volume_window_by_asset={"sol": 0.0},
        liquidity_min_perp_quote_volume_window_by_asset={"sol": 0.0},
        liquidity_min_spot_trades_window_by_asset={"sol": 0.0},
        liquidity_min_perp_trades_window_by_asset={"sol": 0.0},
    )
    proxy = build_backtest_regime_liquidity_proxy(
        market="sol",
        profile=spec,
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[100, 105, 110, 115, 120, 125],
            trade_values=[10, 10, 11, 11, 12, 12],
        ),
    )
    liquidity_state = proxy.get_status(pd.Timestamp("2026-03-01T00:05:00Z"))

    payload = build_backtest_regime_state(
        market="sol",
        profile=spec,
        features=_sample_features(ret_15m=0.0020, ret_30m=0.0030),
        liquidity_state=liquidity_state,
    )

    assert liquidity_state is not None
    assert liquidity_state.reason_codes == ("ok",)
    assert payload["status"] == "ok"
    assert payload["reason"] == "regime_state_built"
    assert payload["state"] == "NORMAL"
    assert payload["target_state"] == "NORMAL"
    assert payload["pressure"] == "up"
    assert payload["reason_codes"] == ["liquidity_ok"]
    assert payload["liquidity_reason_codes"] == ["ok"]
    assert payload["source_of_truth"] == {
        "liquidity_state_available": True,
        "liquidity_metrics_available": True,
        "feature_returns_available": True,
    }


def test_build_backtest_regime_state_enters_defense_after_confirmations() -> None:
    spec = replace(
        resolve_live_profile_spec("deep_otm"),
        regime_defense_min_dir_prob_boost=0.07,
        regime_defense_disable_offsets=(9, 7),
    )
    liquidity_state = {
        "status": "ok",
        "reason": "spot_quote_ratio",
        "blocked": False,
        "degraded": True,
        "reason_codes": ["spot_quote_ratio", "spot_trades_ratio", "perp_quote_ratio"],
        "metrics": {
            "spot_quote_ratio": 0.30,
            "perp_quote_ratio": 0.30,
            "spot_trades_ratio": 0.30,
            "perp_trades_ratio": 0.30,
            "soft_fail_count": 3.0,
            "hard_fail_count": 0.0,
        },
    }
    features = _sample_features(ret_15m=-0.0020, ret_30m=-0.0030)

    first = build_backtest_regime_state(
        market="sol",
        profile=spec,
        features=features,
        liquidity_state=liquidity_state,
    )
    second = build_backtest_regime_state(
        market="sol",
        profile=spec,
        features=features,
        liquidity_state=liquidity_state,
        previous_state=first,
    )

    assert first["state"] == "NORMAL"
    assert first["target_state"] == "DEFENSE"
    assert first["pending_target"] == "DEFENSE"
    assert first["pending_count"] == 1
    assert second["state"] == "DEFENSE"
    assert second["pressure"] == "down"
    assert second["guard_hints"] == {
        "min_dir_prob_boost": 0.07,
        "disabled_offsets": [7, 9],
        "defense_force_with_pressure": True,
        "defense_max_trades_per_market": 1,
    }


def test_build_backtest_regime_state_returns_disabled_payload_when_profile_disables_controller() -> None:
    payload = build_backtest_regime_state(
        market="btc",
        profile="default",
        features=_sample_features(ret_15m=0.0010, ret_30m=0.0015),
        liquidity_state={
            "status": "ok",
            "reason": "ok",
            "blocked": False,
            "reason_codes": ["ok"],
            "metrics": {"spot_quote_ratio": 1.0},
        },
    )

    assert payload["enabled"] is False
    assert payload["reason"] == "regime_controller_disabled"
    assert payload["reason_codes"] == ["disabled"]
    assert payload["state"] == "NORMAL"
    assert payload["liquidity_state_status"] == "ok"
    assert payload["liquidity_reason_codes"] == ["ok"]


def test_attach_backtest_regime_parity_adds_row_level_state_and_summary() -> None:
    decisions = pd.DataFrame(
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

    out, summary, profile_spec = attach_backtest_regime_parity(
        market="sol",
        profile="deep_otm",
        decisions=decisions,
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[1000, 1000, 1000, 1000, 10, 10, 10, 10],
            trade_values=[100, 100, 100, 100, 1, 1, 1, 1],
        ),
        parity=BacktestParitySpec(
            liquidity_proxy_mode="spot_kline_mirror",
            liquidity_lookback_minutes=2,
            liquidity_baseline_minutes=4,
        ),
    )

    assert profile_spec.profile == "deep_otm"
    assert summary.liquidity_proxy_mode == "spot_kline_mirror"
    assert summary.evaluated_rows == 2
    assert summary.liquidity_available_rows == 2
    assert summary.liquidity_degraded_rows == 2
    assert summary.regime_state_counts["NORMAL"] == 1
    assert summary.regime_state_counts["DEFENSE"] == 1
    assert summary.regime_pressure_counts["down"] == 2
    assert bool(out.loc[0, "liquidity_degraded"]) is True
    assert out.loc[1, "regime_state"] == "DEFENSE"
    assert out.loc[1, "regime_guard_hints"]["defense_force_with_pressure"] is True


def test_attach_backtest_regime_parity_drives_regime_guard_rejects() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "offset": 7,
                "ret_15m": -0.0020,
                "ret_30m": -0.0030,
                "p_up": 0.80,
                "p_down": 0.20,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "ok",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.80,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.79,
            },
            {
                "decision_ts": "2026-03-01T00:06:00Z",
                "offset": 7,
                "ret_15m": -0.0020,
                "ret_30m": -0.0030,
                "p_up": 0.80,
                "p_down": 0.20,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "ok",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.80,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.79,
            },
        ]
    )
    parity = BacktestParitySpec(
        liquidity_proxy_mode="spot_kline_mirror",
        liquidity_lookback_minutes=2,
        liquidity_baseline_minutes=4,
    )

    attached, _summary, profile_spec = attach_backtest_regime_parity(
        market="sol",
        profile="deep_otm",
        decisions=decisions,
        raw_klines=_sample_klines(
            start="2026-03-01T00:00:00Z",
            quote_values=[1000, 1000, 1000, 1000, 10, 10, 10, 10],
            trade_values=[100, 100, 100, 100, 1, 1, 1, 1],
        ),
        parity=parity,
    )
    out, summary = apply_live_guard_parity(
        market="sol",
        profile="deep_otm",
        decisions=attached,
        profile_spec=replace(profile_spec, liquidity_guard_block=True),
    )

    assert attached.loc[1, "regime_state"] == "DEFENSE"
    assert summary.blocked_rows == 2
    assert out.loc[0, "policy_action"] == "reject"
    assert out.loc[0, "guard_primary_reason"] == "ret30m_up_floor"
    assert out.loc[1, "policy_action"] == "reject"
    assert out.loc[1, "guard_primary_reason"] == "regime_direction_pressure"


def test_apply_live_guard_parity_keeps_tail_space_guard_from_row_features() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "offset": 7,
                "p_up": 0.80,
                "p_down": 0.20,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "ok",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.80,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.79,
                "ret_from_strike": -0.02,
                "move_z_strike": 2.5,
            }
        ]
    )

    out, summary = apply_live_guard_parity(
        market="sol",
        profile="deep_otm",
        decisions=decisions,
    )

    assert summary.blocked_rows == 1
    assert out.loc[0, "policy_action"] == "reject"
    assert out.loc[0, "guard_primary_reason"] == "tail_space_too_far"


def test_apply_live_guard_parity_uses_5m_long_return_guard() -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "offset": 2,
                "p_up": 0.20,
                "p_down": 0.80,
                "score_valid": True,
                "score_reason": "",
                "policy_action": "trade",
                "policy_reason": "trade",
                "trade_decision": True,
                "quote_status": "ok",
                "quote_up_ask": 0.71,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.70,
                "quote_down_bid": 0.28,
                "ret_5m": 0.001,
                "ret_15m": 0.02,
            }
        ]
    )

    out, summary = apply_live_guard_parity(
        market="xrp",
        profile="deep_otm_5m",
        decisions=decisions,
    )

    assert summary.blocked_rows == 1
    assert out.loc[0, "policy_action"] == "reject"
    assert out.loc[0, "guard_primary_reason"] == "ret30m_down_ceiling"
