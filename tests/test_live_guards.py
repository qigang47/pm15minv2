from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.live.signal.decision import build_decision_snapshot
from pm15min.live.profiles import resolve_live_profile_spec


def _base_signal_row() -> dict[str, object]:
    return {
        "offset": 7,
        "decision_ts": "2026-03-19T08:23:00+00:00",
        "recommended_side": "DOWN",
        "p_signal": 0.20,
        "p_up_raw": 0.20,
        "p_down_raw": 0.80,
        "p_eff_up": 0.20,
        "p_eff_down": 0.80,
        "p_up_lcb": 0.20,
        "p_up_ucb": 0.20,
        "p_up": 0.20,
        "p_down": 0.80,
        "confidence": 0.80,
        "edge": 0.20,
        "score_valid": True,
        "score_reason": "",
        "coverage": {
            "effective_missing_feature_count": 0,
            "not_allowed_blacklist_count": 0,
        },
        "feature_snapshot": {
            "ret_30m": 0.001,
            "ret_from_strike": -0.002,
            "move_z": 1.0,
        },
    }


def _set_up_interval_signal(
    row: dict[str, object],
    *,
    raw: float = 0.70,
    lcb: float = 0.70,
    eff_up: float | None = None,
    ucb: float | None = None,
) -> None:
    eff_up = lcb if eff_up is None else eff_up
    ucb = raw if ucb is None else ucb
    row["recommended_side"] = "UP"
    row["p_signal"] = raw
    row["p_up_raw"] = raw
    row["p_down_raw"] = 1.0 - raw
    row["p_eff_up"] = eff_up
    row["p_eff_down"] = 1.0 - ucb
    row["p_up_lcb"] = lcb
    row["p_up_ucb"] = ucb
    row["p_up"] = eff_up
    row["p_down"] = 1.0 - ucb
    row["confidence"] = eff_up


def _set_down_interval_signal(
    row: dict[str, object],
    *,
    raw: float = 0.28,
    eff_down: float = 0.72,
    ucb: float | None = None,
) -> None:
    ucb = raw if ucb is None else ucb
    row["recommended_side"] = "DOWN"
    row["p_signal"] = raw
    row["p_up_raw"] = raw
    row["p_down_raw"] = 1.0 - raw
    row["p_eff_up"] = raw
    row["p_eff_down"] = eff_down
    row["p_up_lcb"] = raw
    row["p_up_ucb"] = ucb
    row["p_up"] = raw
    row["p_down"] = eff_down
    row["confidence"] = eff_down


def test_decision_rejects_when_ret_30m_guard_fails() -> None:
    row = _base_signal_row()
    row["feature_snapshot"] = {
        "ret_30m": 0.02,
        "ret_from_strike": -0.002,
        "move_z": 1.0,
    }
    payload = {
        "market": "xrp",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    out = build_decision_snapshot(payload)
    assert out["decision"]["status"] == "reject"
    assert out["rejected_offsets"][0]["guard_reasons"] == ["ret30m_down_ceiling"]


def test_deep_otm_baseline_ret30_thresholds_match_tightened_btc_eth_policy() -> None:
    spec = resolve_live_profile_spec("deep_otm_baseline")

    assert spec.tail_space_guard_enabled is False
    assert spec.min_net_edge_for(offset=7, entry_price=0.30) == 0.0
    assert spec.min_net_edge_for(offset=8, entry_price=0.10) == 0.0
    assert spec.min_net_edge_for(offset=9, entry_price=0.05) == 0.0
    assert spec.ret_30m_up_floor_for("btc") == 0.002
    assert spec.ret_30m_up_floor_for("eth") == 0.0015
    assert spec.ret_30m_up_floor_for("xrp") == -0.04
    assert spec.ret_30m_down_ceiling_for("btc") == 0.0
    assert spec.ret_30m_down_ceiling_for("eth") == 0.0
    assert spec.ret_30m_down_ceiling_for("xrp") == 0.009
    assert spec.ret_30m_down_ceiling_for("sol") == 0.002


def test_decision_rejects_when_tail_space_guard_fails() -> None:
    row = _base_signal_row()
    _set_up_interval_signal(row)
    row["feature_snapshot"] = {
        "ret_30m": 0.0,
        "ret_from_strike": -0.01,
        "move_z": 3.0,
    }
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    out = build_decision_snapshot(payload)
    assert out["decision"]["status"] == "reject"
    assert "tail_space_too_far" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_rejects_when_trade_side_blocked_by_env(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_ALLOWED_TRADE_SIDES", "DOWN")
    row = _base_signal_row()
    _set_up_interval_signal(row)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "reject"
    assert "trade_side_blocked" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_applies_market_scoped_trade_side_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_ALLOWED_TRADE_SIDES", "DOWN")
    monkeypatch.setenv("PM15MIN_ALLOWED_TRADE_SIDES_SOL", "UP")
    row = _base_signal_row()
    _set_up_interval_signal(row)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }

    allowed = build_decision_snapshot(payload, quote_payload)
    blocked = build_decision_snapshot({**payload, "market": "xrp"}, quote_payload)

    assert allowed["decision"]["status"] == "accept"
    assert blocked["decision"]["status"] == "reject"
    assert "trade_side_blocked" in blocked["rejected_offsets"][0]["guard_reasons"]


def test_decision_snapshot_preserves_signal_freshness_metadata() -> None:
    row = _base_signal_row()
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "latest_feature_decision_ts": "2026-03-19T08:23:00+00:00",
        "builder_feature_set": "v6_user_core",
        "feature_rows": 384,
        "offset_signals": [row],
    }
    out = build_decision_snapshot(payload)
    assert out["latest_feature_decision_ts"] == "2026-03-19T08:23:00+00:00"
    assert out["builder_feature_set"] == "v6_user_core"
    assert out["feature_rows"] == 384


def test_decision_rejects_when_entry_price_band_fails() -> None:
    row = _base_signal_row()
    _set_down_interval_signal(row, raw=0.20, eff_down=0.80)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.40,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.39,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "reject"
    assert "entry_price_max" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_rejects_when_down_ucb_is_not_below_threshold() -> None:
    row = _base_signal_row()
    _set_down_interval_signal(row, raw=0.41, eff_down=0.59, ucb=0.41)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.58,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "reject"
    assert "up_ucb_above_threshold" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_accepts_when_quote_guards_pass() -> None:
    row = _base_signal_row()
    _set_down_interval_signal(row, raw=0.28, eff_down=0.72, ucb=0.28)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "accept"
    assert "entry_price_band" in out["applied_guard_layers"]
    assert "probability_interval_threshold" in out["applied_guard_layers"]
    assert out["decision"]["selected_entry_price"] == 0.29
    assert out["decision"]["selected_trigger_metric"] == "p_up_ucb"
    assert out["accepted_offsets"][0]["trigger_probability"] == 0.28
    assert out["accepted_offsets"][0]["quote_metrics"]["roi_net_vs_quote"] > 0.0


def test_decision_rejects_up_when_lcb_is_not_above_threshold() -> None:
    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.68, lcb=0.60, eff_up=0.60, ucb=0.68)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.05,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.04,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "reject"
    assert "up_lcb_below_threshold" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_keeps_up_side_when_up_interval_passes_even_if_down_is_cheaper(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_ALLOWED_TRADE_SIDES", raising=False)
    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.70, lcb=0.65, eff_up=0.65, ucb=0.70)
    row["edge"] = 0.07
    row["feature_snapshot"] = {
        "ret_30m": 0.0,
        "ret_from_strike": 0.0,
        "move_z": 1.0,
    }
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.29,
                "quote_down_ask": 0.05,
                "quote_up_bid": 0.28,
                "quote_down_bid": 0.04,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(payload, quote_payload)

    assert out["decision"]["status"] == "accept"
    assert out["decision"]["selected_side"] == "UP"
    assert out["accepted_offsets"][0]["recommended_side"] == "UP"
    assert out["accepted_offsets"][0]["model_recommended_side"] == "UP"
    assert out["accepted_offsets"][0]["trigger_side"] == "UP"
    assert out["accepted_offsets"][0]["trigger_metric"] == "p_up_lcb"
    assert out["accepted_offsets"][0]["trigger_probability"] == 0.65
    assert out["accepted_offsets"][0]["quote_metrics"]["roi_net_vs_quote"] > 0.0


def test_decision_deep_otm_uses_down_confidence_to_buy_down_side(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_ALLOWED_TRADE_SIDES", raising=False)
    row = _base_signal_row()
    _set_down_interval_signal(row, raw=0.28, eff_down=0.72, ucb=0.28)
    row["edge"] = 0.44
    row["feature_snapshot"] = {
        "ret_30m": 0.0,
        "ret_from_strike": 0.0,
        "move_z": 1.0,
    }
    payload = {
        "market": "xrp",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.74,
                "quote_down_ask": 0.05,
                "quote_up_bid": 0.73,
                "quote_down_bid": 0.04,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(payload, quote_payload)

    assert out["decision"]["status"] == "accept"
    assert out["decision"]["selected_side"] == "DOWN"
    assert out["accepted_offsets"][0]["recommended_side"] == "DOWN"
    assert out["accepted_offsets"][0]["model_recommended_side"] == "DOWN"
    assert out["accepted_offsets"][0]["trigger_side"] == "DOWN"
    assert out["accepted_offsets"][0]["trigger_metric"] == "p_up_ucb"
    assert out["accepted_offsets"][0]["trigger_probability"] == 0.28


def test_decision_dual_candidate_falls_back_without_explicit_side_probabilities() -> None:
    row = _base_signal_row()
    for key in (
        "p_signal",
        "p_up_raw",
        "p_down_raw",
        "p_eff_up",
        "p_eff_down",
        "p_up_lcb",
        "p_up_ucb",
        "p_up",
        "p_down",
    ):
        row.pop(key, None)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(payload, quote_payload)

    assert out["decision"]["status"] == "accept"
    assert out["decision"]["selected_side"] == "DOWN"
    assert out["accepted_offsets"][0]["recommended_side"] == "DOWN"
    assert out["accepted_offsets"][0]["model_recommended_side"] == "DOWN"
    assert out["accepted_offsets"][0]["opposite_probability"] is None


def test_decision_rejects_when_liquidity_guard_blocks() -> None:
    blocked_spec = replace(resolve_live_profile_spec("deep_otm"), liquidity_guard_block=True)
    original_resolver = build_decision_snapshot.__globals__["resolve_live_profile_spec"]
    build_decision_snapshot.__globals__["resolve_live_profile_spec"] = lambda profile: blocked_spec
    try:
        row = _base_signal_row()
        row["p_down"] = 0.70
        payload = {
            "market": "sol",
            "profile": "deep_otm",
            "cycle": "15m",
            "target": "direction",
            "active_bundle": {},
            "active_bundle_selection_path": "/tmp/selection.json",
            "snapshot_ts": "2026-03-19T15-00-00Z",
            "liquidity_state": {
                "status": "ok",
                "reason": "spot_quote_window",
                "guard_enabled": True,
                "ok": False,
                "blocked": True,
                "reason_codes": ["spot_quote_window"],
            },
            "offset_signals": [row],
        }
        quote_payload = {
            "snapshot_ts": "2026-03-19T15-00-10Z",
            "quote_rows": [
                {
                    "offset": 7,
                    "status": "ok",
                    "market_id": "market-1",
                    "quote_up_ask": 0.20,
                    "quote_down_ask": 0.29,
                    "quote_up_bid": 0.19,
                    "quote_down_bid": 0.28,
                    "reasons": [],
                }
            ],
        }
        out = build_decision_snapshot(payload, quote_payload)
    finally:
        build_decision_snapshot.__globals__["resolve_live_profile_spec"] = original_resolver
    assert out["decision"]["status"] == "reject"
    assert "liquidity_guard" in out["applied_guard_layers"]
    assert "liquidity_guard_blocked" in out["rejected_offsets"][0]["guard_reasons"]
    assert "liquidity_spot_quote_window" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_rejects_when_regime_direction_pressure_blocks() -> None:
    blocked_spec = replace(resolve_live_profile_spec("deep_otm"), liquidity_guard_block=True)
    original_resolver = build_decision_snapshot.__globals__["resolve_live_profile_spec"]
    build_decision_snapshot.__globals__["resolve_live_profile_spec"] = lambda profile: blocked_spec
    try:
        row = _base_signal_row()
        row["p_down"] = 0.70
        payload = {
            "market": "sol",
            "profile": "deep_otm",
            "cycle": "15m",
            "target": "direction",
            "active_bundle": {},
            "active_bundle_selection_path": "/tmp/selection.json",
            "snapshot_ts": "2026-03-19T15-00-00Z",
            "regime_state": {
                "status": "ok",
                "reason": "regime_state_built",
                "enabled": True,
                "state": "DEFENSE",
                "target_state": "DEFENSE",
                "pressure": "up",
                "reason_codes": ["liquidity_blocked"],
                "guard_hints": {
                    "min_dir_prob_boost": 0.0,
                    "disabled_offsets": [],
                    "defense_force_with_pressure": True,
                },
            },
            "offset_signals": [row],
        }
        quote_payload = {
            "snapshot_ts": "2026-03-19T15-00-10Z",
            "quote_rows": [
                {
                    "offset": 7,
                    "status": "ok",
                    "market_id": "market-1",
                    "quote_up_ask": 0.20,
                    "quote_down_ask": 0.29,
                    "quote_up_bid": 0.19,
                    "quote_down_bid": 0.28,
                    "reasons": [],
                }
            ],
        }
        out = build_decision_snapshot(payload, quote_payload)
    finally:
        build_decision_snapshot.__globals__["resolve_live_profile_spec"] = original_resolver
    assert out["decision"]["status"] == "reject"
    assert "regime_controller" in out["applied_guard_layers"]
    assert "regime_direction_pressure" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_does_not_reject_when_liquidity_blocking_disabled(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_ALLOWED_TRADE_SIDES", raising=False)
    monkeypatch.delenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", raising=False)
    monkeypatch.delenv("PM15MIN_MAX_TRADES_PER_MARKET", raising=False)
    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.80, lcb=0.80, eff_up=0.80, ucb=0.80)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "liquidity_state": {
            "status": "ok",
            "reason": "spot_quote_window",
            "guard_enabled": True,
            "ok": False,
            "blocked": True,
            "reason_codes": ["spot_quote_window"],
        },
        "regime_state": {
            "status": "ok",
            "reason": "regime_state_built",
            "enabled": True,
            "state": "DEFENSE",
            "target_state": "DEFENSE",
            "pressure": "down",
            "reason_codes": ["liquidity_blocked"],
            "guard_hints": {
                "min_dir_prob_boost": 0.0,
                "disabled_offsets": [],
                "defense_force_with_pressure": True,
            },
        },
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "accept"
    guard_reasons = out["accepted_offsets"][0]["guard_reasons"]
    assert "liquidity_guard_blocked" not in guard_reasons
    assert "regime_direction_pressure" not in guard_reasons


def test_decision_rejects_when_env_trade_count_cap_hits(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "1")

    row = _base_signal_row()
    row["p_down"] = 0.70
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={"market_offset_trade_count": {"market-1_7": 1}},
    )

    assert out["decision"]["status"] == "reject"
    assert "trade_count_cap" in out["applied_guard_layers"]
    assert out["rejected_offsets"][0]["guard_reasons"] == ["max_trades_per_offset"]
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count"] == 1
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_effective"] == 1
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_source"] == "PM15MIN_MAX_TRADES_PER_MARKET_SOL"
    monkeypatch.delenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", raising=False)


def test_decision_rejects_when_repeat_same_decision_cap_hits_by_default(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    row = _base_signal_row()
    row["p_down"] = 0.70
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={"market_offset_trade_count": {"market-1_7": 1}},
    )

    assert out["decision"]["status"] == "reject"
    assert out["rejected_offsets"][0]["guard_reasons"] == ["max_trades_per_offset"]
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count"] == 1
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count_lock_side"] is False
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_effective"] == 1
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_source"] == "repeat_same_decision_max_trades"


def test_decision_rejects_when_repeat_same_decision_cap_hits_per_side(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    base_spec = resolve_live_profile_spec("deep_otm")
    side_locked_spec = replace(base_spec, repeat_same_decision_lock_side=True)
    monkeypatch.setattr("pm15min.live.signal.decision.resolve_live_profile_spec", lambda profile: side_locked_spec)

    row = _base_signal_row()
    row["p_down"] = 0.70
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={
            "market_offset_trade_count": {"market-1_7": 1},
            "market_offset_side_trade_count": {
                "market-1_7_DOWN": 1,
                "market-1_7_UP": 0,
            },
        },
    )

    assert out["decision"]["status"] == "reject"
    assert out["rejected_offsets"][0]["guard_reasons"] == ["max_trades_per_offset"]
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count"] == 1
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count_key"] == "market-1_7_DOWN"
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count_lock_side"] is True


def test_decision_allows_opposite_side_when_repeat_same_decision_cap_is_side_locked(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_ALLOWED_TRADE_SIDES", raising=False)
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    base_spec = resolve_live_profile_spec("deep_otm")
    side_locked_spec = replace(base_spec, repeat_same_decision_lock_side=True)
    monkeypatch.setattr("pm15min.live.signal.decision.resolve_live_profile_spec", lambda profile: side_locked_spec)

    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.70, lcb=0.70, eff_up=0.70, ucb=0.70)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.29,
                "quote_down_ask": 0.20,
                "quote_up_bid": 0.28,
                "quote_down_bid": 0.19,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={"market_offset_trade_count": {"market-1_7": 3}},
    )

    assert out["decision"]["status"] == "accept"
    assert out["accepted_offsets"][0]["account_context"]["session_trade_count"] == 0
    assert out["accepted_offsets"][0]["account_context"]["session_trade_count_key"] == "market-1_7_UP"
    assert out["accepted_offsets"][0]["account_context"]["session_trade_count_lock_side"] is True
    assert out["accepted_offsets"][0]["account_context"]["session_trade_count_scope"] == "market_offset_side"


def test_decision_repeat_same_decision_lock_side_uses_side_counts_when_available(monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_ALLOWED_TRADE_SIDES", raising=False)
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    base_spec = resolve_live_profile_spec("deep_otm")
    side_locked_spec = replace(base_spec, repeat_same_decision_lock_side=True)
    monkeypatch.setattr("pm15min.live.signal.decision.resolve_live_profile_spec", lambda profile: side_locked_spec)
    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.70, lcb=0.70, eff_up=0.70, ucb=0.70)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.29,
                "quote_down_ask": 0.20,
                "quote_up_bid": 0.28,
                "quote_down_bid": 0.19,
                "reasons": [],
            }
        ],
    }

    rejected = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={"market_offset_side_trade_count": {"market-1_7_UP": 1}},
    )

    assert rejected["decision"]["status"] == "reject"
    rejected_up = next(row for row in rejected["rejected_offsets"] if row.get("recommended_side") == "UP")
    assert rejected_up["account_context"]["session_trade_count"] == 1
    assert rejected_up["account_context"]["session_trade_count_key"] == "market-1_7_UP"
    assert rejected_up["account_context"]["session_trade_count_scope"] == "market_offset_side"

    _set_down_interval_signal(row, raw=0.30, eff_down=0.70, ucb=0.30)
    accepted = build_decision_snapshot(
        payload,
        quote_payload,
        session_state={"market_offset_side_trade_count": {"market-1_7_UP": 1}},
    )

    assert accepted["decision"]["status"] == "accept"
    assert accepted["accepted_offsets"][0]["account_context"]["session_trade_count"] == 0
    assert accepted["accepted_offsets"][0]["account_context"]["session_trade_count_key"] == "market-1_7_DOWN"
    assert accepted["accepted_offsets"][0]["account_context"]["session_trade_count_scope"] == "market_offset_side"


def test_decision_rejects_when_regime_trade_count_cap_hits(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    base_spec = resolve_live_profile_spec("deep_otm")
    capped_spec = replace(base_spec, repeat_same_decision_max_trades=3, regime_defense_max_trades_per_market=1)
    monkeypatch.setattr("pm15min.live.signal.decision.resolve_live_profile_spec", lambda profile: capped_spec)

    row = _base_signal_row()
    row["p_down"] = 0.70
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "regime_state": {
            "status": "ok",
            "reason": "regime_state_built",
            "enabled": True,
            "state": "DEFENSE",
            "target_state": "DEFENSE",
            "pressure": "neutral",
            "reason_codes": ["liquidity_blocked"],
            "guard_hints": {
                "min_dir_prob_boost": 0.0,
                "disabled_offsets": [],
                "defense_force_with_pressure": True,
                "defense_max_trades_per_market": 1,
            },
        },
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    account_state_payload = {
        "snapshot_ts": "2026-03-19T15-00-05Z",
        "open_orders": {"status": "ok", "orders": []},
        "positions": {
            "status": "ok",
            "positions": [],
        },
    }

    out = build_decision_snapshot(
        payload,
        quote_payload,
        account_state_payload,
        session_state={"market_offset_trade_count": {"market-1_7": 1}},
    )
    assert out["decision"]["status"] == "reject"
    assert "trade_count_cap" in out["applied_guard_layers"]
    assert "max_trades_per_offset" in out["rejected_offsets"][0]["guard_reasons"]
    assert "regime_trade_count_cap" in out["rejected_offsets"][0]["guard_reasons"]
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count"] == 1
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_base"] == 3
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_effective"] == 1


def test_decision_rejects_when_cash_balance_stop_hits(monkeypatch) -> None:
    base_spec = resolve_live_profile_spec("deep_otm")
    guarded_spec = replace(base_spec, stop_trading_below_cash_usd=100.0)
    monkeypatch.setattr("pm15min.live.signal.decision.resolve_live_profile_spec", lambda profile: guarded_spec)

    row = _base_signal_row()
    row["p_down"] = 0.70
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    account_state_payload = {
        "snapshot_ts": "2026-03-19T15-00-05Z",
        "open_orders": {"status": "ok", "orders": []},
        "positions": {"status": "ok", "positions": [], "cash_balance_usd": 80.0, "cash_balance_status": "ok"},
        "summary": {"cash_balance_usd": 80.0, "cash_balance_available": True},
    }

    out = build_decision_snapshot(payload, quote_payload, account_state_payload)

    assert out["decision"]["status"] == "reject"
    assert "cash_balance_guard" in out["applied_guard_layers"]
    assert "cash_balance_stop" in out["rejected_offsets"][0]["guard_reasons"]
    assert out["rejected_offsets"][0]["account_context"]["cash_balance_usd"] == 80.0


def test_decision_uses_compact_account_summary_without_raw_positions() -> None:
    row = _base_signal_row()
    _set_up_interval_signal(row, raw=0.80, lcb=0.80, eff_up=0.80, ucb=0.80)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-19T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-19T15-00-10Z",
        "quote_rows": [
            {
                "offset": 7,
                "status": "ok",
                "market_id": "market-1",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.29,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.28,
                "reasons": [],
            }
        ],
    }
    account_state_payload = {
        "snapshot_ts": "2026-03-19T15-00-05Z",
        "open_orders": {
            "status": "ok",
            "summary": {
                "total_orders": 1,
                "market_ids": ["market-1"],
                "by_market_id": {"market-1": 1},
            },
        },
        "positions": {
            "status": "ok",
            "summary": {
                "total_positions": 1,
                "market_ids": ["market-1"],
                "by_market_id": {"market-1": 1},
            },
            "cash_balance_usd": 250.0,
            "cash_balance_status": "ok",
        },
        "summary": {
            "cash_balance_usd": 250.0,
            "cash_balance_available": True,
            "active_market_ids": ["market-1"],
            "active_market_count": 1,
        },
    }

    out = build_decision_snapshot(payload, quote_payload, account_state_payload)

    selected = (out["accepted_offsets"] or out["rejected_offsets"])[0]
    assert selected["account_context"]["open_orders_count"] == 1
    assert selected["account_context"]["position_count"] == 1
    assert selected["account_context"]["current_market_active"] is True


def test_decision_rejects_when_depth_fill_ratio_blocks_trade(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_LIVE_DECISION_DEPTH_ENFORCED", "1")
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-down",
                "side": "down",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 1.0], [0.31, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    row = _base_signal_row()
    row["offset"] = 8
    row["decision_ts"] = "2026-03-20T00:08:00+00:00"
    row["p_down"] = 0.80
    payload = {
        "market": "xrp",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-20T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-20T15-00-10Z",
        "quote_rows": [
            {
                "offset": 8,
                "status": "ok",
                "market_id": "market-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.19,
                "quote_down_ask_size_1": 1.0,
                "quote_captured_ts_ms_down": captured_ts_ms,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(payload, quote_payload, rewrite_root=root)

    assert out["decision"]["status"] == "reject"
    assert "depth_fill_ratio_below_threshold" in out["rejected_offsets"][0]["guard_reasons"]
    assert out["rejected_offsets"][0]["quote_metrics"]["entry_price_l1"] == 0.20
    assert out["rejected_offsets"][0]["quote_metrics"]["entry_price"] == 0.20
    assert out["rejected_offsets"][0]["quote_metrics"]["depth_plan"]["status"] == "blocked"


def test_decision_depth_enforced_defaults_on_when_env_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("PM15MIN_LIVE_DECISION_DEPTH_ENFORCED", raising=False)
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-down",
                "side": "down",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 1.0], [0.31, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    row = _base_signal_row()
    row["offset"] = 8
    row["decision_ts"] = "2026-03-20T00:08:00+00:00"
    row["p_down"] = 0.80
    payload = {
        "market": "xrp",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "active_bundle": {},
        "active_bundle_selection_path": "/tmp/selection.json",
        "snapshot_ts": "2026-03-20T15-00-00Z",
        "offset_signals": [row],
    }
    quote_payload = {
        "snapshot_ts": "2026-03-20T15-00-10Z",
        "quote_rows": [
            {
                "offset": 8,
                "status": "ok",
                "market_id": "market-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_up_ask": 0.20,
                "quote_down_ask": 0.20,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.19,
                "quote_down_ask_size_1": 1.0,
                "quote_captured_ts_ms_down": captured_ts_ms,
                "reasons": [],
            }
        ],
    }

    out = build_decision_snapshot(payload, quote_payload, rewrite_root=root)

    assert out["decision"]["status"] == "reject"
    assert "depth_fill_ratio_below_threshold" in out["rejected_offsets"][0]["guard_reasons"]
    assert out["rejected_offsets"][0]["quote_metrics"]["depth_enforced"] is True
    assert out["rejected_offsets"][0]["quote_metrics"]["depth_plan"]["status"] == "blocked"


def test_resolve_live_profile_spec_applies_cash_stop_env_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_STOP_TRADING_BELOW_CASH_USD", "100")
    spec = resolve_live_profile_spec("deep_otm")
    assert spec.stop_trading_below_cash_usd == 100.0
    monkeypatch.delenv("PM15MIN_STOP_TRADING_BELOW_CASH_USD", raising=False)
