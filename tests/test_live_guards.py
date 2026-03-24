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


def test_decision_rejects_when_tail_space_guard_fails() -> None:
    row = _base_signal_row()
    row["recommended_side"] = "UP"
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


def test_decision_rejects_when_entry_price_band_fails() -> None:
    row = _base_signal_row()
    row["p_down"] = 0.80
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


def test_decision_rejects_when_net_edge_vs_quote_fails() -> None:
    row = _base_signal_row()
    row["p_down"] = 0.59
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
                "quote_down_ask": 0.585,
                "quote_up_bid": 0.19,
                "quote_down_bid": 0.58,
                "reasons": [],
            }
        ],
    }
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "reject"
    assert "net_edge_below_quote_threshold" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_accepts_when_quote_guards_pass() -> None:
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
    out = build_decision_snapshot(payload, quote_payload)
    assert out["decision"]["status"] == "accept"
    assert "entry_price_band" in out["applied_guard_layers"]
    assert "net_edge_vs_quote" in out["applied_guard_layers"]
    assert "roi_threshold_vs_quote" in out["applied_guard_layers"]
    assert out["decision"]["selected_entry_price"] == 0.29
    assert out["accepted_offsets"][0]["quote_metrics"]["roi_net_vs_quote"] > 0.0


def test_decision_rejects_when_liquidity_guard_blocks() -> None:
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
    assert out["decision"]["status"] == "reject"
    assert "liquidity_guard" in out["applied_guard_layers"]
    assert "liquidity_guard_blocked" in out["rejected_offsets"][0]["guard_reasons"]
    assert "liquidity_spot_quote_window" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_rejects_when_regime_direction_pressure_blocks() -> None:
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
    assert out["decision"]["status"] == "reject"
    assert "regime_controller" in out["applied_guard_layers"]
    assert "regime_direction_pressure" in out["rejected_offsets"][0]["guard_reasons"]


def test_decision_rejects_when_env_trade_count_cap_hits(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "3")

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
        session_state={"market_offset_trade_count": {"market-1_7": 3}},
    )

    assert out["decision"]["status"] == "reject"
    assert "trade_count_cap" in out["applied_guard_layers"]
    assert out["rejected_offsets"][0]["guard_reasons"] == ["max_trades_per_offset"]
    assert out["rejected_offsets"][0]["account_context"]["session_trade_count"] == 3
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_effective"] == 3
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_source"] == "PM15MIN_MAX_TRADES_PER_MARKET_SOL"
    monkeypatch.delenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", raising=False)


def test_decision_rejects_when_regime_trade_count_cap_hits(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET_SOL", "")
    monkeypatch.setenv("PM15MIN_MAX_TRADES_PER_MARKET", "")
    base_spec = resolve_live_profile_spec("deep_otm")
    capped_spec = replace(base_spec, regime_defense_max_trades_per_market=1)
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
    assert out["rejected_offsets"][0]["account_context"]["max_trades_per_market_base"] == 0
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


def test_decision_rejects_when_depth_fill_ratio_blocks_trade(tmp_path: Path) -> None:
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


def test_resolve_live_profile_spec_applies_cash_stop_env_override(monkeypatch) -> None:
    monkeypatch.setenv("PM15MIN_STOP_TRADING_BELOW_CASH_USD", "100")
    spec = resolve_live_profile_spec("deep_otm")
    assert spec.stop_trading_below_cash_usd == 100.0
    monkeypatch.delenv("PM15MIN_STOP_TRADING_BELOW_CASH_USD", raising=False)
