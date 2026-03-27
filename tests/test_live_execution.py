from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.io.json_files import write_json_atomic
from pm15min.live.account import persist_open_orders_snapshot, persist_positions_snapshot
from pm15min.live.execution import build_execution_snapshot
from pm15min.live.profiles import resolve_live_profile_spec


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_execution_snapshot_no_action_when_decision_rejects(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "reject", "selected_offset": None},
        "accepted_offsets": [],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "no_action"
    assert out["execution"]["reason"] == "decision_reject"
    assert out["execution"]["retry_policy"]["status"] == "inactive"


def test_execution_snapshot_prefers_local_latest_full_depth_snapshot(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    write_json_atomic(
        {
            "dataset": "orderbook_latest_full_snapshot",
            "market": "sol",
            "cycle": "15m",
            "captured_ts_ms": captured_ts_ms,
            "records": [
                {
                    "captured_ts_ms": captured_ts_ms,
                    "source_ts_ms": captured_ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "asset": "sol",
                    "cycle": "15m",
                    "asks": [{"price": 0.20, "size": 1.0}, {"price": 0.2005, "size": 5.0}],
                    "bids": [{"price": 0.19, "size": 2.0}],
                    "source": "clob",
                }
            ],
        },
        data_cfg.layout.orderbook_latest_full_snapshot_path,
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "plan"
    assert out["execution"]["depth_plan"]["status"] == "ok"
    assert out["execution"]["depth_plan"]["depth_source_kind"] == "local_latest"


def test_execution_snapshot_reuses_precomputed_depth_plan(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    price_cap = 0.8 / 1.01
    calls = {"repriced": 0}

    def _should_not_run(*args, **kwargs):
        raise AssertionError("depth plan should be reused from decision payload")

    def _recomputed_repriced(*, spec, selected_row, repriced_entry_price):
        calls["repriced"] += 1
        return (
            {
                "repriced_entry_price": float(repriced_entry_price),
                "repriced_effective_price": float(repriced_entry_price),
                "repriced_fee_rate": 0.02,
                "repriced_raw_edge": 0.55,
                "repriced_min_net_edge_required": 0.01,
                "repriced_roi_net": 2.75,
                "repriced_roi_threshold_required": 0.0,
            },
            [],
        )

    monkeypatch.setattr("pm15min.live.execution.build_depth_execution_plan", _should_not_run)
    monkeypatch.setattr("pm15min.live.execution.repriced_order_guard", _recomputed_repriced)

    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_side": "UP",
                    "entry_price": 0.2005,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                    "depth_enforced": True,
                    "requested_notional_usd": 1.0,
                    "price_cap": price_cap,
                    "depth_reason": None,
                    "depth_plan": {
                        "status": "ok",
                        "max_price": 0.2005,
                        "fill_ratio": 1.0,
                        "filled_shares": 4.987531172069826,
                    },
                    "repriced_metrics": {
                        "repriced_entry_price": 0.2005,
                        "repriced_effective_price": 0.2005,
                        "repriced_fee_rate": 0.01,
                        "repriced_raw_edge": 0.5995,
                        "repriced_min_net_edge_required": 0.01,
                        "repriced_roi_net": 2.99002493765586,
                        "repriced_roi_threshold_required": 0.0,
                    },
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload)

    assert out["execution"]["status"] == "plan"
    assert out["execution"]["depth_plan"]["status"] == "ok"
    assert out["execution"]["depth_plan_reused"] is True
    assert out["execution"]["repriced_metrics"]["repriced_entry_price"] == 0.2005
    assert out["execution"]["repriced_metrics"]["repriced_fee_rate"] == 0.02
    assert calls == {"repriced": 1}


def test_execution_snapshot_blocks_when_reused_depth_plan_fails_repriced_guard(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    price_cap = 0.8 / 1.01

    def _should_not_run(*args, **kwargs):
        raise AssertionError("depth plan should be reused from decision payload")

    def _blocked_repriced(*, spec, selected_row, repriced_entry_price):
        return (
            {
                "repriced_entry_price": float(repriced_entry_price),
                "repriced_effective_price": float(repriced_entry_price),
                "repriced_fee_rate": 0.01,
                "repriced_raw_edge": 0.05,
                "repriced_min_net_edge_required": 0.10,
                "repriced_roi_net": -0.02,
                "repriced_roi_threshold_required": 0.0,
            },
            ["repriced_net_edge_below_threshold", "repriced_roi_below_threshold"],
        )

    monkeypatch.setattr("pm15min.live.execution.build_depth_execution_plan", _should_not_run)
    monkeypatch.setattr("pm15min.live.execution.repriced_order_guard", _blocked_repriced)

    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_side": "UP",
                    "entry_price": 0.2005,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                    "depth_enforced": True,
                    "requested_notional_usd": 1.0,
                    "price_cap": price_cap,
                    "depth_reason": None,
                    "depth_plan": {
                        "status": "ok",
                        "max_price": 0.2005,
                        "fill_ratio": 1.0,
                        "filled_shares": 4.987531172069826,
                    },
                    "repriced_metrics": {
                        "repriced_entry_price": 0.2005,
                        "repriced_effective_price": 0.2005,
                        "repriced_fee_rate": 0.01,
                        "repriced_raw_edge": 0.5995,
                        "repriced_min_net_edge_required": 0.01,
                        "repriced_roi_net": 2.99002493765586,
                        "repriced_roi_threshold_required": 0.0,
                    },
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload)

    assert out["execution"]["status"] == "blocked"
    assert out["execution"]["depth_plan_reused"] is True
    assert "repriced_net_edge_below_threshold" in out["execution"]["execution_reasons"]
    assert "repriced_roi_below_threshold" in out["execution"]["execution_reasons"]


def test_execution_snapshot_recomputes_when_cached_depth_plan_is_blocked(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls = {"depth": 0, "repriced": 0}

    def _recomputed_depth_plan(**kwargs):
        calls["depth"] += 1
        return (
            {
                "status": "ok",
                "max_price": 0.2005,
                "fill_ratio": 1.0,
                "filled_shares": 4.987531172069826,
            },
            None,
        )

    def _recomputed_repriced(*, spec, selected_row, repriced_entry_price):
        calls["repriced"] += 1
        return (
            {
                "repriced_entry_price": float(repriced_entry_price),
                "repriced_effective_price": float(repriced_entry_price),
                "repriced_fee_rate": 0.01,
                "repriced_raw_edge": 0.5995,
                "repriced_min_net_edge_required": 0.01,
                "repriced_roi_net": 2.99002493765586,
                "repriced_roi_threshold_required": 0.0,
            },
            [],
        )

    monkeypatch.setattr("pm15min.live.execution.build_depth_execution_plan", _recomputed_depth_plan)
    monkeypatch.setattr("pm15min.live.execution.repriced_order_guard", _recomputed_repriced)

    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_side": "UP",
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                    "depth_enforced": True,
                    "requested_notional_usd": 1.0,
                    "price_cap": 0.8 / 1.01,
                    "depth_reason": "depth_fill_unavailable",
                    "depth_plan": {
                        "status": "blocked",
                        "max_price": None,
                        "fill_ratio": 0.0,
                    },
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload)

    assert out["execution"]["status"] == "plan"
    assert out["execution"]["depth_plan_reused"] is False
    assert calls == {"depth": 1, "repriced": 1}


def test_execution_snapshot_does_not_reuse_cached_depth_plan_when_live_depth_is_preferred(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls = {"depth": 0, "repriced": 0}

    def _fresh_depth_plan(**kwargs):
        calls["depth"] += 1
        return (
            {
                "status": "ok",
                "max_price": 0.201,
                "fill_ratio": 1.0,
                "filled_shares": 4.975124378109452,
            },
            None,
        )

    def _recomputed_repriced(*, spec, selected_row, repriced_entry_price):
        calls["repriced"] += 1
        return (
            {
                "repriced_entry_price": float(repriced_entry_price),
                "repriced_effective_price": float(repriced_entry_price),
                "repriced_fee_rate": 0.01,
                "repriced_raw_edge": 0.599,
                "repriced_min_net_edge_required": 0.01,
                "repriced_roi_net": 2.97,
                "repriced_roi_threshold_required": 0.0,
            },
            [],
        )

    monkeypatch.setattr("pm15min.live.execution.build_depth_execution_plan", _fresh_depth_plan)
    monkeypatch.setattr("pm15min.live.execution.repriced_order_guard", _recomputed_repriced)

    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_side": "UP",
                    "entry_price": 0.2005,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                    "depth_enforced": True,
                    "requested_notional_usd": 1.0,
                    "price_cap": 0.8 / 1.01,
                    "depth_reason": None,
                    "depth_plan": {
                        "status": "ok",
                        "max_price": 0.2005,
                        "fill_ratio": 1.0,
                        "filled_shares": 4.987531172069826,
                    },
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload, prefer_live_depth=True)

    assert out["execution"]["status"] == "plan"
    assert out["execution"]["depth_plan_reused"] is False
    assert out["execution"]["depth_plan"]["max_price"] == 0.201
    assert calls == {"depth": 1, "repriced": 1}


def test_execution_snapshot_blocks_when_depth_fill_ratio_is_too_small(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 1.0]],
                "bids": [[0.19, 1.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 1.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "blocked"
    assert "depth_fill_ratio_below_threshold" in out["execution"]["execution_reasons"]
    assert out["execution"]["retry_policy"]["status"] == "inactive"
    assert out["execution"]["retry_policy"]["reason"] == "depth_fill_ratio_below_threshold"
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["enabled"] is False
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["retry_interval_sec"] == 0.0
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["max_retries"] == 0
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["retry_state_key"] == ""
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["trigger_statuses"] == []
    assert out["execution"]["retry_policy"]["pre_submit_depth_retry"]["mode"] == "runner_loop_window_rechecks"


def test_execution_snapshot_prefers_provider_for_time_bound_live_decision(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    class _Provider:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls.append(str(token_id))
            return {
                "asks": [{"price": "0.20", "size": "10"}],
                "bids": [{"price": "0.19", "size": "10"}],
                "timestamp": "2026-03-20T00:08:30Z",
            }

    provider = _Provider()
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000),
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload, orderbook_provider=provider, prefer_live_depth=True)
    assert out["execution"]["status"] == "plan"
    assert out["execution"]["depth_plan"]["status"] == "ok"
    assert out["execution"]["depth_plan"]["depth_source_kind"] == "provider"
    assert provider.calls == ["token-up"]


def test_execution_snapshot_provider_only_skips_local_depth_fallback(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setenv("PM15MIN_LIVE_ORDERBOOK_PROVIDER_ONLY", "1")
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    write_json_atomic(
        {
            "dataset": "orderbook_latest_full_snapshot",
            "market": "sol",
            "cycle": "15m",
            "captured_ts_ms": captured_ts_ms,
            "records": [
                {
                    "captured_ts_ms": captured_ts_ms,
                    "source_ts_ms": captured_ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "asset": "sol",
                    "cycle": "15m",
                    "asks": [{"price": 0.20, "size": 1.0}, {"price": 0.2005, "size": 5.0}],
                    "bids": [{"price": 0.19, "size": 2.0}],
                    "source": "clob",
                }
            ],
        },
        data_cfg.layout.orderbook_latest_full_snapshot_path,
    )

    class _Provider:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def get_orderbook_summary(self, token_id, *, levels=0, timeout=1.2, force_refresh=False):
            self.calls.append(str(token_id))
            return None

    provider = _Provider()
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload, orderbook_provider=provider, prefer_live_depth=True)
    assert out["execution"]["status"] == "blocked"
    assert "depth_provider_missing" in out["execution"]["execution_reasons"]
    assert out["execution"]["depth_plan"]["status"] == "missing"
    assert out["execution"]["depth_plan"]["depth_source_kind"] == "provider"
    assert provider.calls == ["token-up"]


def test_execution_snapshot_plans_when_full_depth_is_sufficient(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 1.0], [0.2005, 5.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "plan"
    assert out["execution"]["token_id"] == "token-up"
    assert out["execution"]["order_type"] == "FAK"
    assert out["execution"]["requested_notional_usd"] == 1.0
    assert out["execution"]["depth_plan"]["status"] == "ok"
    assert out["execution"]["depth_plan"]["fill_ratio"] >= 1.0
    assert out["execution"]["repriced_metrics"]["repriced_entry_price"] >= 0.20
    assert out["execution"]["retry_policy"]["status"] == "armed"
    assert out["execution"]["retry_policy"]["post_submit_order_retry"]["retry_state_keys"] == [
        "attempts",
        "last_attempt",
        "last_error",
        "fast_retry",
        "retry_interval_seconds",
    ]
    assert out["execution"]["retry_policy"]["post_submit_order_retry"]["retryable_on_non_success_response"] is True
    assert out["execution"]["retry_policy"]["post_submit_fak_retry"]["enabled"] is True
    assert out["execution"]["retry_policy"]["post_submit_fak_retry"]["response_driven"] is True
    assert out["execution"]["retry_policy"]["post_submit_fak_retry"]["retryable_message_hints"] == ["no orders found to match"]
    assert out["execution"]["retry_policy"]["same_decision_repeat"]["enabled"] is True
    assert out["execution"]["retry_policy"]["same_decision_repeat"]["success_state_last_error"] == "matched_repeat_window"
    assert out["execution"]["cancel_policy"]["status"] == "inactive"
    assert out["execution"]["cancel_policy"]["reason"] == "order_type_has_no_resting_order"
    assert out["execution"]["redeem_policy"]["status"] == "unavailable"
    assert out["execution"]["redeem_policy"]["reason"] == "positions_snapshot_missing"
    assert out["execution"]["redeem_policy"]["condition_id"] == "cond-1"


def test_execution_snapshot_applies_regime_stake_scale(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    base_spec = resolve_live_profile_spec("deep_otm")
    scaled_spec = replace(
        base_spec,
        regime_apply_stake_scale=True,
        regime_caution_stake_multiplier=0.5,
    )
    monkeypatch.setattr("pm15min.live.execution.resolve_live_profile_spec", lambda profile: scaled_spec)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "regime_state": {"state": "CAUTION"},
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "plan"
    assert out["execution"]["stake_base_usd"] == 1.0
    assert out["execution"]["stake_multiplier"] == 0.5
    assert out["execution"]["stake_regime_state"] == "CAUTION"
    assert out["execution"]["requested_notional_usd"] == 0.5
    assert out["execution"]["requested_shares"] == 2.5


def test_execution_snapshot_uses_cash_balance_step_stake(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.20, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "account_summary": {
            "cash_balance_usd": 210.0,
            "cash_balance_available": True,
        },
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_eff_up": 0.80,
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }

    out = build_execution_snapshot(cfg, payload)

    assert out["execution"]["status"] == "plan"
    assert out["execution"]["stake_base_usd"] == 2.0
    assert out["execution"]["stake_source"] == "cash_balance_step"
    assert out["execution"]["cash_balance_usd"] == 210.0
    assert out["execution"]["stake_step_levels"] == 2
    assert out["execution"]["requested_notional_usd"] == 2.0
    assert out["execution"]["requested_shares"] == 10.0


def test_execution_snapshot_blocks_when_repriced_entry_breaks_band(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:08:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:08:30+00:00",
                "asks": [[0.301, 5.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "quote_up_ask": 0.29,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "blocked"
    assert "repriced_entry_price_max" in out["execution"]["execution_reasons"]
    assert out["execution"]["retry_policy"]["status"] == "inactive"


def test_execution_snapshot_marks_cancel_policy_ready_for_resting_order_near_end(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    base_spec = resolve_live_profile_spec("deep_otm")
    maker_spec = replace(base_spec, default_order_type="GTC", cancel_markets_when_minutes_left=2)
    monkeypatch.setattr("pm15min.live.execution.resolve_live_profile_spec", lambda profile: maker_spec)
    persist_open_orders_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_open_orders_snapshot",
            "snapshot_ts": "2026-03-20T00-13-31Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "orders": [
                {
                    "order_id": "order-1",
                    "market_id": "market-1",
                    "token_id": "token-up",
                }
            ],
            "summary": {
                "total_orders": 1,
                "by_market_id": {"market-1": 1},
                "by_token_id": {"token-up": 1},
            },
        },
    )
    persist_positions_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_positions_snapshot",
            "snapshot_ts": "2026-03-20T00-13-31Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "positions": [],
            "redeem_plan": {
                "cond-1": {
                    "condition_id": "cond-1",
                    "index_sets": [1],
                    "positions_count": 1,
                    "size_sum": 5.0,
                    "current_value_sum": 1.2,
                    "cash_pnl_sum": 0.0,
                }
            },
            "summary": {
                "total_positions": 1,
                "redeemable_positions": 1,
                "redeemable_conditions": 1,
            },
        },
    )
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    captured_ts_ms = int(pd.Timestamp("2026-03-20T00:13:30Z").timestamp() * 1000)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "logged_at": "2026-03-20T00:13:30+00:00",
                "asks": [[0.20, 10.0]],
                "bids": [[0.19, 2.0]],
            }
        ],
    )
    payload = {
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "snapshot_ts": "2026-03-20T00-00-00Z",
        "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        "accepted_offsets": [
            {
                "offset": 7,
                "recommended_side": "UP",
                "decision_ts": "2026-03-20T00:13:30+00:00",
                "p_up": 0.80,
                "confidence": 0.80,
                "quote_metrics": {
                    "entry_price": 0.20,
                    "fee_rate": 0.01,
                    "slippage_bps": 0.0,
                    "roi_net_vs_quote": 0.20,
                },
                "quote_row": {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                    "question": "test",
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "decision_ts": "2026-03-20T00:13:30+00:00",
                    "quote_up_ask": 0.20,
                    "quote_up_bid": 0.19,
                    "quote_up_ask_size_1": 10.0,
                    "quote_captured_ts_ms_up": captured_ts_ms,
                },
            }
        ],
    }
    out = build_execution_snapshot(cfg, payload)
    assert out["execution"]["status"] == "plan"
    assert out["execution"]["order_type"] == "GTC"
    assert out["execution"]["cancel_policy"]["status"] == "ready"
    assert out["execution"]["cancel_policy"]["reason"] == "open_orders_present_in_cancel_window"
    assert out["execution"]["cancel_policy"]["cancel_window_minutes"] == 2
    assert out["execution"]["cancel_policy"]["matching_open_orders_count"] == 1
    assert out["execution"]["redeem_policy"]["status"] == "ready"
    assert out["execution"]["redeem_policy"]["reason"] == "redeemable_positions_present"
    assert out["execution"]["redeem_policy"]["index_sets"] == [1]
