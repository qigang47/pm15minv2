from __future__ import annotations

import json
from pathlib import Path

import pm15min.live.runner as runner_module
from pm15min.core.config import LiveConfig
from pm15min.live.layout import LiveStateLayout
from pm15min.live.runner import run_live_runner
from pm15min.live.runner import iteration as runner_iteration_module
from pm15min.live.runner.runtime import _load_persisted_session_state, _resolve_iteration_limit
from pm15min.live.signal.utils import LiveClosedBarNotReadyError


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def test_runner_exports_distinct_prepare_and_preview_prewarm_callables() -> None:
    assert runner_module.prewarm_live_signal_inputs is not runner_module.prewarm_live_signal_preview


def test_run_live_runner_writes_summary_and_log(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    monkeypatch.setattr(
        "pm15min.live.runner.run_live_data_foundation",
        lambda *args, **kwargs: {"status": "ok", "dataset": "foundation"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {
                "status": "accept",
                "selected_offset": 7,
                "selected_side": "DOWN",
                "selected_quote_market_id": "market-1",
            },
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "plan", "order_type": "FAK", "market_id": "market-1", "selected_offset": 7},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {
            "latest": root / "var" / "live" / "state" / "execution" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "latest.json",
            "snapshot": root / "var" / "live" / "state" / "execution" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "snapshots" / "snapshot_ts=2026-03-20T00-00-01Z" / "execution.json",
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "order_submitted", "order_response": {"status": "live"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-02Z",
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    summary = run_live_runner(cfg, iterations=2, loop=True, sleep_sec=0.0, persist=True)
    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 2
    assert summary["session_state"]["market_offset_trade_count"] == {"market-1_7": 2}
    assert summary["last_iteration"]["decision"]["status"] == "accept"
    assert summary["last_iteration"]["execution"]["status"] == "plan"
    assert summary["last_iteration"]["session_state"]["market_offset_trade_count"] == {"market-1_7": 2}
    assert summary["last_iteration"]["risk_summary"]["decision"]["status"] == "accept"
    assert summary["last_iteration"]["risk_summary"]["execution"]["status"] == "plan"
    assert summary["last_iteration"]["runner_health"]["overall_status"] == "ok"
    assert summary["last_iteration"]["runner_health"]["primary_blocker"] is None
    assert summary["last_iteration"]["risk_alerts"] == []
    assert summary["last_iteration"]["order_action"]["status"] == "ok"
    assert summary["last_iteration"]["cancel_action"]["status"] == "ok"
    assert summary["last_iteration"]["redeem_action"]["status"] == "ok"

    latest_path = root / "var" / "live" / "state" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "latest.json"
    log_path = root / "var" / "live" / "logs" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "runner.jsonl"
    assert latest_path.exists()
    assert log_path.exists()

    latest = json.loads(latest_path.read_text(encoding="utf-8"))
    assert latest["status"] == "ok"
    assert latest["completed_iterations"] == 2

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any('"event": "runner_iteration"' in line for line in lines)
    assert any('"runner_log_tracked": true' in line for line in lines)


def test_run_live_runner_skips_normal_log_for_untracked_offset(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.delenv("PM15MIN_RUNNER_UNTRACKED_LOG_INTERVAL_SEC", raising=False)
    monkeypatch.delenv("PM15MIN_RUNNER_LOG_OFFSETS", raising=False)

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {"status": "accept", "selected_offset": 5, "selected_side": "DOWN", "selected_quote_market_id": "market-1"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "plan", "order_type": "FAK", "market_id": "market-1", "selected_offset": 5},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "order_submitted", "order_response": {"status": "live"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders": {"status": "ok"}, "positions": {"status": "ok"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    run_live_runner(cfg, iterations=2, loop=True, sleep_sec=0.0, persist=True)

    log_path = root / "var" / "live" / "logs" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "runner.jsonl"
    assert not log_path.exists()


def test_run_live_runner_can_raise_log_frequency_for_tracked_offsets(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setenv("PM15MIN_RUNNER_TRACKED_LOG_INTERVAL_SEC", "0")

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {"status": "accept", "selected_offset": 7, "selected_side": "DOWN", "selected_quote_market_id": "market-1"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "plan", "order_type": "FAK", "market_id": "market-1", "selected_offset": 7},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "order_submitted", "order_response": {"status": "live"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders": {"status": "ok"}, "positions": {"status": "ok"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    run_live_runner(cfg, iterations=2, loop=True, sleep_sec=0.0, persist=True)

    log_path = root / "var" / "live" / "logs" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "runner.jsonl"
    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 2


def test_run_live_runner_writes_compact_audit_log(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    bundle_dir = root / "research" / "model_bundles" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "bundle=test_bundle"
    diagnostics_dir = bundle_dir / "offsets" / "offset=7" / "diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    (diagnostics_dir / "logreg_coefficients.json").write_text(
        json.dumps(
            {
                "intercept": -0.1,
                "rows": [
                    {"feature": "move_z", "coefficient": 0.45, "abs_coefficient": 0.45, "direction": "positive", "rank": 1},
                    {"feature": "ret_5m", "coefficient": -0.12, "abs_coefficient": 0.12, "direction": "negative", "rank": 2},
                ],
            }
        ),
        encoding="utf-8",
    )
    (diagnostics_dir / "lgb_feature_importance.json").write_text(
        json.dumps(
            {
                "rows": [
                    {"feature": "move_z", "gain_importance": 120.0, "gain_share": 0.6, "split_importance": 15, "split_share": 0.3, "rank": 1},
                    {"feature": "ret_5m", "gain_importance": 40.0, "gain_share": 0.2, "split_importance": 8, "split_share": 0.16, "rank": 2},
                ],
            }
        ),
        encoding="utf-8",
    )
    (diagnostics_dir / "factor_direction_summary.json").write_text(
        json.dumps(
            {
                "rows": [
                    {"feature": "move_z", "direction_score": 0.51, "direction": "positive", "target_correlation": 0.51, "logreg_coefficient": 0.45, "lgb_gain_importance": 120.0, "rank": 1},
                    {"feature": "ret_5m", "direction_score": -0.19, "direction": "negative", "target_correlation": -0.19, "logreg_coefficient": -0.12, "lgb_gain_importance": 40.0, "rank": 2},
                ],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "bundle_dir": str(bundle_dir),
            "bundle_label": "test_bundle",
            "builder_feature_set": "v6_user_core",
            "bundle_feature_set": "v6_user_core",
            "active_bundle_selection_path": str(root / "selection.json"),
            "active_bundle": {"bundle_label": "test_bundle"},
            "decision": {
                "status": "accept",
                "selected_offset": 7,
                "selected_side": "UP",
                "selected_confidence": 0.82,
                "selected_edge": 0.64,
                "selected_decision_ts": "2026-03-20T00:08:00+00:00",
                "selected_quote_market_id": "market-1",
                "selected_entry_price": 0.31,
                "selected_roi_net_vs_quote": 1.1,
                "selected_p_lgb": 0.84,
                "selected_p_lr": 0.78,
                "selected_p_signal": 0.816,
                "selected_w_lgb": 0.6,
                "selected_w_lr": 0.4,
                "selected_probability_mode": "raw_blend",
            },
            "accepted_offsets": [
                {
                    "offset": 7,
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "signal_target": "direction",
                    "recommended_side": "UP",
                    "threshold": 0.55,
                    "score_valid": True,
                    "score_reason": "",
                    "confidence": 0.82,
                    "edge": 0.64,
                    "p_lgb": 0.84,
                    "p_lr": 0.78,
                    "p_signal": 0.816,
                    "w_lgb": 0.6,
                    "w_lr": 0.4,
                    "p_up": 0.82,
                    "p_down": 0.18,
                    "probability_mode": "raw_blend",
                    "guard_reasons": [],
                    "coverage": {"effective_missing_feature_count": 0, "not_allowed_blacklist_count": 0, "nan_feature_count": 0},
                    "quote_row": {"status": "ok", "market_id": "market-1", "condition_id": "cond-1", "question": "Will SOL go up?", "token_up": "token-up", "token_down": "token-down"},
                    "quote_metrics": {"entry_price": 0.31, "roi_net_vs_quote": 1.1, "price_cap": 0.42},
                    "feature_snapshot": {"move_z": 1.8, "ret_5m": 0.01},
                    "account_context": {"cash_balance_usd": 100.0},
                }
            ],
            "rejected_offsets": [
                {
                    "offset": 8,
                    "decision_ts": "2026-03-20T00:08:00+00:00",
                    "signal_target": "direction",
                    "recommended_side": "UP",
                    "threshold": 0.55,
                    "score_valid": True,
                    "score_reason": "",
                    "confidence": 0.58,
                    "edge": 0.16,
                    "p_lgb": 0.59,
                    "p_lr": 0.57,
                    "p_signal": 0.582,
                    "w_lgb": 0.6,
                    "w_lr": 0.4,
                    "p_up": 0.58,
                    "p_down": 0.42,
                    "probability_mode": "raw_blend",
                    "guard_reasons": ["entry_price_above_max"],
                    "coverage": {"effective_missing_feature_count": 0, "not_allowed_blacklist_count": 0, "nan_feature_count": 0},
                    "quote_row": {"status": "ok", "market_id": "market-2"},
                    "quote_metrics": {"entry_price": 0.74},
                    "feature_snapshot": {"move_z": 0.9, "ret_5m": 0.002},
                    "account_context": {"cash_balance_usd": 100.0},
                }
            ],
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {
                "status": "plan",
                "reason": None,
                "execution_reasons": [],
                "order_type": "FAK",
                "market_id": "market-1",
                "selected_offset": 7,
                "selected_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "entry_price": 0.31,
                "requested_notional_usd": 25.0,
                "price_cap": 0.42,
                "depth_plan": {"status": "ok", "max_price": 0.32},
                "retry_policy": {"should_retry": False},
                "cancel_policy": {"mode": "none"},
                "redeem_policy": {"mode": "none"},
            },
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {
            "status": "ok",
            "reason": "order_submitted",
            "dry_run": False,
            "attempt": 1,
            "attempted": True,
            "action_key": "action-1",
            "gate": {"decision": "allow", "reason": "first_attempt"},
            "order_request": {"token_id": "token-up", "side": "BUY", "price": 0.31, "size": 80.0},
            "order_response": {"success": True, "status": "live", "order_id": "order-1", "message": None},
            "trading_gateway": {"adapter": "direct"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders": {"status": "ok"}, "positions": {"status": "ok"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    summary = run_live_runner(cfg, iterations=2, loop=True, sleep_sec=0.0, persist=True)

    audit_path = root / "var" / "live" / "logs" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "audit.jsonl"
    assert summary["runner_audit_log_path"] == str(audit_path)
    assert audit_path.exists()

    events = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert [event["event"] for event in events] == ["decision_state", "order_action"]
    decision_event = events[0]
    assert decision_event["signal_bundle"]["bundle_label"] == "test_bundle"
    assert decision_event["decision"]["selected_p_signal"] == 0.816
    assert decision_event["decision"]["selected_w_lgb"] == 0.6
    assert decision_event["selected_offset_context"]["feature_snapshot"]["move_z"] == 1.8
    assert decision_event["selected_model_context"]["top_logreg_coefficients"][0]["feature"] == "move_z"
    assert decision_event["selected_model_context"]["top_lgb_feature_importance"][0]["feature"] == "move_z"
    assert decision_event["selected_factor_snapshot"]["top_positive_factors"][0]["feature"] == "move_z"
    assert decision_event["selected_factor_snapshot"]["top_positive_factors"][0]["live_value"] == 1.8
    assert decision_event["rejected_offset_summaries"][0]["guard_reasons"] == ["entry_price_above_max"]

    order_event = events[1]
    assert order_event["order_action"]["reason"] == "order_submitted"
    assert order_event["order_action"]["order_response"]["order_id"] == "order-1"
    assert order_event["execution"]["selected_offset"] == 7
    assert order_event["selected_factor_snapshot"]["top_negative_factors"][0]["feature"] == "ret_5m"


def test_run_live_runner_does_not_count_dry_run_between_iterations(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    observed_counts: list[dict[str, int]] = []

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )

    def _decide(*args, **kwargs):
        session_state = kwargs.get("session_state") or {}
        observed_counts.append(dict(session_state.get("market_offset_trade_count") or {}))
        return {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {
                "status": "accept",
                "selected_offset": 7,
                "selected_side": "DOWN",
                "selected_quote_market_id": "market-1",
            },
        }

    monkeypatch.setattr("pm15min.live.runner.decide_live_latest", _decide)
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {
                "status": "plan",
                "order_type": "FAK",
                "market_id": "market-1",
                "selected_offset": 7,
            },
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "dry_run", "order_response": {"status": "dry_run"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-02Z",
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    summary = run_live_runner(cfg, iterations=2, loop=True, sleep_sec=0.0, persist=True)

    assert observed_counts == [{}, {}]
    assert summary["session_state"]["market_offset_trade_count"] == {}
    assert summary["session_state"]["market_offset_side_trade_count"] == {}


def test_run_live_runner_does_not_restore_dry_run_trade_count_after_restart(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    observed_counts: list[dict[str, int]] = []

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )

    def _decide(*args, **kwargs):
        session_state = kwargs.get("session_state") or {}
        observed_counts.append(dict(session_state.get("market_offset_trade_count") or {}))
        return {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {
                "status": "accept",
                "selected_offset": 7,
                "selected_side": "DOWN",
                "selected_quote_market_id": "market-1",
            },
        }

    monkeypatch.setattr("pm15min.live.runner.decide_live_latest", _decide)
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {
                "status": "plan",
                "order_type": "FAK",
                "market_id": "market-1",
                "selected_offset": 7,
            },
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "dry_run", "order_response": {"status": "dry_run"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-02Z",
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    first = run_live_runner(cfg, iterations=1, loop=False, sleep_sec=0.0, persist=True)
    second = run_live_runner(cfg, iterations=1, loop=False, sleep_sec=0.0, persist=True)

    assert first["session_state"]["market_offset_trade_count"] == {}
    assert second["session_state"]["market_offset_trade_count"] == {}
    assert first["session_state"]["market_offset_side_trade_count"] == {}
    assert second["session_state"]["market_offset_side_trade_count"] == {}
    assert observed_counts == [{}, {}]


def test_run_live_runner_restores_session_trade_count_from_audit_log(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    observed_counts: list[dict[str, int]] = []

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )

    def _decide(*args, **kwargs):
        session_state = kwargs.get("session_state") or {}
        observed_counts.append(dict(session_state.get("market_offset_trade_count") or {}))
        return {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {
                "status": "accept",
                "selected_offset": 7,
                "selected_side": "DOWN",
                "selected_quote_market_id": "market-1",
            },
        }

    monkeypatch.setattr("pm15min.live.runner.decide_live_latest", _decide)
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {
                "status": "plan",
                "order_type": "FAK",
                "market_id": "market-1",
                "selected_offset": 7,
                "selected_side": "DOWN",
                "decision_ts": "2026-03-20T00:08:00+00:00",
            },
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {
            "status": "ok",
            "reason": "order_submitted",
            "attempted": True,
            "action_key": "action-1",
            "attempt": 1,
            "dry_run": False,
            "order_request": {"side": "DOWN", "price": 0.2, "size": 5.0, "token_id": "token-down"},
            "order_response": {"status": "live", "order_id": "order-1", "success": True},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-02Z",
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}},
    )

    first = run_live_runner(cfg, iterations=1, loop=False, sleep_sec=0.0, persist=True)
    latest_runner = root / "var" / "live" / "state" / "runner" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "latest.json"
    latest_runner.unlink()
    second = run_live_runner(cfg, iterations=1, loop=False, sleep_sec=0.0, persist=True)

    assert first["session_state"]["market_offset_trade_count"] == {"market-1_7": 1}
    assert first["session_state"]["market_offset_side_trade_count"] == {"market-1_7_DOWN": 1}
    assert second["session_state"]["market_offset_trade_count"] == {"market-1_7": 2}
    assert second["session_state"]["market_offset_side_trade_count"] == {"market-1_7_DOWN": 2}
    assert observed_counts == [{}, {"market-1_7": 1}]


def test_run_live_runner_uses_fixed_period_sleep(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    monkeypatch.setattr("pm15min.live.runner.build_orderbook_provider_from_env", lambda *args, **kwargs: object())
    monkeypatch.setattr("pm15min.live.runner.build_in_memory_cached_orderbook_provider", lambda provider, **kwargs: provider)
    monkeypatch.setattr("pm15min.live.runner.append_jsonl", lambda *args, **kwargs: None)
    monkeypatch.setattr("pm15min.live.runner.write_json_atomic", lambda *args, **kwargs: None)

    class _FakeTime:
        def __init__(self) -> None:
            self.now = 0.0
            self.sleeps: list[float] = []

        def time(self) -> float:
            return float(self.now)

        def monotonic(self) -> float:
            return float(self.now)

        def sleep(self, seconds: float) -> None:
            self.sleeps.append(float(seconds))
            self.now += float(seconds)

    fake_time = _FakeTime()
    starts: list[float] = []

    def _build_runner_iteration(*args, **kwargs):
        starts.append(fake_time.monotonic())
        fake_time.now += 0.05
        return {
            "snapshot_ts": f"run-{len(starts)}",
            "decision": {
                "status": "reject",
                "selected_offset": 7,
                "selected_side": None,
                "selected_quote_market_id": None,
            },
            "execution": {
                "status": "no_action",
                "reason": "decision_reject",
            },
            "runner_health": {"overall_status": "ok"},
            "risk_alert_summary": {"has_critical": False},
            "session_state": kwargs.get("session_state") or {},
        }

    monkeypatch.setattr("pm15min.live.runner.build_runner_iteration", _build_runner_iteration)
    monkeypatch.setattr("pm15min.live.runner.runtime.time.time", fake_time.time)
    monkeypatch.setattr("pm15min.live.runner.runtime.time.monotonic", fake_time.monotonic)
    monkeypatch.setattr("pm15min.live.runner.runtime.time.sleep", fake_time.sleep)

    summary = run_live_runner(cfg, iterations=3, loop=True, sleep_sec=0.3, persist=False)

    assert summary["completed_iterations"] == 3
    assert starts == [0.0, 0.3, 0.6]
    assert fake_time.sleeps == [0.25, 0.25]


def test_load_persisted_session_state_ignores_dry_run_audit_entries(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    layout = LiveStateLayout.discover(root=root)
    audit_path = layout.runner_audit_log_path(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
    )
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event": "order_action",
                        "snapshot_ts": "2026-03-20T00-00-00Z",
                        "execution": {
                            "market_id": "market-1",
                            "selected_offset": 7,
                            "selected_side": "UP",
                        },
                        "order_action": {
                            "status": "ok",
                            "reason": "dry_run",
                            "action_key": "dry-run-action",
                            "attempt": 1,
                            "dry_run": True,
                        },
                    }
                ),
                json.dumps(
                    {
                        "event": "order_action",
                        "snapshot_ts": "2026-03-20T00-00-05Z",
                        "execution": {
                            "market_id": "market-1",
                            "selected_offset": 7,
                            "selected_side": "DOWN",
                        },
                        "order_action": {
                            "status": "ok",
                            "reason": "order_submitted",
                            "action_key": "real-action",
                            "attempt": 1,
                            "dry_run": False,
                        },
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    state = _load_persisted_session_state(
        layout=layout,
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
    )

    assert state["market_offset_trade_count"] == {"market-1_7": 1}
    assert state["market_offset_side_trade_count"] == {"market-1_7_DOWN": 1}
    assert state["action_gate_state"] == {
        "order": {
            "real-action": {
                "action_key": "real-action",
                "snapshot_ts": "2026-03-20T00-00-05Z",
                "status": "ok",
                "reason": "order_submitted",
                "attempt": 1,
                "last_attempt_snapshot_ts": "2026-03-20T00-00-05Z",
                "last_attempt_status": "ok",
                "last_attempt_reason": "order_submitted",
                "dry_run": False,
            }
        }
    }


def test_load_persisted_session_state_preserves_side_effect_state(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    layout = LiveStateLayout.discover(root=root)
    latest_path = layout.latest_runner_path(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.write_text(
        json.dumps(
            {
                "session_state": {
                    "market_offset_trade_count": {"market-1_7": 1},
                    "market_offset_side_trade_count": {"market-1_7_UP": 1},
                    "action_gate_state": {},
                    "side_effect_state": {
                        "account_sync": {
                            "last_started_at_epoch": 100.0,
                            "last_completed_at_epoch": 101.5,
                            "last_snapshot_ts": "2026-03-20T00-00-01Z",
                            "last_status": "ok",
                            "last_reason": None,
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )

    state = _load_persisted_session_state(
        layout=layout,
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
    )

    assert state["market_offset_trade_count"] == {"market-1_7": 1}
    assert state["market_offset_side_trade_count"] == {"market-1_7_UP": 1}
    assert state["side_effect_state"] == {
        "account_sync": {
            "last_started_at_epoch": 100.0,
            "last_completed_at_epoch": 101.5,
            "last_snapshot_ts": "2026-03-20T00-00-01Z",
            "last_status": "ok",
            "last_reason": None,
        }
    }


def test_run_live_runner_can_disable_side_effects(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {"status": "reject", "selected_offset": None},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "no_action", "reason": "decision_reject"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )

    summary = run_live_runner(
        cfg,
        iterations=1,
        loop=False,
        sleep_sec=0.0,
        persist=True,
        apply_side_effects=False,
    )
    assert summary["status"] == "ok"
    assert summary["last_iteration"]["order_action"] is None
    assert summary["last_iteration"]["cancel_action"] is None
    assert summary["last_iteration"]["redeem_action"] is None
    assert summary["last_iteration"]["risk_summary"]["decision"]["status"] == "reject"
    assert summary["last_iteration"]["risk_summary"]["execution"]["status"] == "no_action"
    assert summary["last_iteration"]["runner_health"]["pre_side_effect_status"] == "blocked"
    assert summary["last_iteration"]["runner_health"]["primary_blocker"] == "decision_not_accept"
    alert_codes = [row["code"] for row in summary["last_iteration"]["risk_alerts"]]
    assert "decision_reject" in alert_codes
    assert "execution_no_action" in alert_codes


def test_run_live_runner_can_throttle_account_sync_and_disable_cancel_redeem(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls = {"account": 0, "cancel": 0, "redeem": 0}
    monkeypatch.setenv("PM15MIN_RUNNER_ACCOUNT_SYNC_INTERVAL_SEC", "60")
    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_CANCEL_POLICY", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_REDEEM_POLICY", "0")

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {"status": "accept", "selected_offset": 7, "selected_side": "DOWN", "selected_quote_market_id": "market-1"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "plan", "order_type": "FAK", "market_id": "market-1", "selected_offset": 7},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "ok", "reason": "order_submitted", "order_response": {"status": "live"}},
    )

    def _fake_account_state(*args, **kwargs):
        calls["account"] += 1
        return {
            "snapshot_ts": f"2026-03-20T00-00-0{calls['account']}Z",
            "open_orders": {"status": "ok"},
            "positions": {"status": "ok"},
        }

    def _fake_cancel(*args, **kwargs):
        calls["cancel"] += 1
        return {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}}

    def _fake_redeem(*args, **kwargs):
        calls["redeem"] += 1
        return {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}}

    monkeypatch.setattr("pm15min.live.runner.build_account_state_snapshot", _fake_account_state)
    monkeypatch.setattr("pm15min.live.runner.apply_cancel_policy", _fake_cancel)
    monkeypatch.setattr("pm15min.live.runner.apply_redeem_policy", _fake_redeem)

    summary = run_live_runner(cfg, iterations=3, loop=True, sleep_sec=0.0, persist=True)

    assert calls == {"account": 1, "cancel": 0, "redeem": 0}
    assert summary["completed_iterations"] == 3
    assert summary["last_iteration"]["order_action"]["status"] == "ok"
    assert summary["last_iteration"]["account_state_payload"]["status"] == "skipped"
    assert summary["last_iteration"]["account_state_payload"]["reason"] == "account_sync_interval_not_elapsed"
    assert summary["last_iteration"]["cancel_action"]["status"] == "skipped"
    assert summary["last_iteration"]["cancel_action_payload"]["reason"] == "cancel_policy_disabled"
    assert summary["last_iteration"]["redeem_action"]["status"] == "skipped"
    assert summary["last_iteration"]["redeem_action_payload"]["reason"] == "redeem_policy_disabled"
    assert summary["last_iteration"]["runner_health"]["overall_status"] == "ok"
    assert summary["last_iteration"]["runner_health"]["primary_blocker"] is None
    assert "side_effect_state" in summary["session_state"]
    assert summary["session_state"]["side_effect_state"]["account_sync"]["last_status"] == "skipped"


def test_resolve_iteration_limit_supports_daemon_mode() -> None:
    assert _resolve_iteration_limit(iterations=0, loop=True) is None
    assert _resolve_iteration_limit(iterations=-1, loop=True) is None
    assert _resolve_iteration_limit(iterations=0, loop=False) == 1
    assert _resolve_iteration_limit(iterations=2, loop=True) == 2


def test_run_live_runner_recovers_from_side_effect_step_errors(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    calls = {"cancel": 0, "redeem": 0}

    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "decision": {"status": "accept", "selected_offset": 7, "selected_side": "UP"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "execution": {"status": "plan", "order_type": "FAK"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.persist_execution_snapshot",
        lambda *args, **kwargs: {"latest": root / "latest.json", "snapshot": root / "snapshot.json"},
    )

    def _raise_submit(*args, **kwargs):
        raise RuntimeError("submit boom")

    def _raise_account_state(*args, **kwargs):
        raise RuntimeError("sync boom")

    def _cancel(*args, **kwargs):
        calls["cancel"] += 1
        return {"status": "ok", "reason": "cancel_policy_applied", "summary": {"cancelled_orders": 0}}

    def _redeem(*args, **kwargs):
        calls["redeem"] += 1
        return {"status": "ok", "reason": "redeem_policy_applied", "summary": {"redeemed_conditions": 0}}

    monkeypatch.setattr("pm15min.live.runner.submit_execution_payload", _raise_submit)
    monkeypatch.setattr("pm15min.live.runner.build_account_state_snapshot", _raise_account_state)
    monkeypatch.setattr("pm15min.live.runner.apply_cancel_policy", _cancel)
    monkeypatch.setattr("pm15min.live.runner.apply_redeem_policy", _redeem)

    summary = run_live_runner(cfg, iterations=1, loop=False, sleep_sec=0.0, persist=True)

    assert summary["status"] == "ok_with_errors"
    assert summary["completed_iterations"] == 1
    assert summary["last_iteration"]["order_action"]["status"] == "error"
    assert summary["last_iteration"]["order_action"]["reason"] == "submit_execution_payload_exception"
    assert summary["last_iteration"]["account_state"]["open_orders_status"] == "error"
    assert summary["last_iteration"]["cancel_action"]["status"] == "ok"
    assert summary["last_iteration"]["redeem_action"]["status"] == "ok"
    assert summary["last_iteration"]["runner_health"]["overall_status"] == "error"
    assert summary["last_iteration"]["runner_health"]["primary_blocker"] == "order_action_error"
    assert summary["last_iteration"]["runner_health"]["blocker_stage"] == "order"
    alert_codes = [row["code"] for row in summary["last_iteration"]["risk_alerts"]]
    assert "order_action_error" in alert_codes
    assert "account_state_sync_error" in alert_codes
    assert calls["cancel"] == 1
    assert calls["redeem"] == 1


def test_run_live_runner_prewarms_signal_once_per_minute(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    prewarm_calls: list[None] = []
    fake_now = {"value": 60.9}

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0.5")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3.0")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))
    monkeypatch.setattr("pm15min.live.runner.run_live_data_foundation", lambda *args, **kwargs: {"status": "ok"})
    monkeypatch.setattr(
        "pm15min.live.runner.build_liquidity_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-00Z", "status": "ok", "blocked": False, "reason": "ok"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.prewarm_live_signal_cache",
        lambda *args, **kwargs: prewarm_calls.append(None) or {"status": "ok", "cache_hit": False, "elapsed_ms": 12.0, "snapshot_ts": "2026-03-20T00-01-00Z", "offsets": [7, 8, 9]},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.prewarm_live_signal_inputs",
        lambda *args, **kwargs: {"status": "ok", "elapsed_ms": 9.0, "feature_rows": 9, "latest_feature_decision_ts": "2026-03-20T00:00:00Z", "offsets": [7, 8, 9]},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.decide_live_latest",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-00Z",
            "timings_ms": {
                "signal_stage_ms": 0.5,
                "signal_bundle_resolution_stage_ms": 0.11,
                "signal_feature_prepare_stage_ms": 0.22,
                "signal_feature_frame_stage_ms": 0.33,
                "signal_liquidity_state_stage_ms": 0.44,
                "signal_regime_state_stage_ms": 0.55,
                "signal_offset_scoring_stage_ms": 0.66,
                "quote_stage_ms": 1.0,
                "account_context_stage_ms": 0.1,
                "decision_build_stage_ms": 0.2,
                "signal_cache_hit": True,
            },
            "decision": {"status": "reject"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_execution_snapshot",
        lambda *args, **kwargs: {
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "timings_ms": {"depth_stage_ms": 0.1, "depth_plan_reused": True},
            "execution": {"status": "no_action"},
        },
    )
    monkeypatch.setattr(
        "pm15min.live.runner.submit_execution_payload",
        lambda *args, **kwargs: {"status": "skipped", "reason": "execution_not_plan:no_action"},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.build_account_state_snapshot",
        lambda *args, **kwargs: {"snapshot_ts": "2026-03-20T00-00-02Z", "open_orders": {"status": "ok"}, "positions": {"status": "ok"}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_cancel_policy",
        lambda *args, **kwargs: {"status": "skipped", "reason": "cancel_policy_disabled", "summary": {"cancelled_orders": 0}},
    )
    monkeypatch.setattr(
        "pm15min.live.runner.apply_redeem_policy",
        lambda *args, **kwargs: {"status": "skipped", "reason": "redeem_policy_disabled", "summary": {"redeemed_conditions": 0}},
    )

    summary = run_live_runner(
        cfg,
        iterations=2,
        loop=True,
        sleep_sec=0.0,
        persist=False,
        run_foundation=False,
        apply_side_effects=False,
    )

    assert len(prewarm_calls) == 1
    assert summary["last_iteration"]["timings_ms"]["signal_prewarm_triggered"] is False
    assert summary["last_iteration"]["timings_ms"]["decision_signal_bundle_resolution_stage_ms"] == 0.11
    assert summary["last_iteration"]["timings_ms"]["decision_signal_feature_prepare_stage_ms"] == 0.22
    assert summary["last_iteration"]["timings_ms"]["decision_signal_feature_frame_stage_ms"] == 0.33
    assert summary["last_iteration"]["timings_ms"]["decision_signal_liquidity_state_stage_ms"] == 0.44
    assert summary["last_iteration"]["timings_ms"]["decision_signal_regime_state_stage_ms"] == 0.55
    assert summary["last_iteration"]["timings_ms"]["decision_signal_offset_scoring_stage_ms"] == 0.66
    assert summary["last_iteration"]["signal_prewarm_payload"]["reason"] == "signal_prewarm_finalize_already_attempted_for_bucket"


def test_maybe_prewarm_signal_cache_retries_after_closed_bar_deferral(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 60.05}
    calls = {"count": 0}

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", "0")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))

    def _prewarm(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise LiveClosedBarNotReadyError("live_closed_bar_not_ready expected_open_time=2026-03-20T00:07:00+00:00")
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
        }

    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    first = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=lambda *args, **kwargs: {"status": "ok"},
        prewarm_live_signal_cache_fn=_prewarm,
    )
    assert first["status"] == "deferred"
    assert first["triggered"] is False
    assert first["reason"] == "signal_prewarm_waiting_for_closed_bar"

    fake_now["value"] = 60.25
    second = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=lambda *args, **kwargs: {"status": "ok"},
        prewarm_live_signal_cache_fn=_prewarm,
    )
    assert second["status"] == "ok"
    assert second["triggered"] is True
    assert calls["count"] == 2


def test_maybe_prewarm_signal_cache_finalize_waits_for_closed_bar_within_same_iteration(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 60.05}
    calls = {"count": 0}

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", "0.30")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_RETRY_INTERVAL_SEC", "0.10")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))
    monkeypatch.setattr(
        "pm15min.live.runner.iteration.time.sleep",
        lambda seconds: fake_now.__setitem__("value", float(fake_now["value"]) + float(seconds)),
    )

    def _prewarm(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            raise LiveClosedBarNotReadyError("live_closed_bar_not_ready expected_open_time=2026-03-20T00:07:00+00:00")
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
        }

    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    payload = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=lambda *args, **kwargs: {"status": "ok"},
        prewarm_live_signal_cache_fn=_prewarm,
    )
    assert payload["status"] == "ok"
    assert payload["triggered"] is True
    assert payload["stage"] == "finalize"
    assert calls["count"] == 3


def test_maybe_prewarm_signal_cache_prepare_can_bridge_into_finalize(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 119.60}
    calls: list[str] = []

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MIN_DELAY_SEC", "57")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MAX_DELAY_SEC", "59.9")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_FINALIZE_BRIDGE_SEC", "1.0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", "0")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))
    monkeypatch.setattr(
        "pm15min.live.runner.iteration.time.sleep",
        lambda seconds: fake_now.__setitem__("value", float(fake_now["value"]) + float(seconds)),
    )

    def _prewarm_prepare(*args, **kwargs):
        calls.append("prepare")
        return {
            "status": "ok",
            "elapsed_ms": 9.0,
            "feature_rows": 9,
            "latest_feature_decision_ts": "2026-03-20T00:00:00Z",
            "offsets": [7, 8, 9],
        }

    def _prewarm_finalize(*args, **kwargs):
        calls.append(str(kwargs.get("marker_source") or "prewarm_finalize"))
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
            "marker_source": str(kwargs.get("marker_source") or "prewarm_finalize"),
        }

    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    payload = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert payload["status"] == "ok"
    assert payload["stage"] == "finalize"
    assert calls == ["prepare", "prewarm_finalize"]


def test_maybe_prewarm_signal_cache_bridges_using_post_prepare_time(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 118.40}
    calls: list[str] = []

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MIN_DELAY_SEC", "57")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MAX_DELAY_SEC", "59.9")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_FINALIZE_BRIDGE_SEC", "1.0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", "0")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))
    monkeypatch.setattr(
        "pm15min.live.runner.iteration.time.sleep",
        lambda seconds: fake_now.__setitem__("value", float(fake_now["value"]) + float(seconds)),
    )

    def _prewarm_prepare(*args, **kwargs):
        calls.append("prepare")
        fake_now["value"] += 0.90
        return {
            "status": "ok",
            "elapsed_ms": 900.0,
            "feature_rows": 9,
            "latest_feature_decision_ts": "2026-03-20T00:00:00Z",
            "offsets": [7, 8, 9],
        }

    def _prewarm_finalize(*args, **kwargs):
        calls.append(str(kwargs.get("marker_source") or "prewarm_finalize"))
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
            "marker_source": str(kwargs.get("marker_source") or "prewarm_finalize"),
        }

    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    payload = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert payload["status"] == "ok"
    assert payload["stage"] == "finalize"
    assert calls == ["prepare", "prewarm_finalize"]


def test_maybe_prewarm_signal_cache_wider_bridge_catches_earlier_prepare(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 117.20}
    calls: list[str] = []

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MIN_DELAY_SEC", "57")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MAX_DELAY_SEC", "59.9")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_FINALIZE_BRIDGE_SEC", "4.0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_FINALIZE_CLOSED_BAR_WAIT_SEC", "0")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))
    monkeypatch.setattr(
        "pm15min.live.runner.iteration.time.sleep",
        lambda seconds: fake_now.__setitem__("value", float(fake_now["value"]) + float(seconds)),
    )

    def _prewarm_prepare(*args, **kwargs):
        calls.append("prepare")
        return {
            "status": "ok",
            "elapsed_ms": 12.0,
            "feature_rows": 9,
            "latest_feature_decision_ts": "2026-03-20T00:00:00Z",
            "offsets": [7, 8, 9],
        }

    def _prewarm_finalize(*args, **kwargs):
        calls.append(str(kwargs.get("marker_source") or "prewarm_finalize"))
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
            "marker_source": str(kwargs.get("marker_source") or "prewarm_finalize"),
        }

    cfg = LiveConfig.build(market="xrp", profile="deep_otm", cycle_minutes=15)
    payload = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert payload["status"] == "ok"
    assert payload["stage"] == "finalize"
    assert calls == ["prepare", "prewarm_finalize"]
    assert fake_now["value"] >= 120.0


def test_signal_prewarm_prepare_trigger_sec_staggers_markets() -> None:
    assert runner_iteration_module._signal_prewarm_prepare_trigger_sec(market="sol", default=57.0, upper_bound=59.9) == 56.6
    assert runner_iteration_module._signal_prewarm_prepare_trigger_sec(market="xrp", default=57.0, upper_bound=59.9) == 57.2
    assert runner_iteration_module._signal_prewarm_prepare_trigger_sec(market="eth", default=57.0, upper_bound=59.9) == 57.8
    assert runner_iteration_module._signal_prewarm_prepare_trigger_sec(market="btc", default=57.0, upper_bound=59.9) == 58.4


def test_maybe_prewarm_signal_cache_prepare_and_finalize_use_separate_bucket_guards(monkeypatch) -> None:
    session_state: dict[str, object] = {}
    fake_now = {"value": 60.10}
    calls: list[str] = []

    monkeypatch.setenv("PM15MIN_RUNNER_ENABLE_SIGNAL_PREWARM", "1")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MIN_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_MAX_DELAY_SEC", "3")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MIN_DELAY_SEC", "57")
    monkeypatch.setenv("PM15MIN_RUNNER_SIGNAL_PREWARM_PREPARE_MAX_DELAY_SEC", "59.9")
    monkeypatch.setattr("pm15min.live.runner.iteration.time.time", lambda: float(fake_now["value"]))

    def _prewarm_prepare(*args, **kwargs):
        calls.append("prewarm_prepare_inputs")
        return {
            "status": "ok",
            "elapsed_ms": 9.0,
            "feature_rows": 9,
            "latest_feature_decision_ts": "2026-03-20T00:00:00Z",
            "offsets": [7, 8, 9],
        }

    def _prewarm_finalize(*args, **kwargs):
        calls.append(str(kwargs.get("marker_source") or "prewarm_finalize"))
        return {
            "status": "ok",
            "cache_hit": False,
            "elapsed_ms": 12.0,
            "snapshot_ts": "2026-03-20T00:01:00Z",
            "offsets": [7, 8, 9],
            "marker_source": str(kwargs.get("marker_source") or "prewarm_finalize"),
        }

    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    first = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert first["status"] == "ok"
    assert first["stage"] == "finalize"
    assert first["triggered"] is True

    fake_now["value"] = 117.90
    second = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert second["status"] == "ok"
    assert second["stage"] == "prepare"
    assert second["triggered"] is True

    fake_now["value"] = 118.10
    third = runner_iteration_module._maybe_prewarm_signal_cache(
        cfg,
        target="direction",
        feature_set=None,
        session_state=session_state,
        prewarm_live_signal_inputs_fn=_prewarm_prepare,
        prewarm_live_signal_cache_fn=_prewarm_finalize,
    )
    assert third["status"] == "skipped"
    assert third["reason"] == "signal_prewarm_prepare_already_attempted_for_bucket"
    assert calls == ["prewarm_finalize", "prewarm_prepare_inputs"]
