from __future__ import annotations

import json
from pathlib import Path

from pm15min.core.config import LiveConfig
from pm15min.live.runner import run_live_runner
from pm15min.live.runner.runtime import _resolve_iteration_limit


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


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


def test_run_live_runner_carries_session_trade_count_between_iterations(tmp_path: Path, monkeypatch) -> None:
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

    assert observed_counts == [{}, {"market-1_7": 1}]
    assert summary["session_state"]["market_offset_trade_count"] == {"market-1_7": 2}


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
