from __future__ import annotations

import threading
import time
from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.live.account import persist_open_orders_snapshot, persist_positions_snapshot
from pm15min.live.actions import apply_cancel_policy, apply_redeem_policy, submit_execution_payload
from pm15min.live.trading.contracts import CancelOrderResult, PlaceOrderResult, RedeemResult


class FakeGateway:
    adapter_name = "fake"

    def __init__(
        self,
        *,
        place_order_result: PlaceOrderResult | None = None,
        place_order_exc: Exception | None = None,
        cancel_order_results: dict[str, CancelOrderResult] | None = None,
        cancel_order_exc: Exception | None = None,
        redeem_result: RedeemResult | None = None,
        redeem_exc: Exception | None = None,
    ) -> None:
        self.place_order_result = place_order_result or PlaceOrderResult(success=True, status="live", order_id="order-123")
        self.place_order_exc = place_order_exc
        self.cancel_order_results = dict(cancel_order_results or {})
        self.cancel_order_exc = cancel_order_exc
        self.redeem_result = redeem_result or RedeemResult(success=True, status="confirmed", tx_hash="0xtx", state="confirmed")
        self.redeem_exc = redeem_exc
        self.place_order_calls: list[object] = []
        self.cancel_order_calls: list[str] = []
        self.redeem_calls: list[object] = []

    def place_order(self, request):
        self.place_order_calls.append(request)
        if self.place_order_exc is not None:
            raise self.place_order_exc
        return self.place_order_result

    def cancel_order(self, order_id: str):
        self.cancel_order_calls.append(order_id)
        if self.cancel_order_exc is not None:
            raise self.cancel_order_exc
        return self.cancel_order_results.get(
            order_id,
            CancelOrderResult(success=False, status="cancel_order_failed", order_id=order_id, message="cancel_order_failed"),
        )

    def redeem_positions(self, request):
        self.redeem_calls.append(request)
        if self.redeem_exc is not None:
            raise self.redeem_exc
        return self.redeem_result


def _patch_v2_roots(monkeypatch, root: Path) -> None:
    monkeypatch.setattr("pm15min.core.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.data.layout.rewrite_root", lambda: root)
    monkeypatch.setattr("pm15min.research.layout.rewrite_root", lambda: root)


def _patch_action_snapshot_labels(monkeypatch, labels: list[str]) -> None:
    sequence = iter(labels)
    monkeypatch.setattr("pm15min.live.actions.utc_snapshot_label", lambda: next(sequence))


def test_apply_cancel_policy_cancels_orders_in_window(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-20T00:00:00Z")
    cycle_end = pd.Timestamp("2026-03-20T00:15:00Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    persist_open_orders_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_open_orders_snapshot",
            "snapshot_ts": "2026-03-20T00-14-00Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "orders": [
                {"order_id": "order-1", "market_id": "market-1", "token_id": "token-up"},
                {"order_id": "order-2", "market_id": "market-x", "token_id": "token-x"},
            ],
            "summary": {"total_orders": 2, "by_market_id": {"market-1": 1, "market-x": 1}, "by_token_id": {}},
        },
    )
    gateway = FakeGateway(
        cancel_order_results={
            "order-1": CancelOrderResult(success=True, status="cancelled", order_id="order-1"),
        }
    )

    payload = apply_cancel_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        now=pd.Timestamp("2026-03-20T00:14:00Z"),
        gateway=gateway,
    )

    assert payload["status"] == "ok"
    assert payload["summary"]["candidate_orders"] == 1
    assert payload["summary"]["cancelled_orders"] == 1
    assert payload["trading_gateway"]["adapter"] == "fake"
    assert payload["results"][0]["order_id"] == "order-1"
    assert payload["results"][0]["status"] == "cancelled"
    assert "latest_cancel_action_path" in payload


def test_apply_redeem_policy_redeems_redeem_plan(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    persist_positions_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_positions_snapshot",
            "snapshot_ts": "2026-03-20T00-14-00Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "positions": [],
            "redeem_plan": {
                "cond-1": {
                    "condition_id": "cond-1",
                    "index_sets": [1, 2],
                    "positions_count": 2,
                    "size_sum": 8.0,
                    "current_value_sum": 2.4,
                    "cash_pnl_sum": 0.0,
                }
            },
            "summary": {
                "total_positions": 2,
                "redeemable_positions": 2,
                "redeemable_conditions": 1,
            },
        },
    )
    gateway = FakeGateway(
        redeem_result=RedeemResult(success=True, status="confirmed", tx_hash="0xtx", state="confirmed")
    )

    payload = apply_redeem_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert payload["status"] == "ok"
    assert payload["summary"]["candidate_conditions"] == 1
    assert payload["summary"]["redeemed_conditions"] == 1
    assert payload["trading_gateway"]["adapter"] == "fake"
    assert payload["results"][0]["condition_id"] == "cond-1"
    assert payload["results"][0]["status"] == "redeemed"
    assert payload["results"][0]["tx_hash"] == "0xtx"
    assert "latest_redeem_action_path" in payload


def test_apply_redeem_policy_reports_missing_redeem_config_without_gateway(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    persist_positions_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_positions_snapshot",
            "snapshot_ts": "2026-03-20T00-14-00Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "positions": [],
            "redeem_plan": {
                "cond-1": {
                    "condition_id": "cond-1",
                    "index_sets": [1],
                    "positions_count": 1,
                    "size_sum": 4.0,
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
    monkeypatch.setenv("POLYMARKET_PRIVATE_KEY", "pk")
    monkeypatch.setenv("RPC_URL", "")
    monkeypatch.setenv("POLYGON_RPC", "")
    monkeypatch.setenv("POLYGON_RPC_URL", "")
    monkeypatch.setenv("WEB3_PROVIDER_URI", "")
    monkeypatch.setenv("RPC_URL_BACKUPS", "")
    monkeypatch.setenv("POLYGON_RPC_BACKUPS", "")
    monkeypatch.setenv("RPC_FALLBACKS", "")
    monkeypatch.setenv("POLYGON_RPC_FALLBACKS", "")
    monkeypatch.setenv("BUILDER_API_KEY", "")
    monkeypatch.setenv("BUILDER_SECRET", "")
    monkeypatch.setenv("BUILDER_PASS_PHRASE", "")

    payload = apply_redeem_policy(
        cfg,
        persist=False,
        refresh_account_state=False,
        dry_run=False,
        gateway=None,
    )

    assert payload["status"] == "skipped"
    assert payload["reason"] == "missing_redeem_relay_config"


def test_submit_execution_payload_places_order(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(monkeypatch, ["2026-03-20T00-08-10Z"])
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )

    payload = submit_execution_payload(
        cfg,
        execution_payload={
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "market": "sol",
            "profile": "deep_otm",
            "cycle": "15m",
            "target": "direction",
            "execution": {
                "status": "plan",
                "reason": None,
                "selected_offset": 7,
                "selected_side": "UP",
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "market_id": "market-1",
                "token_id": "token-up",
                "order_type": "FAK",
                "entry_price": 0.20,
                "requested_notional_usd": 1.0,
                "requested_shares": 5.0,
                "repriced_metrics": {
                    "repriced_roi_net": 0.12,
                    "repriced_roi_threshold_required": 0.0,
                },
            },
        },
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert payload["status"] == "ok"
    assert payload["reason"] == "order_submitted"
    assert payload["trading_gateway"]["adapter"] == "fake"
    assert payload["order_request"]["order_kind"] == "market"
    assert payload["order_response"]["order_id"] == "order-123"
    assert "latest_order_action_path" in payload


def test_submit_execution_payload_skips_duplicate_success(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-00-00Z", "2026-03-20T00-00-01Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    execution_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }

    first = submit_execution_payload(
        cfg,
        execution_payload=execution_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )
    second = submit_execution_payload(
        cfg,
        execution_payload=execution_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert first["status"] == "ok"
    assert first["attempt"] == 1
    assert second["status"] == "skipped"
    assert second["reason"] == "action_already_succeeded"
    assert second["attempt"] == 1
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_uses_explicit_window_bounds(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setattr("pm15min.live.actions.utc_snapshot_label", lambda: "2026-03-20T00-08-10Z")
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )

    payload = submit_execution_payload(
        cfg,
        execution_payload={
            "snapshot_ts": "2026-03-20T00-00-01Z",
            "market": "sol",
            "profile": "deep_otm",
            "cycle": "15m",
            "target": "direction",
            "execution": {
                "status": "plan",
                "reason": None,
                "selected_offset": 7,
                "selected_side": "UP",
                "decision_ts": "2026-03-20T00:07:00+00:00",
                "window_start_ts": "2026-03-20T00:08:00+00:00",
                "window_end_ts": "2026-03-20T00:09:00+00:00",
                "window_duration_seconds": 60.0,
                "market_id": "market-1",
                "token_id": "token-up",
                "order_type": "FAK",
                "entry_price": 0.20,
                "requested_notional_usd": 1.0,
                "requested_shares": 5.0,
                "repriced_metrics": {
                    "repriced_roi_net": 0.12,
                    "repriced_roi_threshold_required": 0.0,
                },
            },
        },
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert payload["status"] == "ok"
    assert payload["reason"] == "order_submitted"
    assert payload["decision_window_source"] == "explicit_window"
    assert payload["decision_window_start_ts"] == "2026-03-20T00:08:00+00:00"
    assert payload["decision_window_end_ts"] == "2026-03-20T00:09:00+00:00"
    assert payload["decision_age_seconds"] == 10.0
    assert payload["decision_window_remaining_seconds"] == 50.0
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_serializes_duplicate_submit_under_lock(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    monkeypatch.setattr("pm15min.live.actions.utc_snapshot_label", lambda: "2026-03-20T00-08-10Z")
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    entered_event = threading.Event()
    release_event = threading.Event()

    class BlockingGateway(FakeGateway):
        def place_order(self, request):
            self.place_order_calls.append(request)
            entered_event.set()
            if not release_event.wait(timeout=2.0):
                raise RuntimeError("lock_test_timeout")
            return self.place_order_result

    gateway = BlockingGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    execution_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "window_start_ts": "2026-03-20T00:08:00+00:00",
            "window_end_ts": "2026-03-20T00:09:00+00:00",
            "window_duration_seconds": 60.0,
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }
    results: list[dict[str, object] | None] = [None, None]

    def _submit(idx: int) -> None:
        results[idx] = submit_execution_payload(
            cfg,
            execution_payload=execution_payload,
            persist=True,
            refresh_account_state=False,
            dry_run=False,
            gateway=gateway,
        )

    first = threading.Thread(target=_submit, args=(0,))
    second = threading.Thread(target=_submit, args=(1,))
    first.start()
    assert entered_event.wait(timeout=2.0)
    second.start()
    time.sleep(0.1)
    assert len(gateway.place_order_calls) == 1
    release_event.set()
    first.join(timeout=2.0)
    second.join(timeout=2.0)

    assert all(result is not None for result in results)
    reasons = {str(result["reason"]) for result in results if result is not None}
    statuses = {str(result["status"]) for result in results if result is not None}
    assert reasons == {"order_submitted", "action_already_succeeded"}
    assert statuses == {"ok", "skipped"}
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_preserves_duplicate_guard_across_no_action_cycles(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-00-00Z", "2026-03-20T00-00-01Z", "2026-03-20T00-00-02Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    planned_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }
    no_action_payload = {
        **planned_payload,
        "execution": {
            "status": "no_action",
            "reason": "decision_reject",
            "selected_offset": None,
            "selected_side": None,
            "decision_ts": None,
            "market_id": None,
            "token_id": None,
            "order_type": None,
            "entry_price": None,
            "requested_notional_usd": None,
            "requested_shares": None,
            "repriced_metrics": {},
        },
    }

    first = submit_execution_payload(
        cfg,
        execution_payload=planned_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )
    skipped = submit_execution_payload(
        cfg,
        execution_payload=no_action_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )
    second = submit_execution_payload(
        cfg,
        execution_payload=planned_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert first["status"] == "ok"
    assert skipped["status"] == "skipped"
    assert skipped["reason"] == "execution_not_plan:no_action"
    assert second["status"] == "skipped"
    assert second["reason"] == "action_already_succeeded"
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_uses_session_gate_state_for_nonconsecutive_duplicates(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-00-00Z", "2026-03-20T00-00-01Z", "2026-03-20T00-00-02Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    session_state: dict[str, object] = {}
    first_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }
    second_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 8,
            "selected_side": "DOWN",
            "decision_ts": "2026-03-20T00:09:00+00:00",
            "market_id": "market-1",
            "token_id": "token-down",
            "order_type": "FAK",
            "entry_price": 0.18,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.5555555556,
            "repriced_metrics": {
                "repriced_roi_net": 0.10,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }

    first = submit_execution_payload(
        cfg,
        execution_payload=first_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        session_state=session_state,
        gateway=gateway,
    )
    second = submit_execution_payload(
        cfg,
        execution_payload=second_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        session_state=session_state,
        gateway=gateway,
    )
    third = submit_execution_payload(
        cfg,
        execution_payload=first_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        session_state=session_state,
        gateway=gateway,
    )

    assert first["status"] == "ok"
    assert second["status"] == "ok"
    assert third["status"] == "skipped"
    assert third["reason"] == "action_already_succeeded"
    assert len(gateway.place_order_calls) == 2


def test_submit_execution_payload_throttles_recent_failure(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-00-00Z", "2026-03-20T00-00-00Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(place_order_exc=RuntimeError("boom"))
    execution_payload = {
        "snapshot_ts": "2026-03-20T00-00-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }

    first = submit_execution_payload(
        cfg,
        execution_payload=execution_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )
    second = submit_execution_payload(
        cfg,
        execution_payload=execution_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert first["status"] == "error"
    assert first["reason"] == "place_order_exception"
    assert first["order_response"]["message"] == "RuntimeError: boom"
    assert second["status"] == "skipped"
    assert second["reason"] == "action_retry_throttled_recent_failure"
    assert second["attempt"] == 1
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_uses_stable_window_action_key_across_reprices(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-08-05Z", "2026-03-20T00-08-10Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    first_payload = {
        "snapshot_ts": "2026-03-20T00-08-01Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "window_start_ts": "2026-03-20T00:08:00+00:00",
            "window_end_ts": "2026-03-20T00:09:00+00:00",
            "window_duration_seconds": 60.0,
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }
    second_payload = {
        **first_payload,
        "execution": {
            **first_payload["execution"],
            "entry_price": 0.21,
            "requested_shares": 4.7619047619,
        },
    }

    first = submit_execution_payload(
        cfg,
        execution_payload=first_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        session_state={},
        gateway=gateway,
    )
    second = submit_execution_payload(
        cfg,
        execution_payload=second_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        session_state={},
        gateway=gateway,
    )

    assert first["status"] == "ok"
    assert second["status"] == "skipped"
    assert second["reason"] == "action_already_succeeded"
    assert first["action_key"] == second["action_key"]
    assert len(gateway.place_order_calls) == 1


def test_submit_execution_payload_skips_stale_decision(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(monkeypatch, ["2026-03-20T00-10-10Z"])
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    gateway = FakeGateway(
        place_order_result=PlaceOrderResult(success=True, status="live", order_id="order-123")
    )
    execution_payload = {
        "snapshot_ts": "2026-03-20T00-10-00Z",
        "market": "sol",
        "profile": "deep_otm",
        "cycle": "15m",
        "target": "direction",
        "execution": {
            "status": "plan",
            "reason": None,
            "selected_offset": 7,
            "selected_side": "UP",
            "decision_ts": "2026-03-20T00:08:00+00:00",
            "window_start_ts": "2026-03-20T00:07:00+00:00",
            "window_end_ts": "2026-03-20T00:08:00+00:00",
            "window_duration_seconds": 60.0,
            "market_id": "market-1",
            "token_id": "token-up",
            "order_type": "FAK",
            "entry_price": 0.20,
            "requested_notional_usd": 1.0,
            "requested_shares": 5.0,
            "repriced_metrics": {
                "repriced_roi_net": 0.12,
                "repriced_roi_threshold_required": 0.0,
            },
        },
    }

    payload = submit_execution_payload(
        cfg,
        execution_payload=execution_payload,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert payload["status"] == "skipped"
    assert payload["reason"] == "decision_stale"
    assert payload["decision_window_start_ts"] == "2026-03-20T00:07:00+00:00"
    assert payload["decision_window_end_ts"] == "2026-03-20T00:08:00+00:00"
    assert payload["decision_age_seconds"] == 190.0
    assert payload["decision_window_remaining_seconds"] == 0.0
    assert payload["max_decision_age_seconds"] == 60.0
    assert len(gateway.place_order_calls) == 0


def test_apply_cancel_policy_throttles_recent_failure(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-14-00Z", "2026-03-20T00-14-00Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)
    cycle_start = pd.Timestamp("2026-03-20T00:00:00Z")
    cycle_end = pd.Timestamp("2026-03-20T00:15:00Z")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": int(cycle_start.timestamp()),
                    "cycle_end_ts": int(cycle_end.timestamp()),
                    "token_up": "token-up",
                    "token_down": "token-down",
                    "question": "Sol up or down",
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    persist_open_orders_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_open_orders_snapshot",
            "snapshot_ts": "2026-03-20T00-14-00Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "orders": [
                {"order_id": "order-1", "market_id": "market-1", "token_id": "token-up"},
            ],
            "summary": {"total_orders": 1, "by_market_id": {"market-1": 1}, "by_token_id": {}},
        },
    )
    gateway = FakeGateway(cancel_order_exc=RuntimeError("cancel boom"))

    first = apply_cancel_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        now=pd.Timestamp("2026-03-20T00:14:00Z"),
        gateway=gateway,
    )
    second = apply_cancel_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        now=pd.Timestamp("2026-03-20T00:14:00Z"),
        gateway=gateway,
    )

    assert first["status"] == "error"
    assert "RuntimeError: cancel boom" in first["results"][0]["reason"]
    assert second["status"] == "skipped"
    assert second["reason"] == "action_retry_throttled_recent_failure"
    assert second["attempt"] == 1
    assert len(gateway.cancel_order_calls) == 1


def test_apply_redeem_policy_skips_duplicate_success(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    _patch_v2_roots(monkeypatch, root)
    _patch_action_snapshot_labels(
        monkeypatch,
        ["2026-03-20T00-14-00Z", "2026-03-20T00-14-01Z"],
    )
    cfg = LiveConfig.build(market="sol", profile="deep_otm", cycle_minutes=15)
    persist_positions_snapshot(
        rewrite_root=root,
        payload={
            "domain": "live",
            "dataset": "live_positions_snapshot",
            "snapshot_ts": "2026-03-20T00-14-00Z",
            "market": "sol",
            "status": "ok",
            "reason": None,
            "positions": [],
            "redeem_plan": {
                "cond-1": {
                    "condition_id": "cond-1",
                    "index_sets": [1, 2],
                    "positions_count": 2,
                    "size_sum": 8.0,
                    "current_value_sum": 2.4,
                    "cash_pnl_sum": 0.0,
                }
            },
            "summary": {
                "total_positions": 2,
                "redeemable_positions": 2,
                "redeemable_conditions": 1,
            },
        },
    )
    gateway = FakeGateway(
        redeem_result=RedeemResult(success=True, status="confirmed", tx_hash="0xtx", state="confirmed")
    )

    first = apply_redeem_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )
    second = apply_redeem_policy(
        cfg,
        persist=True,
        refresh_account_state=False,
        dry_run=False,
        gateway=gateway,
    )

    assert first["status"] == "ok"
    assert first["attempt"] == 1
    assert second["status"] == "skipped"
    assert second["reason"] == "action_already_succeeded"
    assert second["attempt"] == 1
    assert len(gateway.redeem_calls) == 1
