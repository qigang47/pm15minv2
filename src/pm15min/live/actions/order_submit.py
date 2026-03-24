from __future__ import annotations

from typing import Any

from ..account import build_account_state_snapshot
from .builders import build_action_key, build_order_request_from_execution
from .gate import apply_gate_context, evaluate_action_gate, record_attempt_result
from .persistence import persist_order_payload
from ..profiles import resolve_live_profile_spec
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env_if_ready,
    build_place_order_request_from_payload,
    describe_live_trading_gateway,
)


def submit_execution_payload(
    cfg,
    *,
    execution_payload: dict[str, Any],
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    gateway: LiveTradingGateway | None = None,
    utc_snapshot_label_fn,
) -> dict[str, Any]:
    cycle = str(execution_payload.get("cycle") or f"{int(cfg.cycle_minutes)}m")
    target = str(execution_payload.get("target") or "direction")
    snapshot_ts = utc_snapshot_label_fn()
    execution = execution_payload.get("execution") or {}
    execution_status = str(execution.get("status") or "")
    gateway_meta = describe_live_trading_gateway(gateway)

    payload = {
        "domain": "live",
        "dataset": "live_order_action",
        "snapshot_ts": snapshot_ts,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": cycle,
        "target": target,
        "dry_run": bool(dry_run),
        "refresh_account_state": bool(refresh_account_state),
        "trading_gateway": gateway_meta,
        "execution_snapshot_ts": execution_payload.get("snapshot_ts"),
        "execution_snapshot_path": execution_payload.get("execution_snapshot_path"),
        "latest_execution_path": execution_payload.get("latest_execution_path"),
        "execution_status": execution_status,
        "execution_reason": execution.get("reason"),
        "status": "skipped",
        "reason": None,
        "action_key": None,
        "action_signature": None,
        "attempt": 0,
        "attempted": False,
        "last_attempt_snapshot_ts": None,
        "last_attempt_status": None,
        "last_attempt_reason": None,
        "gate": None,
        "order_request": None,
        "order_response": None,
        "post_submit_account_state": None,
    }
    if execution_status != "plan":
        payload["reason"] = f"execution_not_plan:{execution_status or 'missing'}"
        return persist_order_payload(cfg=cfg, payload=payload, persist=persist)

    order_request, request_error = build_order_request_from_execution(execution=execution)
    if request_error is not None:
        payload["reason"] = request_error
        return persist_order_payload(cfg=cfg, payload=payload, persist=persist)
    payload["order_request"] = order_request
    payload["action_signature"] = dict(order_request)
    payload["action_key"] = build_action_key(payload["action_signature"])

    if gateway is None:
        resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(require_auth=True)
        if resolved_gateway is None:
            payload["reason"] = missing_reason or "missing_polymarket_private_key"
            return persist_order_payload(cfg=cfg, payload=payload, persist=persist)
    else:
        resolved_gateway = gateway

    spec = resolve_live_profile_spec(cfg.profile)
    gate = evaluate_action_gate(
        cfg=cfg,
        action_type="order",
        cycle=cycle,
        target=target,
        spec=spec,
        action_key=str(payload["action_key"]),
        snapshot_ts=snapshot_ts,
        dry_run=dry_run,
    )
    apply_gate_context(payload=payload, gate=gate)
    if gate["decision"] == "skip":
        payload["reason"] = gate["reason"]
        return persist_order_payload(cfg=cfg, payload=payload, persist=persist)

    if dry_run:
        payload["status"] = "ok"
        payload["reason"] = "dry_run"
        payload["order_response"] = {
            "success": True,
            "status": "dry_run",
            "order_id": None,
            "message": None,
        }
        return persist_order_payload(cfg=cfg, payload=payload, persist=persist)

    try:
        response = resolved_gateway.place_order(build_place_order_request_from_payload(order_request)).to_dict()
    except Exception as exc:
        response = {
            "success": False,
            "status": "place_order_exception",
            "order_id": None,
            "message": f"{type(exc).__name__}: {exc}",
        }
    payload["order_response"] = response
    success = bool(response.get("success"))
    payload["status"] = "ok" if success else "error"
    payload["reason"] = "order_submitted" if success else (response.get("status") or "place_order_failed")
    record_attempt_result(payload=payload)
    if success and refresh_account_state:
        try:
            account_state = build_account_state_snapshot(cfg, persist=persist, gateway=gateway)
        except Exception as exc:
            payload["post_submit_account_state"] = {
                "status": "error",
                "reason": f"{type(exc).__name__}: {exc}",
            }
        else:
            payload["post_submit_account_state"] = {
                "snapshot_ts": account_state.get("snapshot_ts"),
                "open_orders_status": (account_state.get("open_orders") or {}).get("status"),
                "positions_status": (account_state.get("positions") or {}).get("status"),
            }
    return persist_order_payload(cfg=cfg, payload=payload, persist=persist)
