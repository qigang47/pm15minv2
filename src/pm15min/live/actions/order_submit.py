from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any

from ..account import build_account_state_snapshot
from .builders import (
    build_action_key,
    build_order_action_signature,
    build_order_request_from_execution,
    snapshot_label_to_timestamp,
)
from .gate import (
    apply_gate_context,
    evaluate_action_gate,
    persist_session_action_payload,
    record_attempt_result,
)
from ..layout import LiveStateLayout
from .persistence import persist_order_payload
from ..profiles import resolve_live_profile_spec
from ..trading.gateway import LiveTradingGateway
from ..trading.service import (
    build_live_trading_gateway_from_env_if_ready,
    build_place_order_request_from_payload,
    describe_live_trading_gateway,
)


DEFAULT_MAX_DECISION_AGE_SECONDS = 60.0


def submit_execution_payload(
    cfg,
    *,
    execution_payload: dict[str, Any],
    persist: bool = True,
    refresh_account_state: bool = True,
    dry_run: bool = False,
    session_state: dict[str, Any] | None = None,
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
    payload["action_signature"] = build_order_action_signature(order_request=order_request)
    payload["action_key"] = build_action_key(payload["action_signature"])
    decision_window = _resolve_decision_window(
        order_request=order_request,
        snapshot_ts=snapshot_ts,
    )
    payload["decision_window_start_ts"] = decision_window["window_start_ts"]
    payload["decision_window_end_ts"] = decision_window["window_end_ts"]
    payload["decision_window_source"] = decision_window["source"]
    payload["decision_age_seconds"] = decision_window["elapsed_seconds"]
    payload["decision_window_remaining_seconds"] = decision_window["remaining_seconds"]
    payload["max_decision_age_seconds"] = decision_window["duration_seconds"]
    if bool(decision_window["stale"]):
        payload["reason"] = "decision_stale"
        return persist_order_payload(cfg=cfg, payload=payload, persist=persist)

    if gateway is None:
        resolved_gateway, missing_reason = build_live_trading_gateway_from_env_if_ready(require_auth=True)
        if resolved_gateway is None:
            payload["reason"] = missing_reason or "missing_polymarket_private_key"
            return persist_order_payload(cfg=cfg, payload=payload, persist=persist)
    else:
        resolved_gateway = gateway

    spec = resolve_live_profile_spec(cfg.profile)
    lock_path = _order_submission_lock_path(
        cfg=cfg,
        cycle=cycle,
        target=target,
        action_key=str(payload["action_key"] or ""),
    )
    payload["submission_lock_path"] = str(lock_path)
    with _exclusive_file_lock(lock_path):
        gate = evaluate_action_gate(
            cfg=cfg,
            action_type="order",
            cycle=cycle,
            target=target,
            spec=spec,
            action_key=str(payload["action_key"]),
            snapshot_ts=snapshot_ts,
            dry_run=dry_run,
            session_state=session_state,
        )
        apply_gate_context(payload=payload, gate=gate)
        if gate["decision"] == "skip":
            payload["reason"] = gate["reason"]
            persist_session_action_payload(session_state=session_state, action_type="order", payload=payload)
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
            persist_session_action_payload(session_state=session_state, action_type="order", payload=payload)
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
        persist_session_action_payload(session_state=session_state, action_type="order", payload=payload)
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


def _resolve_decision_window(
    *,
    order_request: dict[str, Any],
    snapshot_ts: object,
) -> dict[str, object]:
    window_start_ts = order_request.get("window_start_ts") or order_request.get("decision_ts")
    window_end_ts = order_request.get("window_end_ts")
    source = "explicit_window" if order_request.get("window_start_ts") or order_request.get("window_end_ts") else "decision_ts_fallback"
    window_start_dt = snapshot_label_to_timestamp(window_start_ts)
    window_end_dt = snapshot_label_to_timestamp(window_end_ts)
    snapshot_dt = snapshot_label_to_timestamp(snapshot_ts)
    duration_seconds = _window_duration_seconds(order_request=order_request, window_start_dt=window_start_dt, window_end_dt=window_end_dt)
    if window_end_dt is None and window_start_dt is not None:
        window_end_dt = window_start_dt + timedelta(seconds=float(duration_seconds))
    elapsed_seconds = _elapsed_seconds(window_start_dt=window_start_dt, snapshot_dt=snapshot_dt)
    remaining_seconds = _remaining_seconds(window_end_dt=window_end_dt, snapshot_dt=snapshot_dt)
    return {
        "window_start_ts": None if window_start_dt is None else window_start_dt.isoformat(),
        "window_end_ts": None if window_end_dt is None else window_end_dt.isoformat(),
        "duration_seconds": duration_seconds,
        "elapsed_seconds": elapsed_seconds,
        "remaining_seconds": remaining_seconds,
        "stale": bool(window_end_dt is not None and snapshot_dt is not None and snapshot_dt >= window_end_dt),
        "source": source,
    }


def _elapsed_seconds(*, window_start_dt, snapshot_dt) -> float | None:
    if window_start_dt is None or snapshot_dt is None:
        return None
    return max(0.0, float((snapshot_dt - window_start_dt).total_seconds()))


def _remaining_seconds(*, window_end_dt, snapshot_dt) -> float | None:
    if window_end_dt is None or snapshot_dt is None:
        return None
    return max(0.0, float((window_end_dt - snapshot_dt).total_seconds()))


def _window_duration_seconds(*, order_request: dict[str, Any], window_start_dt, window_end_dt) -> float:
    if window_start_dt is not None and window_end_dt is not None:
        return max(0.0, float((window_end_dt - window_start_dt).total_seconds()))
    raw = order_request.get("window_duration_seconds")
    try:
        if raw is not None:
            return max(0.0, float(raw))
    except Exception:
        pass
    return _max_decision_age_seconds()


@contextmanager
def _exclusive_file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _order_submission_lock_path(
    *,
    cfg,
    cycle: str,
    target: str,
    action_key: str,
) -> Path:
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    lock_name = str(action_key or "missing_action_key").strip() or "missing_action_key"
    return layout.order_action_state_dir(
        market=cfg.asset.slug,
        cycle=cycle,
        profile=cfg.profile,
        target=target,
    ) / "locks" / f"{lock_name}.lock"


def _max_decision_age_seconds() -> float:
    try:
        return max(0.0, float(os.getenv("PM15MIN_MAX_DECISION_AGE_SECONDS", str(DEFAULT_MAX_DECISION_AGE_SECONDS))))
    except Exception:
        return DEFAULT_MAX_DECISION_AGE_SECONDS
