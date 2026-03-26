from __future__ import annotations

import json
from typing import Any

from .builders import float_or_none, int_or_none, snapshot_label_to_timestamp
from ..layout import LiveStateLayout


ACTION_SUCCESS_STATUSES = {"ok"}
ACTION_RETRYABLE_STATUSES = {"error", "ok_with_errors"}
SESSION_ACTION_STATE_MAX_KEYS = 256


def evaluate_action_gate(
    *,
    cfg,
    action_type: str,
    cycle: str,
    target: str | None,
    spec,
    action_key: str | None,
    snapshot_ts: str,
    dry_run: bool,
    session_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    retry_interval_seconds, max_attempts = action_retry_budget(spec=spec, action_type=action_type)
    previous_context = extract_previous_attempt_context(
        previous_payload=load_session_action_payload(
            session_state=session_state,
            action_type=action_type,
            action_key=action_key,
        ),
        action_key=action_key,
    )
    if not previous_context["same_key"]:
        previous_payload = load_latest_action_payload(
            cfg=cfg,
            action_type=action_type,
            cycle=cycle,
            target=target,
        )
        previous_context = extract_previous_attempt_context(previous_payload=previous_payload, action_key=action_key)
    if dry_run:
        return {
            **previous_context,
            "decision": "bypass",
            "reason": "dry_run",
            "retry_interval_seconds": retry_interval_seconds,
            "max_attempts": max_attempts,
            "previous_attempt": previous_context["attempt"],
            "current_attempt": previous_context["attempt"],
        }

    age_seconds = seconds_since_snapshot(
        current_snapshot_ts=snapshot_ts,
        previous_snapshot_ts=previous_context["last_attempt_snapshot_ts"],
    )
    decision = "allow"
    reason = "first_attempt"
    attempt = 1
    if previous_context["same_key"] and previous_context["attempt"] > 0:
        attempt = int(previous_context["attempt"])
        last_status = str(previous_context["last_attempt_status"] or "")
        if last_status in ACTION_SUCCESS_STATUSES:
            decision = "skip"
            reason = "action_already_succeeded"
        elif attempt >= max_attempts:
            decision = "skip"
            reason = "action_retry_exhausted"
        elif age_seconds is not None and age_seconds < retry_interval_seconds:
            decision = "skip"
            reason = "action_retry_throttled_recent_failure"
        else:
            decision = "allow"
            reason = "retry_allowed"
            attempt += 1
    return {
        **previous_context,
        "decision": decision,
        "reason": reason,
        "retry_interval_seconds": retry_interval_seconds,
        "max_attempts": max_attempts,
        "age_seconds_since_last_attempt": age_seconds,
        "attempt": attempt,
        "previous_attempt": previous_context["attempt"],
        "current_attempt": attempt,
    }


def action_retry_budget(*, spec, action_type: str) -> tuple[float, int]:
    if action_type == "cancel":
        interval_seconds = float_or_none(getattr(spec, "fast_retry_interval_seconds", None))
    else:
        interval_seconds = float_or_none(getattr(spec, "order_retry_interval_seconds", None))
    max_attempts = int_or_none(getattr(spec, "max_order_retries", None))
    return max(0.0, interval_seconds or 0.0), max(1, max_attempts or 1)


def load_latest_action_payload(
    *,
    cfg,
    action_type: str,
    cycle: str,
    target: str | None,
) -> dict[str, Any] | None:
    layout = LiveStateLayout.discover(root=cfg.layout.rewrite.root)
    if action_type == "order":
        path = layout.latest_order_action_path(
            market=cfg.asset.slug,
            cycle=cycle,
            profile=cfg.profile,
            target=str(target or "direction"),
        )
    elif action_type == "cancel":
        path = layout.latest_cancel_action_path(
            market=cfg.asset.slug,
            cycle=cycle,
            profile=cfg.profile,
        )
    elif action_type == "redeem":
        path = layout.latest_redeem_action_path(
            market=cfg.asset.slug,
            cycle=cycle,
            profile=cfg.profile,
        )
    else:
        return None
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def extract_previous_attempt_context(
    *,
    previous_payload: dict[str, Any] | None,
    action_key: str | None,
) -> dict[str, Any]:
    if not isinstance(previous_payload, dict):
        return {
            "same_key": False,
            "attempt": 0,
            "last_attempt_snapshot_ts": None,
            "last_attempt_status": None,
            "last_attempt_reason": None,
        }
    previous_key = previous_payload.get("action_key")
    if not action_key or not previous_key or str(previous_key) != str(action_key):
        return {
            "same_key": False,
            "attempt": 0,
            "last_attempt_snapshot_ts": None,
            "last_attempt_status": None,
            "last_attempt_reason": None,
        }
    attempt = int_or_none(previous_payload.get("attempt")) or 0
    last_attempt_snapshot_ts = previous_payload.get("last_attempt_snapshot_ts")
    last_attempt_status = previous_payload.get("last_attempt_status")
    last_attempt_reason = previous_payload.get("last_attempt_reason")
    dry_run = bool(previous_payload.get("dry_run"))
    status = str(previous_payload.get("status") or "")
    if attempt <= 0 and not dry_run and status in ACTION_SUCCESS_STATUSES.union(ACTION_RETRYABLE_STATUSES):
        attempt = 1
    if last_attempt_snapshot_ts is None and attempt > 0:
        last_attempt_snapshot_ts = previous_payload.get("snapshot_ts")
    if last_attempt_status is None and attempt > 0:
        last_attempt_status = status or None
    if last_attempt_reason is None and attempt > 0:
        last_attempt_reason = previous_payload.get("reason")
    return {
        "same_key": True,
        "attempt": attempt,
        "last_attempt_snapshot_ts": last_attempt_snapshot_ts,
        "last_attempt_status": last_attempt_status,
        "last_attempt_reason": last_attempt_reason,
    }


def apply_gate_context(*, payload: dict[str, Any], gate: dict[str, Any]) -> None:
    payload["attempt"] = int(gate.get("attempt") or 0)
    payload["last_attempt_snapshot_ts"] = gate.get("last_attempt_snapshot_ts")
    payload["last_attempt_status"] = gate.get("last_attempt_status")
    payload["last_attempt_reason"] = gate.get("last_attempt_reason")
    payload["gate"] = {
        "decision": gate.get("decision"),
        "reason": gate.get("reason"),
        "same_key": bool(gate.get("same_key")),
        "retry_interval_seconds": gate.get("retry_interval_seconds"),
        "max_attempts": gate.get("max_attempts"),
        "age_seconds_since_last_attempt": gate.get("age_seconds_since_last_attempt"),
        "previous_attempt": gate.get("previous_attempt") if gate.get("same_key") else 0,
        "current_attempt": gate.get("current_attempt"),
        "previous_last_attempt_snapshot_ts": gate.get("last_attempt_snapshot_ts"),
        "previous_last_attempt_status": gate.get("last_attempt_status"),
        "previous_last_attempt_reason": gate.get("last_attempt_reason"),
    }


def persist_session_action_payload(
    *,
    session_state: dict[str, Any] | None,
    action_type: str,
    payload: dict[str, Any],
) -> None:
    if not isinstance(session_state, dict):
        return
    action_key = str(payload.get("action_key") or "").strip()
    if not action_key or bool(payload.get("dry_run")):
        return
    state = _session_action_state_map(session_state=session_state, action_type=action_type)
    if action_key in state:
        state.pop(action_key, None)
    state[action_key] = {
        "action_key": action_key,
        "snapshot_ts": payload.get("snapshot_ts"),
        "status": payload.get("status"),
        "reason": payload.get("reason"),
        "attempt": int_or_none(payload.get("attempt")) or 0,
        "last_attempt_snapshot_ts": payload.get("last_attempt_snapshot_ts"),
        "last_attempt_status": payload.get("last_attempt_status"),
        "last_attempt_reason": payload.get("last_attempt_reason"),
        "dry_run": bool(payload.get("dry_run")),
    }
    while len(state) > SESSION_ACTION_STATE_MAX_KEYS:
        oldest_key = next(iter(state))
        state.pop(oldest_key, None)


def record_attempt_result(*, payload: dict[str, Any]) -> None:
    payload["attempted"] = True
    payload["last_attempt_snapshot_ts"] = payload.get("snapshot_ts")
    payload["last_attempt_status"] = payload.get("status")
    payload["last_attempt_reason"] = payload.get("reason")


def seconds_since_snapshot(*, current_snapshot_ts: object, previous_snapshot_ts: object) -> float | None:
    current_ts = snapshot_label_to_timestamp(current_snapshot_ts)
    previous_ts = snapshot_label_to_timestamp(previous_snapshot_ts)
    if current_ts is None or previous_ts is None:
        return None
    return max(0.0, float((current_ts - previous_ts).total_seconds()))


def load_session_action_payload(
    *,
    session_state: dict[str, Any] | None,
    action_type: str,
    action_key: str | None,
) -> dict[str, Any] | None:
    if not isinstance(session_state, dict):
        return None
    key = str(action_key or "").strip()
    if not key:
        return None
    state = _session_action_state_map(session_state=session_state, action_type=action_type)
    payload = state.get(key)
    return dict(payload) if isinstance(payload, dict) else None


def _session_action_state_map(
    *,
    session_state: dict[str, Any] | None,
    action_type: str,
) -> dict[str, Any]:
    if not isinstance(session_state, dict):
        return {}
    state = session_state.get("action_gate_state")
    if not isinstance(state, dict):
        state = {}
        session_state["action_gate_state"] = state
    action_map = state.get(action_type)
    if isinstance(action_map, dict):
        return action_map
    action_map = {}
    state[action_type] = action_map
    return action_map
