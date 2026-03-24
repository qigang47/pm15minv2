from __future__ import annotations

from typing import Any


def build_runner_risk_summary(
    *,
    foundation_summary: dict[str, Any] | None,
    liquidity_state_payload: dict[str, Any] | None,
    decision_payload: dict[str, Any],
    execution_payload: dict[str, Any],
    order_action_payload: dict[str, Any] | None,
    account_state_payload: dict[str, Any] | None,
    cancel_action_payload: dict[str, Any] | None,
    redeem_action_payload: dict[str, Any] | None,
    apply_side_effects: bool,
    side_effect_dry_run: bool,
    account_state_status_fn,
) -> dict[str, object]:
    liquidity_state = None if liquidity_state_payload is None else {
        "status": liquidity_state_payload.get("status"),
        "blocked": liquidity_state_payload.get("blocked"),
        "reason": liquidity_state_payload.get("reason"),
    }
    regime_state = decision_payload.get("regime_state") or {}
    decision = decision_payload.get("decision") or {}
    execution = execution_payload.get("execution") or {}
    rejected_offsets = decision_payload.get("rejected_offsets") or []
    top_reject_reasons: list[str] = []
    if rejected_offsets:
        first_reject = rejected_offsets[0] if isinstance(rejected_offsets[0], dict) else {}
        top_reject_reasons = list(first_reject.get("guard_reasons") or [])
    return {
        "foundation": None if foundation_summary is None else {
            "status": foundation_summary.get("status"),
            "reason": foundation_summary.get("reason"),
            "issue_codes": list(foundation_summary.get("issue_codes") or []),
            "degraded_tasks": list(foundation_summary.get("degraded_tasks") or []),
            "run_started_at": foundation_summary.get("run_started_at"),
            "last_completed_at": foundation_summary.get("last_completed_at"),
            "finished_at": foundation_summary.get("finished_at"),
            "completed_iterations": foundation_summary.get("completed_iterations"),
        },
        "liquidity": liquidity_state,
        "regime": {
            "state": regime_state.get("state"),
            "pressure": regime_state.get("pressure"),
            "reason_codes": list(regime_state.get("reason_codes") or []),
        },
        "decision": {
            "status": decision.get("status"),
            "selected_offset": decision.get("selected_offset"),
            "selected_side": decision.get("selected_side"),
            "top_reject_reasons": top_reject_reasons,
        },
        "execution": {
            "status": execution.get("status"),
            "reason": execution.get("reason"),
            "stake_multiplier": execution.get("stake_multiplier"),
            "requested_notional_usd": execution.get("requested_notional_usd"),
        },
        "side_effects": {
            "enabled": bool(apply_side_effects),
            "dry_run": bool(side_effect_dry_run),
            "order_status": None if order_action_payload is None else order_action_payload.get("status"),
            "order_reason": None if order_action_payload is None else order_action_payload.get("reason"),
            "account_state_status": account_state_status_fn(account_state_payload),
            "account_open_orders_status": None if account_state_payload is None else (account_state_payload.get("open_orders") or {}).get("status"),
            "account_positions_status": None if account_state_payload is None else (account_state_payload.get("positions") or {}).get("status"),
            "cancel_status": None if cancel_action_payload is None else cancel_action_payload.get("status"),
            "cancel_reason": None if cancel_action_payload is None else cancel_action_payload.get("reason"),
            "redeem_status": None if redeem_action_payload is None else redeem_action_payload.get("status"),
            "redeem_reason": None if redeem_action_payload is None else redeem_action_payload.get("reason"),
        },
    }


def build_runner_risk_alerts(
    *,
    risk_summary: dict[str, object],
    runner_health: dict[str, object],
) -> list[dict[str, object]]:
    alerts: list[dict[str, object]] = []
    foundation = risk_summary.get("foundation") or {}
    liquidity = risk_summary.get("liquidity") or {}
    regime = risk_summary.get("regime") or {}
    decision = risk_summary.get("decision") or {}
    execution = risk_summary.get("execution") or {}
    side_effects = risk_summary.get("side_effects") or {}

    foundation_status = str(foundation.get("status") or "").lower()
    if foundation_status == "error":
        alerts.append({"severity": "critical", "code": "foundation_not_ok", "detail": foundation.get("reason")})
    elif foundation_status == "ok_with_errors":
        alerts.append({"severity": "warning", "code": "foundation_ok_with_errors", "detail": foundation.get("reason")})
    liquidity_status = str(liquidity.get("status") or "").lower()
    if liquidity_status == "error":
        alerts.append({"severity": "critical", "code": "liquidity_state_error", "detail": liquidity.get("reason")})
    if bool(liquidity.get("blocked")):
        alerts.append({"severity": "critical", "code": "liquidity_blocked", "detail": liquidity.get("reason")})
    if str(regime.get("state") or "").upper() == "DEFENSE":
        alerts.append({"severity": "warning", "code": "regime_defense", "detail": {"pressure": regime.get("pressure"), "reason_codes": list(regime.get("reason_codes") or [])}})
    if str(decision.get("status") or "").lower() == "reject":
        alerts.append({"severity": "warning", "code": "decision_reject", "detail": list(decision.get("top_reject_reasons") or [])})
    execution_status = str(execution.get("status") or "").lower()
    if execution_status in {"blocked", "no_action"}:
        alerts.append({"severity": "warning", "code": f"execution_{execution_status}", "detail": execution.get("reason")})
    elif execution_status == "error":
        alerts.append({"severity": "critical", "code": "execution_error", "detail": execution.get("reason")})
    if str(side_effects.get("order_status") or "").lower() == "error":
        alerts.append({"severity": "critical", "code": "order_action_error", "detail": side_effects.get("order_reason")})
    if str(side_effects.get("account_state_status") or "").lower() == "error":
        alerts.append({"severity": "critical", "code": "account_state_sync_error", "detail": {"open_orders_status": side_effects.get("account_open_orders_status"), "positions_status": side_effects.get("account_positions_status")}})
    if str(side_effects.get("cancel_status") or "").lower() in {"error", "ok_with_errors"}:
        alerts.append({"severity": "warning", "code": "cancel_action_error", "detail": side_effects.get("cancel_reason")})
    if str(side_effects.get("redeem_status") or "").lower() in {"error", "ok_with_errors"}:
        alerts.append({"severity": "warning", "code": "redeem_action_error", "detail": side_effects.get("redeem_reason")})
    if str(runner_health.get("post_side_effect_status") or "").lower() == "dry_run":
        alerts.append({"severity": "info", "code": "side_effects_dry_run", "detail": None})
    return alerts


def summarize_runner_risk_alerts(*, alerts: list[dict[str, object]]) -> dict[str, object]:
    counts = {"critical": 0, "warning": 0, "info": 0, "other": 0, "total": 0}
    rank = {"critical": 3, "warning": 2, "info": 1, "other": 0}
    highest = "none"
    highest_rank = -1
    for alert in alerts:
        severity = str(alert.get("severity") or "other").strip().lower() or "other"
        bucket = severity if severity in {"critical", "warning", "info"} else "other"
        counts[bucket] += 1
        counts["total"] += 1
        severity_rank = rank.get(bucket, 0)
        if severity_rank > highest_rank:
            highest_rank = severity_rank
            highest = bucket
    return {
        "counts": counts,
        "highest_severity": highest,
        "has_critical": counts["critical"] > 0,
    }
