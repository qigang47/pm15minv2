from __future__ import annotations

from typing import Any


def build_runner_health_summary(
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
) -> dict[str, object]:
    checks: list[dict[str, object]] = []

    def _add_check(*, stage: str, code: str, status: str, severity: str, blocking: bool, detail: object = None) -> None:
        checks.append({"stage": stage, "code": code, "status": status, "severity": severity, "blocking": bool(blocking), "detail": detail})

    foundation_status = status_token(None if foundation_summary is None else foundation_summary.get("status"))
    if foundation_summary is None:
        _add_check(stage="foundation", code="foundation_skipped", status="skipped", severity="info", blocking=False)
    elif foundation_status == "ok":
        _add_check(stage="foundation", code="foundation_ok", status="ok", severity="info", blocking=False)
    elif foundation_status == "ok_with_errors":
        _add_check(stage="foundation", code="foundation_ok_with_errors", status="warning", severity="warning", blocking=False, detail=foundation_summary.get("reason"))
    else:
        _add_check(stage="foundation", code="foundation_not_ok", status="error", severity="error", blocking=True, detail=None if foundation_summary is None else foundation_summary.get("reason"))

    liquidity_status = status_token(None if liquidity_state_payload is None else liquidity_state_payload.get("status"))
    liquidity_blocked = bool((liquidity_state_payload or {}).get("blocked"))
    if liquidity_status == "ok" and not liquidity_blocked:
        _add_check(stage="liquidity", code="liquidity_ready", status="ok", severity="info", blocking=False)
    elif liquidity_status == "error":
        _add_check(stage="liquidity", code="liquidity_state_error", status="error", severity="error", blocking=True, detail=(liquidity_state_payload or {}).get("reason"))
    elif liquidity_blocked:
        _add_check(stage="liquidity", code="liquidity_blocked", status="blocked", severity="error", blocking=True, detail=(liquidity_state_payload or {}).get("reason"))
    else:
        _add_check(stage="liquidity", code="liquidity_state_unavailable", status="error", severity="error", blocking=True, detail=(liquidity_state_payload or {}).get("reason"))

    decision_status = status_token((decision_payload.get("decision") or {}).get("status"))
    if decision_status == "accept":
        _add_check(stage="decision", code="decision_accept", status="ok", severity="info", blocking=False)
    elif decision_status == "reject":
        _add_check(stage="decision", code="decision_not_accept", status="blocked", severity="warning", blocking=True)
    else:
        _add_check(stage="decision", code="decision_status_error", status="error", severity="error", blocking=True, detail=decision_status)

    execution = execution_payload.get("execution") or {}
    execution_status = status_token(execution.get("status"))
    if execution_status == "plan":
        _add_check(stage="execution", code="execution_plan_ready", status="ok", severity="info", blocking=False)
    elif execution_status in {"blocked", "no_action"}:
        _add_check(stage="execution", code="execution_not_plan", status="blocked", severity="warning", blocking=True, detail=execution.get("reason"))
    else:
        _add_check(stage="execution", code="execution_status_error", status="error", severity="error", blocking=True, detail=execution.get("reason") or execution_status)

    if not apply_side_effects:
        post_side_effect_status = "disabled"
        _add_check(stage="side_effects", code="side_effects_disabled", status="disabled", severity="info", blocking=False)
    elif side_effect_dry_run:
        post_side_effect_status = "dry_run"
        _add_check(stage="side_effects", code="side_effects_dry_run", status="dry_run", severity="info", blocking=False)
    else:
        order_status = status_token(None if order_action_payload is None else order_action_payload.get("status"))
        if order_status in {None, "skipped"}:
            _add_check(stage="order", code="order_action_skipped", status="skipped", severity="info", blocking=False, detail=None if order_action_payload is None else order_action_payload.get("reason"))
        elif order_status == "ok":
            _add_check(stage="order", code="order_action_ok", status="ok", severity="info", blocking=False)
        else:
            _add_check(stage="order", code="order_action_error", status="error", severity="error", blocking=True, detail=None if order_action_payload is None else order_action_payload.get("reason"))

        account_status = account_state_status(account_state_payload)
        if account_status is None:
            _add_check(stage="account", code="account_state_skipped", status="skipped", severity="info", blocking=False)
        elif account_status == "ok":
            _add_check(stage="account", code="account_state_ok", status="ok", severity="info", blocking=False)
        else:
            _add_check(stage="account", code="account_state_sync_error", status="error", severity="error", blocking=True, detail={"open_orders_status": None if account_state_payload is None else (account_state_payload.get("open_orders") or {}).get("status"), "positions_status": None if account_state_payload is None else (account_state_payload.get("positions") or {}).get("status")})

        cancel_status = status_token(None if cancel_action_payload is None else cancel_action_payload.get("status"))
        if cancel_status in {None, "ok", "skipped"}:
            _add_check(stage="cancel", code="cancel_action_ok", status="ok" if cancel_status == "ok" else "skipped", severity="info", blocking=False)
        else:
            _add_check(stage="cancel", code="cancel_action_error", status="warning", severity="warning", blocking=False, detail=None if cancel_action_payload is None else cancel_action_payload.get("reason"))

        redeem_status = status_token(None if redeem_action_payload is None else redeem_action_payload.get("status"))
        if redeem_status in {None, "ok", "skipped"}:
            _add_check(stage="redeem", code="redeem_action_ok", status="ok" if redeem_status == "ok" else "skipped", severity="info", blocking=False)
        else:
            _add_check(stage="redeem", code="redeem_action_error", status="warning", severity="warning", blocking=False, detail=None if redeem_action_payload is None else redeem_action_payload.get("reason"))

        post_side_effect_status = aggregate_runner_health_status(checks, stages={"order", "account", "cancel", "redeem"})

    pre_side_effect_status = aggregate_runner_health_status(checks, stages={"foundation", "liquidity", "decision", "execution"})
    blocking_checks = [check for check in checks if bool(check.get("blocking")) and str(check.get("status")) in {"error", "blocked"}]
    warning_checks = [check for check in checks if str(check.get("status")) == "warning"]
    if blocking_checks:
        overall_status = "error" if any(str(check.get("status")) == "error" for check in blocking_checks) else "warning"
    elif warning_checks:
        overall_status = "warning"
    else:
        overall_status = "ok"
    primary_blocker = None if not blocking_checks else str(blocking_checks[0].get("code") or "")
    blocker_stage = None if not blocking_checks else str(blocking_checks[0].get("stage") or "")
    return {
        "overall_status": overall_status,
        "pre_side_effect_status": pre_side_effect_status,
        "post_side_effect_status": post_side_effect_status,
        "primary_blocker": primary_blocker,
        "blocker_stage": blocker_stage,
        "blocking_issue_count": len(blocking_checks),
        "warning_issue_count": len(warning_checks),
        "checks": checks,
    }


def aggregate_runner_health_status(checks: list[dict[str, object]], *, stages: set[str]) -> str:
    relevant = [check for check in checks if str(check.get("stage") or "") in stages]
    if not relevant:
        return "skipped"
    statuses = {str(check.get("status") or "") for check in relevant}
    if "error" in statuses:
        return "error"
    if "blocked" in statuses:
        return "blocked"
    if "warning" in statuses:
        return "warning"
    if "dry_run" in statuses:
        return "dry_run"
    if "disabled" in statuses:
        return "disabled"
    if statuses == {"skipped"}:
        return "skipped"
    return "ok"


def account_state_status(payload: dict[str, Any] | None) -> str | None:
    if payload is None:
        return None
    open_orders_status = status_token((payload.get("open_orders") or {}).get("status"))
    positions_status = status_token((payload.get("positions") or {}).get("status"))
    if open_orders_status == "ok" and positions_status == "ok":
        return "ok"
    if open_orders_status in {None, "skipped"} and positions_status in {None, "skipped"}:
        return "skipped"
    return "error"


def status_token(value: object) -> str | None:
    text = str(value or "").strip().lower()
    return text or None
