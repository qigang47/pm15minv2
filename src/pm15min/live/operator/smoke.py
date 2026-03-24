from __future__ import annotations


def build_live_operator_smoke_summary(
    *,
    canonical_scope: dict[str, object],
    gateway_payload: dict[str, object],
    runner_payload: dict[str, object],
    operator_summary: dict[str, object],
) -> dict[str, object]:
    failed_gateway_checks = [
        str(item.get("name") or "")
        for item in (gateway_payload.get("checks") or [])
        if isinstance(item, dict) and not bool(item.get("ok", False))
    ]
    failed_gateway_probes = [
        str(name)
        for name, row in (gateway_payload.get("probes") or {}).items()
        if isinstance(row, dict) and str(row.get("status") or "").lower() not in {"", "not_run"} and not bool(row.get("ok", False))
    ]
    runner_primary_blocker = str(operator_summary.get("primary_blocker") or "")
    reject_category = str(operator_summary.get("decision_reject_category") or "")
    runner_smoke_status = classify_live_runner_smoke_status(
        operator_summary=operator_summary,
        runner_status=runner_payload.get("status"),
        canonical_live_scope_ok=bool(canonical_scope.get("ok")),
    )

    if not bool(canonical_scope.get("ok")):
        status = "blocked"
        reason = "outside_canonical_live_scope"
    elif failed_gateway_checks:
        status = "blocked"
        reason = "gateway_checks_failed"
    elif failed_gateway_probes:
        status = "blocked"
        reason = "gateway_probes_failed"
    elif runner_smoke_status == "missing":
        status = "blocked"
        reason = "runner_missing"
    elif runner_smoke_status in {"infra_blocked", "data_blocked"}:
        status = "blocked"
        reason = "runner_infra_blocked" if runner_smoke_status == "infra_blocked" else "runner_data_blocked"
    elif runner_smoke_status == "foundation_warning_only":
        status = "operational"
        reason = "foundation_warning_only"
    elif runner_smoke_status == "strategy_only_blocked":
        status = "operational"
        reason = "strategy_reject_only"
    elif runner_smoke_status == "ok":
        status = "operational"
        reason = "path_operational"
    else:
        status = "unknown"
        reason = "smoke_status_unknown"

    return {
        "status": status,
        "reason": reason,
        "can_validate_real_side_effect_path": status == "operational",
        "gateway_check_failures": failed_gateway_checks,
        "gateway_probe_failures": failed_gateway_probes,
        "truth_runtime_status": operator_summary.get("truth_runtime_status"),
        "truth_runtime_reason": operator_summary.get("truth_runtime_reason"),
        "truth_runtime_window_refresh_status": operator_summary.get("truth_runtime_window_refresh_status"),
        "truth_runtime_oracle_status": operator_summary.get("truth_runtime_oracle_status"),
        "orderbook_hot_cache_status": operator_summary.get("orderbook_hot_cache_status"),
        "orderbook_hot_cache_reason": operator_summary.get("orderbook_hot_cache_reason"),
        "runner_smoke_status": runner_smoke_status,
        "runner_primary_blocker": runner_primary_blocker or None,
        "runner_decision_reject_category": reject_category or None,
        "runner_decision_reject_interpretation": operator_summary.get("decision_reject_interpretation"),
        "runner_snapshot_ts": runner_payload.get("last_iteration_snapshot_ts"),
    }


def classify_live_runner_smoke_status(
    *,
    operator_summary: dict[str, object],
    runner_status: object,
    canonical_live_scope_ok: bool,
) -> str:
    runner_status = operator_summary.get("runner_status", runner_status)
    if runner_status in {None, "missing"}:
        return "missing"
    if not canonical_live_scope_ok:
        return "infra_blocked"
    risk_alert_summary = operator_summary.get("risk_alert_summary") or {}
    if bool(risk_alert_summary.get("has_critical")):
        return "infra_blocked"
    blocker = str(operator_summary.get("primary_blocker") or "")
    reject_category = str(operator_summary.get("decision_reject_category") or "")
    execution_reason = str(operator_summary.get("execution_reason") or "")
    if not blocker:
        return "ok"
    if blocker in {
        "latest_runner_missing",
        "foundation_not_ok",
        "liquidity_state_error",
        "liquidity_blocked",
        "order_action_error",
        "account_state_sync_error",
        "cancel_action_error",
        "redeem_action_error",
    }:
        return "infra_blocked"
    if blocker == "foundation_ok_with_errors":
        return "foundation_warning_only"
    if blocker == "decision_not_accept":
        if reject_category == "quote_inputs_missing":
            return "data_blocked"
        return "strategy_only_blocked"
    if blocker == "execution_not_plan":
        if execution_reason == "decision_reject":
            if reject_category == "quote_inputs_missing":
                return "data_blocked"
            return "strategy_only_blocked"
        return "infra_blocked"
    return "infra_blocked"
