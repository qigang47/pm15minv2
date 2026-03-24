from __future__ import annotations

from pm15min.research.labels.runtime_visibility import summarize_truth_runtime_visibility

from ..capital_usage import build_live_capital_usage_summary
from .followups import (
    build_decision_reject_diagnostics,
    categorize_decision_reject_reasons,
    categorize_execution_block_reasons,
)
from .utils import summarize_live_risk_alerts


def ready_context_actions(*, operator_summary: dict[str, object]) -> list[str]:
    foundation_status = str(operator_summary.get("foundation_status") or "").strip().lower()
    foundation_issue_codes = {
        str(code)
        for code in (operator_summary.get("foundation_issue_codes") or [])
        if str(code)
    }
    truth_runtime_status = str(operator_summary.get("truth_runtime_status") or "").strip().lower()
    actions: list[str] = []
    if foundation_status == "ok_with_errors" and "oracle_direct_rate_limited" in foundation_issue_codes:
        actions.append("wait for the direct oracle rate-limit window to clear, then rerun data run live-foundation or runner-once --dry-run-side-effects")
        actions.append("treat oracle_prices_table as temporary fail-open fallback until direct oracle recovers")
    if truth_runtime_status == "stale":
        actions.append("truth/oracle window is stale; inspect operator_summary.truth_runtime_reason and rerun data run live-foundation before enabling side effects")
    elif truth_runtime_status == "missing":
        actions.append("truth/oracle window is missing; inspect operator_summary.truth_runtime_reason and rerun data run live-foundation before enabling side effects")
    return actions


def build_live_operator_summary(
    *,
    canonical_scope: dict[str, object],
    latest_state_summary: dict[str, object],
    runner_payload: dict[str, object] | None,
    truth_runtime_summary: dict[str, object] | None = None,
) -> dict[str, object]:
    runner_status = None if runner_payload is None else runner_payload.get("status")
    last_iteration = {} if runner_payload is None else (runner_payload.get("last_iteration") or {})
    risk_alerts = list(last_iteration.get("risk_alerts") or [])
    risk_alert_summary = summarize_live_risk_alerts(alerts=risk_alerts)
    runner_health = last_iteration.get("runner_health") or {}
    liquidity_summary = ((latest_state_summary.get("liquidity") or {}).get("summary") or {})
    regime_summary = ((latest_state_summary.get("regime") or {}).get("summary") or {})
    decision_summary = latest_state_summary.get("decision") or {}
    execution_summary = latest_state_summary.get("execution") or {}
    runner_decision = last_iteration.get("decision") or {}
    runner_execution = last_iteration.get("execution") or {}
    runner_risk_summary = last_iteration.get("risk_summary") or {}
    foundation_summary = (
        last_iteration.get("foundation_summary")
        or (runner_risk_summary.get("foundation") or {})
        or (((latest_state_summary.get("foundation") or {}).get("summary")) or {})
    )
    decision_status = decision_summary.get("status") or runner_decision.get("status")
    execution_status = execution_summary.get("status") or runner_execution.get("status")
    execution_reason = execution_summary.get("reason") or runner_execution.get("reason")
    execution_reasons = list(runner_execution.get("execution_reasons") or ([] if execution_reason is None else [execution_reason]))
    execution_block_category = categorize_execution_block_reasons(
        execution_reason=execution_reason,
        execution_reasons=execution_reasons,
    )
    foundation_issue_codes = [str(code) for code in (foundation_summary.get("issue_codes") or []) if str(code)]
    foundation_degraded_tasks = [
        row for row in (foundation_summary.get("degraded_tasks") or [])
        if isinstance(row, dict)
    ]
    side_effects_summary = runner_risk_summary.get("side_effects") or {}
    order_action_summary = last_iteration.get("order_action") or {}
    account_state_iteration = last_iteration.get("account_state") or {}
    cancel_action_summary = last_iteration.get("cancel_action") or {}
    redeem_action_summary = last_iteration.get("redeem_action") or {}
    decision_top_reject_reasons = list(((runner_risk_summary.get("decision") or {}).get("top_reject_reasons") or []))
    decision_reject_category = categorize_decision_reject_reasons(decision_top_reject_reasons)
    decision_reject_diagnostics = build_decision_reject_diagnostics(last_iteration=last_iteration)
    decision_reject_interpretation = None if decision_reject_diagnostics is None else decision_reject_diagnostics.get("interpretation")
    capital_usage_summary = build_live_capital_usage_summary(
        canonical_scope=canonical_scope,
        latest_state_summary=latest_state_summary,
        last_iteration=last_iteration,
        decision_reject_diagnostics=decision_reject_diagnostics,
    )
    open_orders_summary = latest_state_summary.get("open_orders") or {}
    positions_summary = latest_state_summary.get("positions") or {}
    orderbook_hot_cache_summary = ((latest_state_summary.get("orderbook_hot_cache") or {}).get("summary") or {})
    runner_health_overall_status = runner_health.get("overall_status")
    pre_side_effect_status = runner_health.get("pre_side_effect_status")
    post_side_effect_status = runner_health.get("post_side_effect_status")
    runner_health_blocker = str(runner_health.get("primary_blocker") or "")
    blocker_stage = runner_health.get("blocker_stage")
    blocking_issue_count = int(runner_health.get("blocking_issue_count") or 0)
    warning_issue_count = int(runner_health.get("warning_issue_count") or 0)
    merged_truth_runtime_summary = _merge_truth_runtime_summary(
        truth_runtime_summary=truth_runtime_summary,
        foundation_summary=foundation_summary,
    )
    truth_runtime_visibility = summarize_truth_runtime_visibility(merged_truth_runtime_summary)
    foundation_status = (
        foundation_summary.get("status")
        or merged_truth_runtime_summary.get("truth_runtime_foundation_status")
    )
    foundation_reason = (
        foundation_summary.get("reason")
        or merged_truth_runtime_summary.get("truth_runtime_foundation_reason")
    )
    if not foundation_issue_codes:
        foundation_issue_codes = [
            str(code)
            for code in (merged_truth_runtime_summary.get("truth_runtime_foundation_issue_codes") or [])
            if str(code)
        ]

    can_run_side_effects = bool(
        canonical_scope.get("ok")
        and (not risk_alert_summary.get("has_critical"))
        and (
            (str(pre_side_effect_status or "").lower() == "ok" and blocking_issue_count == 0)
            if runner_health
            else (
                str(decision_status or "").lower() == "accept"
                and str(execution_status or "").lower() == "plan"
            )
        )
    )

    primary_blocker = None
    if not bool(canonical_scope.get("ok")):
        primary_blocker = "outside_canonical_live_scope"
    elif runner_payload is None and not any(
        bool((latest_state_summary.get(name) or {}).get("exists"))
        for name in ("decision", "execution", "liquidity", "regime", "open_orders", "positions")
    ):
        primary_blocker = "latest_runner_missing"
    elif risk_alert_summary.get("has_critical"):
        primary_blocker = runner_health_blocker or "critical_risk_alert_present"
    elif runner_health_blocker:
        primary_blocker = runner_health_blocker
    elif str(foundation_status or "").lower() == "ok_with_errors" and not can_run_side_effects:
        primary_blocker = "foundation_ok_with_errors"
    elif str(liquidity_summary.get("blocked") or "").lower() == "true":
        primary_blocker = "liquidity_blocked"
    elif str(decision_status or "").lower() != "accept":
        primary_blocker = "decision_not_accept"
    elif str(execution_status or "").lower() != "plan":
        primary_blocker = "execution_not_plan"

    secondary_blockers: list[str] = []
    if (
        str(foundation_status or "").lower() == "ok_with_errors"
        and not can_run_side_effects
        and primary_blocker != "foundation_ok_with_errors"
    ):
        secondary_blockers.append("foundation_ok_with_errors")
    if str(liquidity_summary.get("blocked") or "").lower() == "true" and primary_blocker != "liquidity_blocked":
        secondary_blockers.append("liquidity_blocked")
    if str(decision_status or "").lower() != "accept" and primary_blocker != "decision_not_accept":
        secondary_blockers.append("decision_not_accept")
    if str(execution_status or "").lower() != "plan" and primary_blocker != "execution_not_plan":
        secondary_blockers.append("execution_not_plan")

    return {
        "canonical_live_scope_ok": bool(canonical_scope.get("ok")),
        "runner_status": runner_status,
        "can_run_side_effects": can_run_side_effects,
        "primary_blocker": primary_blocker,
        "secondary_blockers": secondary_blockers,
        "blocker_stage": blocker_stage,
        "decision_status": decision_status,
        "decision_top_reject_reasons": decision_top_reject_reasons,
        "decision_reject_category": decision_reject_category,
        "decision_reject_interpretation": decision_reject_interpretation,
        "decision_reject_diagnostics": decision_reject_diagnostics,
        "capital_usage_summary": capital_usage_summary,
        "foundation_status": foundation_status,
        "foundation_reason": foundation_reason,
        "foundation_issue_codes": foundation_issue_codes,
        "foundation_degraded_tasks": foundation_degraded_tasks,
        "foundation_run_started_at": truth_runtime_visibility.get("truth_runtime_run_started_at"),
        "foundation_last_completed_at": truth_runtime_visibility.get("truth_runtime_last_completed_at"),
        "foundation_finished_at": truth_runtime_visibility.get("truth_runtime_finished_at"),
        "foundation_completed_iterations": truth_runtime_visibility.get("truth_runtime_completed_iterations"),
        "foundation_recent_refresh_status": truth_runtime_visibility.get("truth_runtime_window_refresh_status"),
        "foundation_recent_refresh_reason": truth_runtime_visibility.get("truth_runtime_window_refresh_reason"),
        **truth_runtime_visibility,
        "execution_status": execution_status,
        "execution_reason": execution_reason,
        "execution_reasons": execution_reasons,
        "execution_block_category": execution_block_category,
        "liquidity_blocked": liquidity_summary.get("blocked"),
        "liquidity_reason": liquidity_summary.get("reason"),
        "regime_state": regime_summary.get("state"),
        "regime_pressure": regime_summary.get("pressure"),
        "order_action_status": order_action_summary.get("status") or side_effects_summary.get("order_status"),
        "order_action_reason": order_action_summary.get("reason") or side_effects_summary.get("order_reason"),
        "account_state_status": side_effects_summary.get("account_state_status"),
        "account_state_snapshot_ts": account_state_iteration.get("snapshot_ts"),
        "account_open_orders_status": (
            side_effects_summary.get("account_open_orders_status")
            or account_state_iteration.get("open_orders_status")
            or open_orders_summary.get("status")
        ),
        "account_positions_status": (
            side_effects_summary.get("account_positions_status")
            or account_state_iteration.get("positions_status")
            or positions_summary.get("status")
        ),
        "cancel_action_status": cancel_action_summary.get("status") or side_effects_summary.get("cancel_status"),
        "cancel_action_reason": cancel_action_summary.get("reason") or side_effects_summary.get("cancel_reason"),
        "redeem_action_status": redeem_action_summary.get("status") or side_effects_summary.get("redeem_status"),
        "redeem_action_reason": redeem_action_summary.get("reason") or side_effects_summary.get("redeem_reason"),
        "orderbook_hot_cache_status": orderbook_hot_cache_summary.get("status"),
        "orderbook_hot_cache_reason": orderbook_hot_cache_summary.get("reason"),
        "orderbook_hot_cache_summary": orderbook_hot_cache_summary,
        "runner_health_status": runner_health_overall_status,
        "pre_side_effect_status": pre_side_effect_status,
        "post_side_effect_status": post_side_effect_status,
        "blocking_issue_count": blocking_issue_count,
        "warning_issue_count": warning_issue_count,
        "runner_health": runner_health,
        "risk_alert_summary": risk_alert_summary,
    }


def _merge_truth_runtime_summary(
    *,
    truth_runtime_summary: dict[str, object] | None,
    foundation_summary: dict[str, object],
) -> dict[str, object]:
    summary = dict(truth_runtime_summary or {})
    if not summary.get("truth_runtime_foundation_status") and foundation_summary.get("status") is not None:
        summary["truth_runtime_foundation_status"] = foundation_summary.get("status")
    if not summary.get("truth_runtime_foundation_reason") and foundation_summary.get("reason") is not None:
        summary["truth_runtime_foundation_reason"] = foundation_summary.get("reason")
    if not summary.get("truth_runtime_foundation_issue_codes"):
        summary["truth_runtime_foundation_issue_codes"] = list(foundation_summary.get("issue_codes") or [])
    if not summary.get("truth_runtime_foundation_run_started_at") and foundation_summary.get("run_started_at") is not None:
        summary["truth_runtime_foundation_run_started_at"] = foundation_summary.get("run_started_at")
    if not summary.get("truth_runtime_foundation_last_completed_at") and foundation_summary.get("last_completed_at") is not None:
        summary["truth_runtime_foundation_last_completed_at"] = foundation_summary.get("last_completed_at")
    if not summary.get("truth_runtime_foundation_finished_at") and foundation_summary.get("finished_at") is not None:
        summary["truth_runtime_foundation_finished_at"] = foundation_summary.get("finished_at")
    if "truth_runtime_foundation_completed_iterations" not in summary and foundation_summary.get("completed_iterations") is not None:
        summary["truth_runtime_foundation_completed_iterations"] = foundation_summary.get("completed_iterations")
    if "truth_runtime_direct_oracle_fail_open" not in summary:
        issue_codes = {
            str(code)
            for code in (summary.get("truth_runtime_foundation_issue_codes") or [])
            if str(code)
        }
        summary["truth_runtime_direct_oracle_fail_open"] = bool(
            "oracle_direct_rate_limited" in issue_codes
            and str(summary.get("truth_runtime_oracle_prices_table_status") or "").strip().lower() == "ok"
        )
    return summary
