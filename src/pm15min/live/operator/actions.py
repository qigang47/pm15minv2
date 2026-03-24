from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from .followups_blockers import (
    append_decision_not_accept_followups as _append_decision_not_accept_followups,
    append_execution_not_plan_followups as _append_execution_not_plan_followups,
    append_foundation_warning_followups as _append_foundation_warning_followups,
)
from .followups_side_effects import (
    append_account_state_sync_actions as _append_account_state_sync_actions,
    append_cancel_action_followups as _append_cancel_action_followups,
    append_redeem_action_followups as _append_redeem_action_followups,
)

_SIDE_EFFECT_WARNING_STATUSES = {"error", "ok_with_errors"}
_DEFAULT_BOOTSTRAP_ACTION = (
    "run live check-trading-gateway and then runner-once --dry-run-side-effects before enabling side effects"
)
_SIMPLE_PRIMARY_BLOCKER_ACTIONS = {
    "outside_canonical_live_scope": "switch to canonical live scope: profile=deep_otm target=direction markets=sol|xrp",
    "latest_runner_missing": _DEFAULT_BOOTSTRAP_ACTION,
    "critical_risk_alert_present": "inspect latest runner risk_alerts and resolve critical alert before enabling side effects",
    "liquidity_blocked": "run live sync-liquidity-state and inspect latest liquidity snapshot",
    "liquidity_state_error": "inspect latest liquidity state payload and rerun live sync-liquidity-state",
}


@dataclass(frozen=True)
class _OperatorActionContext:
    blocker: str
    secondary_blockers: list[str]
    foundation_status: str
    foundation_reason: str
    foundation_issue_codes: set[str]
    decision_status: str
    reject_category: str
    reject_interpretation: str
    reject_reasons: list[object]
    execution_status: str
    execution_block_category: str
    execution_reason: str
    order_action_status: str
    order_action_reason: str
    account_state_status: str
    account_open_orders_status: str
    account_positions_status: str
    cancel_action_status: str
    cancel_action_reason: str
    redeem_action_status: str
    redeem_action_reason: str


def _as_text(value: object, *, normalize: bool = False) -> str:
    text = str(value or "")
    return text.strip().lower() if normalize else text


def _build_action_context(operator_summary: Mapping[str, object]) -> _OperatorActionContext:
    return _OperatorActionContext(
        blocker=_as_text(operator_summary.get("primary_blocker")),
        secondary_blockers=[
            str(code) for code in (operator_summary.get("secondary_blockers") or []) if str(code)
        ],
        foundation_status=_as_text(operator_summary.get("foundation_status")),
        foundation_reason=_as_text(operator_summary.get("foundation_reason")),
        foundation_issue_codes={
            str(code) for code in (operator_summary.get("foundation_issue_codes") or []) if str(code)
        },
        decision_status=_as_text(operator_summary.get("decision_status"), normalize=True),
        reject_category=_as_text(operator_summary.get("decision_reject_category")),
        reject_interpretation=_as_text(operator_summary.get("decision_reject_interpretation")),
        reject_reasons=list(operator_summary.get("decision_top_reject_reasons") or []),
        execution_status=_as_text(operator_summary.get("execution_status"), normalize=True),
        execution_block_category=_as_text(operator_summary.get("execution_block_category")),
        execution_reason=_as_text(operator_summary.get("execution_reason")),
        order_action_status=_as_text(operator_summary.get("order_action_status"), normalize=True),
        order_action_reason=_as_text(operator_summary.get("order_action_reason")),
        account_state_status=_as_text(operator_summary.get("account_state_status"), normalize=True),
        account_open_orders_status=_as_text(operator_summary.get("account_open_orders_status"), normalize=True),
        account_positions_status=_as_text(operator_summary.get("account_positions_status"), normalize=True),
        cancel_action_status=_as_text(operator_summary.get("cancel_action_status"), normalize=True),
        cancel_action_reason=_as_text(operator_summary.get("cancel_action_reason"), normalize=False).strip(),
        redeem_action_status=_as_text(operator_summary.get("redeem_action_status"), normalize=True),
        redeem_action_reason=_as_text(operator_summary.get("redeem_action_reason"), normalize=False).strip(),
    )


def _append_order_action_error_followups(*, actions: list[str], context: _OperatorActionContext) -> None:
    actions.append("inspect latest runner order_action payload and trading gateway state before retrying side effects")
    if context.order_action_reason:
        actions.append(
            "use operator_summary.order_action_reason together with the latest execution snapshot to narrow whether submit failed in request construction, gateway auth, or order placement"
        )
    if context.account_state_status == "error":
        actions.append(
            "latest account refresh also failed after the submit path; rerun live sync-account-state after stabilizing order submit"
        )
        if context.account_open_orders_status == "error":
            actions.append(
                "rerun live check-trading-gateway --probe-open-orders to isolate whether open-orders refresh failed independently of order submit"
            )
        if context.account_positions_status == "error":
            actions.append(
                "rerun live check-trading-gateway --probe-positions to isolate whether positions refresh failed independently of order submit"
            )
    if context.cancel_action_status in _SIDE_EFFECT_WARNING_STATUSES:
        actions.append(
            "after stabilizing order submit, inspect operator_summary.cancel_action_reason and reconcile latest open_orders before retrying cancel side effects"
        )
    if context.redeem_action_status in _SIDE_EFFECT_WARNING_STATUSES:
        actions.append(
            "after stabilizing order submit, inspect operator_summary.redeem_action_reason and reconcile latest redeemable positions before retrying redeem side effects"
        )


def _append_cancel_redeem_followups_if_needed(*, actions: list[str], context: _OperatorActionContext) -> None:
    if context.cancel_action_status in _SIDE_EFFECT_WARNING_STATUSES:
        append_cancel_action_followups(
            actions=actions,
            cancel_action_status=context.cancel_action_status,
            cancel_action_reason=context.cancel_action_reason,
            account_open_orders_status=context.account_open_orders_status,
        )
    if context.redeem_action_status in _SIDE_EFFECT_WARNING_STATUSES:
        append_redeem_action_followups(
            actions=actions,
            redeem_action_status=context.redeem_action_status,
            redeem_action_reason=context.redeem_action_reason,
            account_positions_status=context.account_positions_status,
        )


def _append_primary_blocker_followups(*, actions: list[str], context: _OperatorActionContext) -> None:
    simple_action = _SIMPLE_PRIMARY_BLOCKER_ACTIONS.get(context.blocker)
    if simple_action:
        actions.append(simple_action)
        return

    if context.blocker == "foundation_ok_with_errors":
        append_foundation_warning_followups(
            actions=actions,
            foundation_reason=context.foundation_reason,
            foundation_issue_codes=context.foundation_issue_codes,
        )
        return

    if context.blocker == "foundation_not_ok":
        actions.append("inspect latest foundation summary and rerun data run live-foundation before retrying runner")
        if context.foundation_status:
            actions.append("inspect data show-summary completeness/issues for the live surface before retrying foundation")
        if context.foundation_reason:
            actions.append(
                "use runner risk_summary.foundation.reason to narrow which source/table failed inside live-foundation"
            )
        return

    if context.blocker == "decision_not_accept":
        append_decision_not_accept_followups(
            actions=actions,
            reject_category=context.reject_category,
            reject_interpretation=context.reject_interpretation,
            reject_reasons=context.reject_reasons,
        )
        return

    if context.blocker == "execution_not_plan":
        append_execution_not_plan_followups(
            actions=actions,
            execution_block_category=context.execution_block_category,
            execution_reason=context.execution_reason,
        )
        return

    if context.blocker == "order_action_error":
        _append_order_action_error_followups(actions=actions, context=context)
        return

    if context.blocker == "account_state_sync_error":
        append_account_state_sync_actions(
            actions=actions,
            order_action_status=context.order_action_status,
            account_state_status=context.account_state_status,
            account_open_orders_status=context.account_open_orders_status,
            account_positions_status=context.account_positions_status,
        )
        return

    if context.blocker == "cancel_action_error":
        append_cancel_action_followups(
            actions=actions,
            cancel_action_status=context.cancel_action_status,
            cancel_action_reason=context.cancel_action_reason,
            account_open_orders_status=context.account_open_orders_status,
        )
        return

    if context.blocker == "redeem_action_error":
        append_redeem_action_followups(
            actions=actions,
            redeem_action_status=context.redeem_action_status,
            redeem_action_reason=context.redeem_action_reason,
            account_positions_status=context.account_positions_status,
        )
        return

    _append_cancel_redeem_followups_if_needed(actions=actions, context=context)
    if not actions:
        actions.append(_DEFAULT_BOOTSTRAP_ACTION)


def _append_secondary_blocker_followups(*, actions: list[str], context: _OperatorActionContext) -> None:
    if context.blocker == "foundation_ok_with_errors":
        if "decision_not_accept" in context.secondary_blockers and context.decision_status != "accept":
            append_decision_not_accept_followups(
                actions=actions,
                reject_category=context.reject_category,
                reject_interpretation=context.reject_interpretation,
                reject_reasons=context.reject_reasons,
            )
        elif "execution_not_plan" in context.secondary_blockers and context.execution_status != "plan":
            append_execution_not_plan_followups(
                actions=actions,
                execution_block_category=context.execution_block_category,
                execution_reason=context.execution_reason,
            )
        return

    if (
        context.blocker in {"decision_not_accept", "execution_not_plan"}
        and "foundation_ok_with_errors" in context.secondary_blockers
    ):
        append_foundation_warning_followups(
            actions=actions,
            foundation_reason=context.foundation_reason,
            foundation_issue_codes=context.foundation_issue_codes,
        )


def append_foundation_warning_followups(
    *,
    actions: list[str],
    foundation_reason: str,
    foundation_issue_codes: set[str],
) -> None:
    _append_foundation_warning_followups(
        actions=actions,
        foundation_reason=foundation_reason,
        foundation_issue_codes=foundation_issue_codes,
    )


def append_decision_not_accept_followups(
    *,
    actions: list[str],
    reject_category: str,
    reject_interpretation: str,
    reject_reasons: list[object],
) -> None:
    _append_decision_not_accept_followups(
        actions=actions,
        reject_category=reject_category,
        reject_interpretation=reject_interpretation,
        reject_reasons=reject_reasons,
    )


def append_execution_not_plan_followups(
    *,
    actions: list[str],
    execution_block_category: str,
    execution_reason: str,
) -> None:
    _append_execution_not_plan_followups(
        actions=actions,
        execution_block_category=execution_block_category,
        execution_reason=execution_reason,
    )


def append_account_state_sync_actions(
    *,
    actions: list[str],
    order_action_status: str,
    account_state_status: str,
    account_open_orders_status: str,
    account_positions_status: str,
) -> None:
    _append_account_state_sync_actions(
        actions=actions,
        order_action_status=order_action_status,
        account_state_status=account_state_status,
        account_open_orders_status=account_open_orders_status,
        account_positions_status=account_positions_status,
    )


def append_cancel_action_followups(
    *,
    actions: list[str],
    cancel_action_status: str,
    cancel_action_reason: str,
    account_open_orders_status: str,
) -> None:
    _append_cancel_action_followups(
        actions=actions,
        cancel_action_status=cancel_action_status,
        cancel_action_reason=cancel_action_reason,
        account_open_orders_status=account_open_orders_status,
    )


def append_redeem_action_followups(
    *,
    actions: list[str],
    redeem_action_status: str,
    redeem_action_reason: str,
    account_positions_status: str,
) -> None:
    _append_redeem_action_followups(
        actions=actions,
        redeem_action_status=redeem_action_status,
        redeem_action_reason=redeem_action_reason,
        account_positions_status=account_positions_status,
    )


def recommend_live_operator_actions(*, operator_summary: dict[str, object]) -> list[str]:
    context = _build_action_context(operator_summary)
    actions: list[str] = []
    _append_primary_blocker_followups(actions=actions, context=context)
    _append_secondary_blocker_followups(actions=actions, context=context)
    return list(dict.fromkeys(actions))
