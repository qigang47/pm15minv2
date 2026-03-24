from __future__ import annotations

from typing import Any

from .order_policy import build_cancel_policy, build_redeem_policy
from .policy_helpers import (
    build_policy_context,
    load_policy_state,
    repriced_order_guard,
    resolve_dynamic_stake_base,
    resolve_execution_account_summary,
    resolve_regime_stake_multiplier,
)
from .retry_policy import build_retry_policy


def build_execution_record(
    *,
    spec,
    execution_status: str,
    execution_reason: str | None,
    execution_reasons: list[str] | None = None,
    order_type: str | None = None,
    policy_context: dict[str, Any] | None = None,
    policy_state: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reasons = list(execution_reasons or ([] if execution_reason is None else [execution_reason]))
    order_type_norm = str(order_type or spec.default_order_type).upper()
    payload = {
        "status": execution_status,
        "reason": execution_reason if execution_reason is not None else (reasons[0] if reasons else None),
        "execution_reasons": reasons,
    }
    if extra:
        payload.update(extra)
    payload["retry_policy"] = build_retry_policy(
        spec=spec,
        execution_status=execution_status,
        execution_reasons=reasons,
        order_type=order_type_norm,
    )
    payload["cancel_policy"] = build_cancel_policy(
        spec=spec,
        order_type=order_type_norm,
        policy_context=policy_context,
        policy_state=policy_state,
    )
    payload["redeem_policy"] = build_redeem_policy(
        policy_context=policy_context,
        policy_state=policy_state,
    )
    return payload
