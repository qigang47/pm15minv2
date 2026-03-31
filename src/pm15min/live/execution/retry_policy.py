from __future__ import annotations

from pm15min.core.retry_contracts import (
    FAK_IMMEDIATE_RETRY_ERROR_HINTS,
    FAST_RETRY_ERROR_HINTS,
    ORDERBOOK_RETRYABLE_REASONS,
)

NON_RESTING_ORDER_TYPES = {"FAK", "FOK"}


def build_retry_policy(
    *,
    spec,
    execution_status: str,
    execution_reasons: list[str],
    order_type: str,
) -> dict[str, object]:
    order_type_norm = str(order_type or spec.default_order_type).upper()
    post_submit_retry_enabled = bool(execution_status == "plan")
    same_decision_enabled = bool(post_submit_retry_enabled and spec.repeat_same_decision_enabled)
    order_retry_interval = float(spec.order_retry_interval_seconds)
    fast_retry_interval = float(spec.fast_retry_interval_seconds)
    order_retry_max = int(spec.max_order_retries)
    fak_retry_max = int(spec.fak_immediate_retry_max)
    if post_submit_retry_enabled:
        status = "armed"
        reason = "post_submit_retry_ready"
    else:
        status = "inactive"
        reason = execution_reasons[0] if execution_reasons else execution_status

    return {
        "status": status,
        "reason": reason,
        "pre_submit_depth_retry": {
            "enabled": False,
            "retry_interval_sec": 0.0,
            "max_retries": 0,
            "retry_state_key": "",
            "trigger_statuses": [],
            "retryable_reasons": sorted(ORDERBOOK_RETRYABLE_REASONS),
            "mode": "runner_loop_window_rechecks",
        },
        "post_submit_order_retry": {
            "enabled": post_submit_retry_enabled,
            "retry_interval_sec": order_retry_interval,
            "fast_retry_interval_sec": fast_retry_interval,
            "max_retries": order_retry_max,
            "retry_state_keys": [
                "attempts",
                "last_attempt",
                "last_error",
                "fast_retry",
                "retry_interval_seconds",
            ],
            "retryable_on_non_success_response": True,
            "fast_retry_error_hints": list(FAST_RETRY_ERROR_HINTS),
        },
        "post_submit_fak_retry": {
            "enabled": bool(post_submit_retry_enabled and order_type_norm == "FAK"),
            "order_type": order_type_norm,
            "immediate_retry_max": fak_retry_max,
            "requires_orderbook_reprice": True,
            "response_driven": True,
            "retryable_message_hints": list(FAK_IMMEDIATE_RETRY_ERROR_HINTS),
        },
        "same_decision_repeat": {
            "enabled": same_decision_enabled,
            "lock_side": bool(spec.repeat_same_decision_lock_side),
            "max_trades": int(spec.repeat_same_decision_max_trades),
            "max_stake_usd": spec.repeat_same_decision_max_stake_usd,
            "max_total_stake_usd": spec.repeat_same_decision_max_total_stake_usd,
            "stake_multiple": float(spec.repeat_same_decision_stake_multiple),
            "success_state_last_error": "matched_repeat_window",
        },
    }
