from __future__ import annotations

from typing import Any

from .policy_helpers import float_or_none, match_open_orders
from .retry_policy import NON_RESTING_ORDER_TYPES


def build_cancel_policy(
    *,
    spec,
    order_type: str,
    policy_context: dict[str, Any] | None,
    policy_state: dict[str, Any] | None,
) -> dict[str, Any]:
    ctx = policy_context or {}
    state = policy_state or {}
    order_type_norm = str(order_type or spec.default_order_type).upper()
    market_id = ctx.get("market_id")
    token_id = ctx.get("token_id")
    cancel_window = getattr(spec, "cancel_markets_when_minutes_left", None)
    minutes_left = float_or_none(ctx.get("minutes_left_to_market_end"))
    expects_resting_orders = order_type_norm not in NON_RESTING_ORDER_TYPES
    open_orders_snapshot = state.get("open_orders_snapshot")
    snapshot_status = None if open_orders_snapshot is None else str(open_orders_snapshot.get("status") or "")
    snapshot_reason = None if open_orders_snapshot is None else open_orders_snapshot.get("reason")
    matching_orders = match_open_orders(
        open_orders_snapshot=open_orders_snapshot,
        market_id=market_id,
        token_id=token_id,
    )

    if not market_id:
        status = "inactive"
        reason = "market_context_missing"
    elif not expects_resting_orders:
        status = "inactive"
        reason = "order_type_has_no_resting_order"
    elif cancel_window is None:
        status = "inactive"
        reason = "cancel_window_not_configured"
    elif ctx.get("decision_ts") is None:
        status = "unavailable"
        reason = "decision_ts_missing"
    elif ctx.get("cycle_end_ts") is None:
        status = "unavailable"
        reason = "market_cycle_end_missing"
    elif open_orders_snapshot is None:
        status = "unavailable"
        reason = "open_orders_snapshot_missing"
    elif snapshot_status != "ok":
        status = "unavailable"
        reason = "open_orders_snapshot_unavailable"
    elif minutes_left is not None and minutes_left <= float(cancel_window):
        if matching_orders:
            status = "ready"
            reason = "open_orders_present_in_cancel_window"
        else:
            status = "inactive"
            reason = "no_open_orders_for_market"
    else:
        status = "watch"
        reason = "market_not_in_cancel_window"

    return {
        "status": status,
        "reason": reason,
        "market_id": market_id,
        "decision_ts": ctx.get("decision_ts"),
        "market_cycle_end_ts": ctx.get("cycle_end_ts"),
        "minutes_left_to_market_end": minutes_left,
        "order_type": order_type_norm,
        "cancel_window_minutes": int(cancel_window) if cancel_window is not None else None,
        "token_id": token_id,
        "open_orders_snapshot_status": snapshot_status,
        "open_orders_snapshot_reason": snapshot_reason,
        "matching_open_orders_count": len(matching_orders),
        "matching_open_order_ids": [
            row.get("order_id")
            for row in matching_orders
            if row.get("order_id") not in (None, "")
        ],
        "scope": "selected_market",
        "trigger": "market_in_cancel_window_and_open_orders_present",
        "side_effect_enabled": False,
        "source_of_truth": {
            "market_cycle_end_ts_present": ctx.get("cycle_end_ts") is not None,
            "resting_open_orders_required": expects_resting_orders,
            "open_orders_state_available": bool(open_orders_snapshot is not None and snapshot_status == "ok"),
        },
    }


def build_redeem_policy(
    *,
    policy_context: dict[str, Any] | None,
    policy_state: dict[str, Any] | None,
) -> dict[str, Any]:
    ctx = policy_context or {}
    state = policy_state or {}
    market_id = ctx.get("market_id")
    condition_id = str(ctx.get("condition_id") or "").strip() or None
    positions_snapshot = state.get("positions_snapshot")
    snapshot_status = None if positions_snapshot is None else str(positions_snapshot.get("status") or "")
    snapshot_reason = None if positions_snapshot is None else positions_snapshot.get("reason")
    redeem_plan = positions_snapshot.get("redeem_plan") if isinstance(positions_snapshot, dict) else {}
    matched_plan = redeem_plan.get(condition_id) if isinstance(redeem_plan, dict) and condition_id else None

    if not market_id:
        status = "inactive"
        reason = "market_context_missing"
    elif not condition_id:
        status = "unavailable"
        reason = "condition_id_missing"
    elif positions_snapshot is None:
        status = "unavailable"
        reason = "positions_snapshot_missing"
    elif snapshot_status != "ok":
        status = "unavailable"
        reason = "positions_snapshot_unavailable"
    elif matched_plan:
        status = "ready"
        reason = "redeemable_positions_present"
    else:
        status = "inactive"
        reason = "condition_not_redeemable"

    return {
        "status": status,
        "reason": reason,
        "market_id": market_id,
        "condition_id": condition_id,
        "decision_ts": ctx.get("decision_ts"),
        "positions_source": "polymarket_data_api_positions",
        "positions_snapshot_status": snapshot_status,
        "positions_snapshot_reason": snapshot_reason,
        "matching_positions_count": None if matched_plan is None else matched_plan.get("positions_count"),
        "index_sets": None if matched_plan is None else matched_plan.get("index_sets"),
        "current_value_sum": None if matched_plan is None else matched_plan.get("current_value_sum"),
        "cash_pnl_sum": None if matched_plan is None else matched_plan.get("cash_pnl_sum"),
        "trigger": "condition_has_redeemable_positions",
        "side_effect_enabled": False,
        "source_of_truth": {
            "condition_id_present": condition_id is not None,
            "positions_state_available": bool(positions_snapshot is not None and snapshot_status == "ok"),
            "redeemable_flag_required": True,
            "current_value_or_cash_pnl_required": True,
            "index_set_builder": "1 << outcomeIndex",
        },
    }
