from __future__ import annotations

from ..operator.utils import float_or_none, int_or_none


def resolve_last_iteration_execution(*, last_iteration: dict[str, object]) -> dict[str, object]:
    execution = last_iteration.get("execution")
    if isinstance(execution, dict):
        return execution
    execution_payload = last_iteration.get("execution_payload") or {}
    if isinstance(execution_payload, dict):
        execution_block = execution_payload.get("execution")
        if isinstance(execution_block, dict):
            return execution_block
    return {}


def resolve_last_iteration_regime_state(
    *,
    last_iteration: dict[str, object],
    latest_state_summary: dict[str, object],
) -> dict[str, object]:
    regime_state = last_iteration.get("regime_state")
    if isinstance(regime_state, dict):
        return regime_state
    return ((latest_state_summary.get("regime") or {}).get("summary") or {})


def resolve_focus_market_context(
    *,
    last_iteration: dict[str, object],
    decision_reject_diagnostics: dict[str, object] | None,
) -> dict[str, object]:
    decision_payload = last_iteration.get("decision_payload") or {}
    if isinstance(decision_payload, dict):
        decision = decision_payload.get("decision") or {}
        selected_offset = int_or_none(decision.get("selected_offset"))
        if selected_offset is not None:
            for row in (decision_payload.get("accepted_offsets") or []):
                if not isinstance(row, dict):
                    continue
                if int_or_none(row.get("offset")) != selected_offset:
                    continue
                quote_row = row.get("quote_row") or {}
                return {
                    "source": "selected_offset",
                    "offset": selected_offset,
                    "market_id": quote_row.get("market_id"),
                    "condition_id": quote_row.get("condition_id"),
                }
    best_rejected = None if decision_reject_diagnostics is None else decision_reject_diagnostics.get("best_rejected_offset")
    if isinstance(best_rejected, dict):
        return {
            "source": "best_rejected_offset",
            "offset": int_or_none(best_rejected.get("offset")),
            "market_id": best_rejected.get("market_id"),
            "condition_id": best_rejected.get("condition_id"),
        }
    return {
        "source": None,
        "offset": None,
        "market_id": None,
        "condition_id": None,
    }


def build_focus_market_usage(
    *,
    account_state_payload: dict[str, object] | None,
    focus_context: dict[str, object],
) -> dict[str, object]:
    market_id = str(focus_context.get("market_id") or "").strip() or None
    condition_id = str(focus_context.get("condition_id") or "").strip() or None
    if market_id is None and condition_id is None:
        return {
            "source": focus_context.get("source"),
            "offset": focus_context.get("offset"),
            "market_id": None,
            "condition_id": None,
            "account_state_available": bool(account_state_payload),
            "position_match_basis": None,
            "open_orders_count": 0,
            "open_orders_notional_usd": 0.0,
            "positions_count": 0,
            "positions_size_sum": 0.0,
            "positions_current_value_usd": 0.0,
            "positions_cash_pnl_usd": 0.0,
            "redeemable_positions": 0,
            "active_trade_count": 0,
        }
    open_orders_snapshot = {} if not isinstance(account_state_payload, dict) else (account_state_payload.get("open_orders") or {})
    positions_snapshot = {} if not isinstance(account_state_payload, dict) else (account_state_payload.get("positions") or {})
    open_orders = open_orders_snapshot.get("orders") if isinstance(open_orders_snapshot, dict) else []
    positions = positions_snapshot.get("positions") if isinstance(positions_snapshot, dict) else []
    open_orders = open_orders if isinstance(open_orders, list) else []
    positions = positions if isinstance(positions, list) else []

    open_orders_count = 0
    open_orders_notional = 0.0
    for row in open_orders:
        if not isinstance(row, dict):
            continue
        if market_id is not None and str(row.get("market_id") or "").strip() != market_id:
            continue
        price = float_or_none(row.get("price")) or 0.0
        size = float_or_none(row.get("size")) or 0.0
        open_orders_count += 1
        open_orders_notional += max(0.0, float(price) * float(size))

    positions_count = 0
    positions_size_sum = 0.0
    positions_current_value_sum = 0.0
    positions_cash_pnl_sum = 0.0
    redeemable_positions = 0
    for row in positions:
        if not isinstance(row, dict):
            continue
        row_market_id = str(row.get("market_id") or "").strip() or None
        row_condition_id = str(row.get("condition_id") or "").strip() or None
        match = False
        if condition_id is not None and row_condition_id == condition_id:
            match = True
        elif market_id is not None and row_market_id == market_id:
            match = True
        if not match:
            continue
        size = float_or_none(row.get("size")) or 0.0
        current_value = float_or_none(row.get("current_value")) or 0.0
        cash_pnl = float_or_none(row.get("cash_pnl")) or 0.0
        positions_count += 1
        positions_size_sum += float(size)
        positions_current_value_sum += float(current_value)
        positions_cash_pnl_sum += float(cash_pnl)
        if bool(row.get("redeemable")):
            redeemable_positions += 1

    return {
        "source": focus_context.get("source"),
        "offset": focus_context.get("offset"),
        "market_id": market_id,
        "condition_id": condition_id,
        "account_state_available": bool(account_state_payload),
        "position_match_basis": "condition_id" if condition_id is not None else ("market_id" if market_id is not None else None),
        "open_orders_count": int(open_orders_count),
        "open_orders_notional_usd": open_orders_notional,
        "positions_count": int(positions_count),
        "positions_size_sum": positions_size_sum,
        "positions_current_value_usd": positions_current_value_sum,
        "positions_cash_pnl_usd": positions_cash_pnl_sum,
        "redeemable_positions": int(redeemable_positions),
        "active_trade_count": int(open_orders_count + positions_count),
    }
