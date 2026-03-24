from __future__ import annotations

from ..operator.utils import float_or_none, ratio_or_none


def build_account_overview(
    *,
    account_summary: dict[str, object],
    account_state_payload: dict[str, object] | None,
    latest_state_summary: dict[str, object],
    focus_usage: dict[str, object],
) -> dict[str, object]:
    open_orders = account_summary.get("open_orders") or {}
    positions = account_summary.get("positions") or {}
    total_open_orders = int(open_orders.get("total_orders") or 0)
    total_positions = int(positions.get("total_positions") or 0)
    redeemable_positions = int(positions.get("redeemable_positions") or 0)
    redeemable_conditions = max(
        int(positions.get("redeemable_conditions") or 0),
        count_redeemable_conditions(account_state_payload=account_state_payload),
    )
    visible_open_order_notional = float_or_none(account_summary.get("visible_open_order_notional_usd")) or 0.0
    visible_position_mark = float_or_none(account_summary.get("visible_position_mark_usd")) or 0.0
    visible_position_cash_pnl = float_or_none(account_summary.get("visible_position_cash_pnl_usd")) or 0.0
    visible_capital_usage = float_or_none(account_summary.get("visible_capital_usage_usd")) or 0.0
    cash_balance_usd = float_or_none(account_summary.get("cash_balance_usd"))
    cash_balance_available = cash_balance_usd is not None
    focus_market_id = str(focus_usage.get("market_id") or "").strip() or None
    focus_condition_id = str(focus_usage.get("condition_id") or "").strip() or None
    focus_market = None
    if focus_market_id is not None or focus_condition_id is not None:
        focus_visible_usage = (float_or_none(focus_usage.get("open_orders_notional_usd")) or 0.0) + (
            float_or_none(focus_usage.get("positions_current_value_usd")) or 0.0
        )
        focus_market = {
            "source": focus_usage.get("source"),
            "offset": focus_usage.get("offset"),
            "market_id": focus_market_id,
            "condition_id": focus_condition_id,
            "active_trade_count": int(focus_usage.get("active_trade_count") or 0),
            "visible_usage_usd": focus_visible_usage,
            "share_of_visible_capital_usage": ratio_or_none(focus_visible_usage, visible_capital_usage),
        }
    payload = {
        "snapshot_ts": account_summary.get("snapshot_ts"),
        "total_open_orders": total_open_orders,
        "open_orders_market_count": int(open_orders.get("market_count") or 0),
        "open_orders_token_count": int(open_orders.get("token_count") or 0),
        "total_positions": total_positions,
        "positions_market_count": int(positions.get("market_count") or 0),
        "positions_condition_count": int(positions.get("condition_count") or 0),
        "redeemable_positions": redeemable_positions,
        "redeemable_conditions": redeemable_conditions,
        "positions_without_market_id": int(positions.get("positions_without_market_id") or 0),
        "visible_open_order_notional_usd": visible_open_order_notional,
        "visible_position_mark_usd": visible_position_mark,
        "visible_position_cash_pnl_usd": visible_position_cash_pnl,
        "visible_capital_usage_usd": visible_capital_usage,
        "has_open_orders": total_open_orders > 0,
        "has_positions": total_positions > 0,
        "has_redeemable_positions": redeemable_positions > 0,
        "cash_balance_available": cash_balance_available,
        "full_account_equity_view": False,
        "coverage": {
            "source": "runner_account_state_payload" if account_state_payload is not None else "latest_state_fallback",
            "open_orders_status": (
                ((account_state_payload.get("open_orders") or {}).get("status"))
                if account_state_payload is not None
                else ((latest_state_summary.get("open_orders") or {}).get("status"))
            ),
            "positions_status": (
                ((account_state_payload.get("positions") or {}).get("status"))
                if account_state_payload is not None
                else ((latest_state_summary.get("positions") or {}).get("status"))
            ),
            "uses_open_order_notional": True,
            "uses_position_current_value": True,
            "uses_account_cash_balance": cash_balance_available,
            "uses_total_account_equity": False,
        },
        "composition": {
            "open_orders_share_of_visible_capital_usage": ratio_or_none(visible_open_order_notional, visible_capital_usage),
            "position_mark_share_of_visible_capital_usage": ratio_or_none(visible_position_mark, visible_capital_usage),
        },
        "focus_market": focus_market,
        "notes": [
            "visible totals only include open-order notional plus current position current_value from the latest account snapshot",
            (
                "cash balance is sourced from the live trading gateway when auth is available; total account equity is still unavailable"
                if cash_balance_available
                else "cash balance / total account equity is not available from the current live gateway contracts"
            ),
        ],
    }
    if cash_balance_available:
        payload["cash_balance_usd"] = cash_balance_usd
    return payload


def fallback_account_summary(*, latest_state_summary: dict[str, object]) -> dict[str, object] | None:
    open_orders_summary = ((latest_state_summary.get("open_orders") or {}).get("summary") or {})
    positions_summary = ((latest_state_summary.get("positions") or {}).get("summary") or {})
    if not open_orders_summary and not positions_summary:
        return None
    open_order_notional = float_or_none(open_orders_summary.get("total_notional_usd")) or 0.0
    position_mark = float_or_none(positions_summary.get("current_value_sum")) or 0.0
    position_cash_pnl = float_or_none(positions_summary.get("cash_pnl_sum")) or 0.0
    return {
        "snapshot_ts": None,
        "open_orders": open_orders_summary,
        "positions": positions_summary,
        "visible_open_order_notional_usd": open_order_notional,
        "visible_position_mark_usd": position_mark,
        "visible_position_cash_pnl_usd": position_cash_pnl,
        "visible_capital_usage_usd": open_order_notional + position_mark,
        "cash_balance_usd": None,
        "cash_balance_available": False,
    }


def count_redeemable_conditions(*, account_state_payload: dict[str, object] | None) -> int:
    positions_snapshot = {} if not isinstance(account_state_payload, dict) else (account_state_payload.get("positions") or {})
    positions = positions_snapshot.get("positions") if isinstance(positions_snapshot, dict) else []
    positions = positions if isinstance(positions, list) else []
    redeemable_conditions = {
        str(row.get("condition_id") or "").strip()
        for row in positions
        if isinstance(row, dict) and bool(row.get("redeemable")) and str(row.get("condition_id") or "").strip()
    }
    return len(redeemable_conditions)
