from __future__ import annotations

from typing import Any


def summarize_open_orders_snapshot(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("orders")
    return summarize_open_orders_rows(rows if isinstance(rows, list) else [])


def summarize_positions_snapshot(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    rows = payload.get("positions")
    redeem_plan = payload.get("redeem_plan")
    summary = summarize_positions_rows(
        rows if isinstance(rows, list) else [],
        redeem_plan=redeem_plan if isinstance(redeem_plan, dict) else {},
    )
    cash_balance_usd = float_or_none(payload.get("cash_balance_usd"))
    summary["cash_balance_usd"] = cash_balance_usd
    summary["cash_balance_available"] = cash_balance_usd is not None
    summary["cash_balance_status"] = payload.get("cash_balance_status")
    return summary


def summarize_account_state_payload(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    open_orders_summary = summarize_open_orders_snapshot(payload.get("open_orders"))
    positions_summary = summarize_positions_snapshot(payload.get("positions"))
    open_orders_notional = float_or_none(None if open_orders_summary is None else open_orders_summary.get("total_notional_usd")) or 0.0
    position_mark = float_or_none(None if positions_summary is None else positions_summary.get("current_value_sum")) or 0.0
    position_cash_pnl = float_or_none(None if positions_summary is None else positions_summary.get("cash_pnl_sum")) or 0.0
    cash_balance_usd = float_or_none(None if positions_summary is None else positions_summary.get("cash_balance_usd"))
    open_order_markets = set(
        [] if open_orders_summary is None else [str(value) for value in open_orders_summary.get("market_ids") or [] if str(value)]
    )
    position_markets = set(
        [] if positions_summary is None else [str(value) for value in positions_summary.get("market_ids") or [] if str(value)]
    )
    active_market_ids = sorted(open_order_markets | position_markets)
    return {
        "snapshot_ts": payload.get("snapshot_ts"),
        "open_orders": open_orders_summary,
        "positions": positions_summary,
        "visible_open_order_notional_usd": open_orders_notional,
        "visible_position_mark_usd": position_mark,
        "visible_position_cash_pnl_usd": position_cash_pnl,
        "visible_capital_usage_usd": open_orders_notional + position_mark,
        "cash_balance_usd": cash_balance_usd,
        "cash_balance_available": cash_balance_usd is not None,
        "active_market_ids": active_market_ids,
        "active_market_count": len(active_market_ids),
    }


def build_redeem_plan(positions: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    plan: dict[str, dict[str, Any]] = {}
    for row in positions:
        condition_id = str(row.get("condition_id") or "").strip()
        if not condition_id:
            continue
        size = float_or_none(row.get("size")) or 0.0
        if size <= 0:
            continue
        if not bool(row.get("redeemable")):
            continue
        current_value = float_or_none(row.get("current_value")) or 0.0
        cash_pnl = float_or_none(row.get("cash_pnl")) or 0.0
        if current_value <= 0.0 and cash_pnl <= 0.0:
            continue
        index_set = int_or_none(row.get("index_set"))
        if index_set is None or index_set <= 0:
            continue
        rec = plan.setdefault(
            condition_id,
            {
                "condition_id": condition_id,
                "index_sets": [],
                "positions_count": 0,
                "size_sum": 0.0,
                "current_value_sum": 0.0,
                "cash_pnl_sum": 0.0,
            },
        )
        rec["positions_count"] = int(rec["positions_count"]) + 1
        rec["size_sum"] = float(rec["size_sum"]) + float(size)
        rec["current_value_sum"] = float(rec["current_value_sum"]) + float(current_value)
        rec["cash_pnl_sum"] = float(rec["cash_pnl_sum"]) + float(cash_pnl)
        existing_index_sets = {int(x) for x in rec["index_sets"]}
        if int(index_set) not in existing_index_sets:
            rec["index_sets"] = sorted([*rec["index_sets"], int(index_set)])
    return plan


def summarize_open_orders_rows(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_market_id: dict[str, int] = {}
    by_token_id: dict[str, int] = {}
    by_market_notional_usd: dict[str, float] = {}
    by_token_notional_usd: dict[str, float] = {}
    total_notional_usd = 0.0
    for row in rows:
        market_id = str(row.get("market_id") or "").strip()
        token_id = str(row.get("token_id") or "").strip()
        price = float_or_none(row.get("price")) or 0.0
        size = float_or_none(row.get("size")) or 0.0
        notional = max(0.0, float(price) * float(size))
        total_notional_usd += notional
        if market_id:
            by_market_id[market_id] = by_market_id.get(market_id, 0) + 1
            by_market_notional_usd[market_id] = by_market_notional_usd.get(market_id, 0.0) + notional
        if token_id:
            by_token_id[token_id] = by_token_id.get(token_id, 0) + 1
            by_token_notional_usd[token_id] = by_token_notional_usd.get(token_id, 0.0) + notional
    return {
        "total_orders": len(rows),
        "total_notional_usd": total_notional_usd,
        "market_count": len(by_market_id),
        "market_ids": sorted(by_market_id),
        "token_count": len(by_token_id),
        "by_market_id": by_market_id,
        "by_market_notional_usd": by_market_notional_usd,
        "by_token_id": by_token_id,
        "by_token_notional_usd": by_token_notional_usd,
    }


def summarize_positions_rows(rows: list[dict[str, Any]], *, redeem_plan: dict[str, Any]) -> dict[str, Any]:
    condition_ids: set[str] = set()
    market_ids: set[str] = set()
    total_size = 0.0
    current_value_sum = 0.0
    cash_pnl_sum = 0.0
    redeemable_positions = 0
    positions_without_market_id = 0
    for row in rows:
        condition_id = str(row.get("condition_id") or "").strip()
        market_id = str(row.get("market_id") or "").strip()
        size = float_or_none(row.get("size")) or 0.0
        current_value = float_or_none(row.get("current_value")) or 0.0
        cash_pnl = float_or_none(row.get("cash_pnl")) or 0.0
        total_size += float(size)
        current_value_sum += float(current_value)
        cash_pnl_sum += float(cash_pnl)
        if bool(row.get("redeemable")):
            redeemable_positions += 1
        if condition_id:
            condition_ids.add(condition_id)
        if market_id:
            market_ids.add(market_id)
        else:
            positions_without_market_id += 1
    return {
        "total_positions": len(rows),
        "redeemable_positions": int(redeemable_positions),
        "redeemable_conditions": len(redeem_plan),
        "size_sum": total_size,
        "current_value_sum": current_value_sum,
        "cash_pnl_sum": cash_pnl_sum,
        "condition_count": len(condition_ids),
        "condition_ids": sorted(condition_ids),
        "market_count": len(market_ids),
        "market_ids": sorted(market_ids),
        "positions_without_market_id": positions_without_market_id,
    }


def float_or_none(value: object) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out


def int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None
