from __future__ import annotations

from typing import Any

from ..profiles import LiveProfileSpec, resolve_max_trades_per_market
from ..session_state import (
    build_market_offset_side_trade_count_key,
    build_market_offset_trade_count_key,
    normalize_trade_side,
)


def liquidity_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    liquidity_state: dict[str, Any] | None,
) -> list[str]:
    if not bool(profile_spec.liquidity_guard_enabled):
        return []
    if not bool(profile_spec.liquidity_guard_block):
        return []
    state = liquidity_state or {}
    if not isinstance(state, dict):
        return []
    if not bool(state.get("blocked", False)):
        return []
    reasons = ["liquidity_guard_blocked"]
    for code in state.get("reason_codes") or []:
        token = str(code or "").strip()
        if token and token not in {"ok", "disabled"}:
            reasons.append(f"liquidity_{token}")
    return reasons


def regime_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    regime_state: dict[str, Any] | None,
    base_threshold: float,
    chosen_prob: float,
) -> list[str]:
    if not bool(profile_spec.regime_controller_enabled):
        return []
    if not bool(profile_spec.liquidity_guard_block):
        return []
    state = regime_state or {}
    if not isinstance(state, dict):
        return []
    if str(state.get("status") or "ok") != "ok":
        return []

    regime_name = str(state.get("state") or "").strip().upper()
    if not regime_name or regime_name == "NORMAL":
        return []

    reasons: list[str] = []
    offset = int(signal_row["offset"])
    side = str(signal_row.get("recommended_side") or "").strip().upper()
    disabled_offsets = set(profile_spec.regime_disabled_offsets_for(regime_name))
    if offset in disabled_offsets:
        reasons.append("regime_offset_disabled")

    pressure = str(state.get("pressure") or "").strip().lower()
    if regime_name == "DEFENSE" and bool(profile_spec.regime_defense_force_with_pressure):
        if pressure == "up" and side == "DOWN":
            reasons.append("regime_direction_pressure")
        elif pressure == "down" and side == "UP":
            reasons.append("regime_direction_pressure")

    boost = profile_spec.regime_min_dir_prob_boost_for(regime_name)
    if boost > 0.0 and float(chosen_prob) < min(0.99, float(base_threshold) + float(boost)):
        reasons.append("regime_direction_prob")

    return reasons


def build_account_context(
    *,
    market: str,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    quote_row: dict[str, Any] | None,
    regime_state: dict[str, Any] | None,
    account_state: dict[str, Any] | None,
    session_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    quote = quote_row or {}
    state = account_state or {}
    market_id = str(quote.get("market_id") or "").strip() or None
    offset = int_or_none(signal_row.get("offset"))
    open_orders_snapshot = state.get("open_orders") or {}
    positions_snapshot = state.get("positions") or {}
    account_summary = state.get("summary") if isinstance(state.get("summary"), dict) else {}
    open_orders_summary = open_orders_snapshot.get("summary") if isinstance(open_orders_snapshot, dict) else {}
    positions_summary = positions_snapshot.get("summary") if isinstance(positions_snapshot, dict) else {}
    open_orders_summary = open_orders_summary if isinstance(open_orders_summary, dict) else {}
    positions_summary = positions_summary if isinstance(positions_summary, dict) else {}
    open_orders = open_orders_snapshot.get("orders") if isinstance(open_orders_snapshot, dict) else open_orders_snapshot
    positions = positions_snapshot.get("positions") if isinstance(positions_snapshot, dict) else positions_snapshot
    open_orders = open_orders if isinstance(open_orders, list) else []
    positions = positions if isinstance(positions, list) else []
    active_market_ids = {
        str(value)
        for value in (
            account_summary.get("active_market_ids")
            or open_orders_summary.get("market_ids")
            or positions_summary.get("market_ids")
            or []
        )
        if str(value)
    }
    if not active_market_ids:
        active_market_ids = {
            str(row.get("market_id") or "").strip()
            for row in open_orders
            if isinstance(row, dict) and str(row.get("market_id") or "").strip()
        }
        active_market_ids.update(
            str(row.get("market_id") or "").strip()
            for row in positions
            if (
                isinstance(row, dict)
                and str(row.get("market_id") or "").strip()
                and (float_or_none(row.get("size")) or 0.0) > 0.0
            )
        )
    cash_balance_usd = float_or_none(account_summary.get("cash_balance_usd"))
    cap_context = _resolve_trade_count_context(
        profile_spec=profile_spec,
        market=market,
        regime_state=regime_state,
    )
    selected_side = normalize_trade_side(signal_row.get("recommended_side"))
    session_trade_count, trade_count_key, trade_count_scope = resolve_session_trade_count(
        cap_context=cap_context,
        session_state=session_state,
        market_id=market_id,
        offset=offset,
        side=selected_side,
    )
    base_context = {
        "selected_offset": offset,
        "selected_side": selected_side,
        "session_trade_count": int(session_trade_count),
        "session_trade_count_key": trade_count_key,
        "session_trade_count_scope": trade_count_scope,
        "session_trade_count_lock_side": trade_count_scope == "market_offset_side",
        "max_trades_per_market_base": int(cap_context["base_cap"]),
        "max_trades_per_market_effective": int(cap_context["effective_cap"]),
        "max_trades_per_market_source": cap_context["base_cap_source"],
        "regime_defense_trade_cap_applied": bool(cap_context["defense_cap_applied"]),
        "regime_defense_max_trades_per_market": int(cap_context["defense_cap"]),
    }
    if market_id is None:
        return {
            "market_id": None,
            "open_orders_count": 0,
            "position_count": 0,
            "active_trade_count": 0,
            "active_market_ids": sorted(active_market_ids),
            "active_market_count": len(active_market_ids),
            "current_market_active": False,
            "account_state_available": bool(state),
            "cash_balance_usd": cash_balance_usd,
            "cash_balance_available": cash_balance_usd is not None,
            **base_context,
        }
    open_orders_count = int_or_none((open_orders_summary.get("by_market_id") or {}).get(market_id))
    if open_orders_count is None:
        open_orders_count = sum(
            1
            for row in open_orders
            if isinstance(row, dict) and str(row.get("market_id") or "").strip() == market_id
        )
    position_count = int_or_none((positions_summary.get("by_market_id") or {}).get(market_id))
    if position_count is None:
        position_count = sum(
            1
            for row in positions
            if (
                isinstance(row, dict)
                and str(row.get("market_id") or "").strip() == market_id
                and (float_or_none(row.get("size")) or 0.0) > 0.0
            )
        )
    return {
        "market_id": market_id,
        "open_orders_count": int(open_orders_count),
        "position_count": int(position_count),
        "active_trade_count": int(open_orders_count + position_count),
        "active_market_ids": sorted(active_market_ids),
        "active_market_count": len(active_market_ids),
        "current_market_active": market_id in active_market_ids,
        "account_state_available": bool(state),
        "cash_balance_usd": cash_balance_usd,
        "cash_balance_available": cash_balance_usd is not None,
        **base_context,
    }


def trade_count_cap_reasons(
    *,
    profile_spec: LiveProfileSpec,
    regime_state: dict[str, Any] | None,
    account_context: dict[str, Any],
) -> list[str]:
    effective_cap = int_or_none(account_context.get("max_trades_per_market_effective")) or 0
    if effective_cap <= 0:
        return []
    session_trade_count = int_or_none(account_context.get("session_trade_count")) or 0
    if session_trade_count < effective_cap:
        return []
    reasons = ["max_trades_per_offset"]
    if bool(account_context.get("regime_defense_trade_cap_applied")):
        reasons.append("regime_trade_count_cap")
    return reasons


def _resolve_trade_count_context(
    *,
    profile_spec: LiveProfileSpec,
    market: str,
    regime_state: dict[str, Any] | None,
) -> dict[str, Any]:
    base_cap, base_cap_source = resolve_max_trades_per_market(profile_spec=profile_spec, market=market)
    repeat_cap = (
        max(0, int_or_none(getattr(profile_spec, "repeat_same_decision_max_trades", 0)) or 0)
        if bool(getattr(profile_spec, "repeat_same_decision_enabled", False))
        else 0
    )
    if repeat_cap > 0:
        if base_cap <= 0 or repeat_cap < int(base_cap):
            base_cap = int(repeat_cap)
            base_cap_source = "repeat_same_decision_max_trades"
    effective_cap = int(base_cap)
    defense_cap = max(0, int(profile_spec.regime_defense_max_trades_per_market))
    defense_cap_applied = False
    state = regime_state or {}
    regime_name = ""
    if bool(profile_spec.regime_controller_enabled) and str(state.get("status") or "ok") == "ok":
        regime_name = str(state.get("state") or "").strip().upper()
    if regime_name == "DEFENSE" and defense_cap > 0:
        if effective_cap <= 0:
            effective_cap = defense_cap
            defense_cap_applied = True
        elif defense_cap < effective_cap:
            effective_cap = defense_cap
            defense_cap_applied = True
    count_scope = "market_offset"
    if (
        base_cap_source == "repeat_same_decision_max_trades"
        and not defense_cap_applied
        and bool(profile_spec.repeat_same_decision_lock_side)
    ):
        count_scope = "market_offset_side"
    return {
        "base_cap": int(base_cap),
        "base_cap_source": base_cap_source,
        "effective_cap": int(effective_cap),
        "defense_cap": int(defense_cap),
        "defense_cap_applied": bool(defense_cap_applied),
        "count_scope": count_scope,
    }


def resolve_session_trade_count(
    *,
    cap_context: dict[str, Any],
    session_state: dict[str, Any] | None,
    market_id: object,
    offset: object,
    side: object,
) -> tuple[int, str | None, str]:
    counts = _session_trade_count_map(session_state)
    side_counts = _session_trade_count_map(session_state, side_aware=True)
    base_key = build_market_offset_trade_count_key(market_id=market_id, offset=offset)
    if base_key is None:
        return 0, None, str(cap_context.get("count_scope") or "market_offset")
    count_scope = str(cap_context.get("count_scope") or "market_offset")
    if count_scope == "market_offset_side":
        side_key = build_market_offset_side_trade_count_key(
            market_id=market_id,
            offset=offset,
            side=side,
        )
        if side_key is not None:
            return int(int_or_none(side_counts.get(side_key)) or 0), side_key, count_scope
    return int(int_or_none(counts.get(base_key)) or 0), base_key, "market_offset"


def _session_trade_count_map(session_state: dict[str, Any] | None, *, side_aware: bool = False) -> dict[str, Any]:
    if not isinstance(session_state, dict):
        return {}
    state_key = "market_offset_side_trade_count" if side_aware else "market_offset_trade_count"
    counts = session_state.get(state_key)
    if isinstance(counts, dict):
        return counts
    return {}


def int_or_none(value) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def float_or_none(value) -> float | None:
    try:
        if value is None:
            return None
        out = float(value)
    except Exception:
        return None
    if out != out:
        return None
    return out
