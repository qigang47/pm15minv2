from __future__ import annotations

from pathlib import Path
import math
from typing import Any

from .utils import (
    float_or_none,
    minutes_left_to_market_end,
    resolve_side_probability,
    timestamp_to_iso,
)


def resolve_regime_stake_multiplier(*, spec, regime_state: dict[str, Any] | None) -> float:
    if not bool(getattr(spec, "regime_apply_stake_scale", False)):
        return 1.0
    state = str((regime_state or {}).get("state") or "NORMAL").strip().upper()
    if state == "DEFENSE":
        return max(0.0, float(getattr(spec, "regime_defense_stake_multiplier", 1.0) or 0.0))
    if state == "CAUTION":
        return max(0.0, float(getattr(spec, "regime_caution_stake_multiplier", 1.0) or 0.0))
    return 1.0


def load_policy_state(*, rewrite_root: Path, market: str) -> dict[str, Any]:
    from ..account import load_latest_open_orders_snapshot, load_latest_positions_snapshot

    return {
        "open_orders_snapshot": load_latest_open_orders_snapshot(rewrite_root=rewrite_root, market=market),
        "positions_snapshot": load_latest_positions_snapshot(rewrite_root=rewrite_root, market=market),
    }


def build_policy_context(
    *,
    selected_row: dict[str, Any] | None = None,
    quote_row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected = selected_row or {}
    quote = quote_row or {}
    decision_ts = selected.get("decision_ts") or quote.get("decision_ts")
    cycle_end_ts = quote.get("cycle_end_ts")
    return {
        "decision_ts": timestamp_to_iso(decision_ts),
        "cycle_end_ts": timestamp_to_iso(cycle_end_ts),
        "minutes_left_to_market_end": minutes_left_to_market_end(
            decision_ts=decision_ts,
            cycle_end_ts=cycle_end_ts,
        ),
        "market_id": quote.get("market_id"),
        "condition_id": quote.get("condition_id"),
    }


def resolve_execution_account_summary(
    *,
    decision_payload: dict[str, Any],
    policy_state: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    summary = decision_payload.get("account_summary")
    if isinstance(summary, dict):
        return summary
    account_state = decision_payload.get("account_state")
    if isinstance(account_state, dict) and isinstance(account_state.get("summary"), dict):
        return account_state.get("summary")
    if not isinstance(policy_state, dict):
        return None
    open_orders = policy_state.get("open_orders_snapshot")
    positions = policy_state.get("positions_snapshot")
    if not isinstance(open_orders, dict) and not isinstance(positions, dict):
        return None
    from ..account.summary import summarize_account_state_payload

    return summarize_account_state_payload(
        {
            "snapshot_ts": None,
            "open_orders": open_orders if isinstance(open_orders, dict) else None,
            "positions": positions if isinstance(positions, dict) else None,
        }
    )


def resolve_dynamic_stake_base(
    *,
    spec,
    account_summary: dict[str, Any] | None,
) -> tuple[float, dict[str, Any]]:
    max_notional = max(0.0, float(getattr(spec, "max_notional_usd", 0.0) or 0.0))
    fixed_stake = min(max(0.0, float(getattr(spec, "stake_usd", 0.0) or 0.0)), max_notional)
    cash_balance = None if not isinstance(account_summary, dict) else float_or_none(account_summary.get("cash_balance_usd"))
    context = {
        "cash_balance_usd": cash_balance,
        "cash_balance_available": cash_balance is not None,
        "stake_source": "fixed_profile",
    }
    if cash_balance is None:
        return fixed_stake, context

    pct = max(0.0, float(getattr(spec, "stake_cash_pct", 0.0) or 0.0))
    step_threshold = float(getattr(spec, "stake_balance_step_threshold_usd", 0.0) or 0.0)
    step_size = float(getattr(spec, "stake_balance_step_usd", 0.0) or 0.0)
    step_base = float(getattr(spec, "stake_balance_base_usd", 0.0) or 0.0)
    step_increment = float(getattr(spec, "stake_balance_increment_usd", 0.0) or 0.0)
    use_step = step_size > 0.0 and step_increment > 0.0 and step_base > 0.0

    dynamic_stake = None
    if use_step:
        dynamic_stake = step_base
        levels = 0
        if cash_balance > step_threshold > 0.0:
            levels = math.floor((cash_balance - step_threshold) / step_size) + 1
        elif step_threshold <= 0.0:
            levels = math.floor(cash_balance / step_size)
        dynamic_stake = step_base + max(0, levels) * step_increment
        context["stake_source"] = "cash_balance_step"
        context["stake_step_levels"] = max(0, levels)
    elif pct > 0.0:
        dynamic_stake = max(0.0, cash_balance * pct)
        context["stake_source"] = "cash_balance_pct"

    if dynamic_stake is None:
        return fixed_stake, context

    min_stake = float_or_none(getattr(spec, "stake_cash_min_usd", None))
    if min_stake is not None:
        dynamic_stake = max(dynamic_stake, float(min_stake))
    max_stake = float_or_none(getattr(spec, "stake_cash_max_usd", None))
    if max_stake is not None and max_stake > 0.0:
        dynamic_stake = min(dynamic_stake, float(max_stake))
    dynamic_stake = min(dynamic_stake, max_notional)
    dynamic_stake = round(max(0.0, dynamic_stake), 2)
    return dynamic_stake, context


def match_open_orders(
    *,
    open_orders_snapshot: dict[str, Any] | None,
    market_id: object,
    token_id: object,
) -> list[dict[str, Any]]:
    if not isinstance(open_orders_snapshot, dict):
        return []
    if str(open_orders_snapshot.get("status") or "") != "ok":
        return []
    selected_market_id = str(market_id or "").strip()
    selected_token_id = str(token_id or "").strip()
    rows = open_orders_snapshot.get("orders") or []
    matched: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        row_market_id = str(row.get("market_id") or "").strip()
        row_token_id = str(row.get("token_id") or "").strip()
        if selected_market_id and row_market_id == selected_market_id:
            matched.append(row)
            continue
        if selected_token_id and row_token_id == selected_token_id:
            matched.append(row)
    return matched


def repriced_order_guard(
    *,
    spec,
    selected_row: dict[str, Any],
    repriced_entry_price: float,
) -> tuple[dict[str, Any], list[str]]:
    reasons: list[str] = []
    side = str(selected_row.get("recommended_side") or "").upper()
    offset = int(selected_row["offset"])
    p_side = resolve_side_probability(selected_row=selected_row, side=side)
    slippage_bps = float(spec.slippage_bps)
    slip = max(0.0, slippage_bps) / 10000.0
    effective_price = float(repriced_entry_price) * (1.0 + slip)
    fee_rate = spec.fee_rate(price=effective_price)
    roi_threshold = spec.roi_threshold_for(offset=offset)
    roi_net = None if p_side is None else float(p_side) / max(effective_price, 1e-9) - 1.0 - fee_rate
    raw_edge = None if p_side is None else float(p_side) - float(repriced_entry_price)
    min_net_edge = spec.min_net_edge_for(offset=offset, entry_price=repriced_entry_price)

    if spec.entry_price_min is not None and repriced_entry_price < float(spec.entry_price_min):
        reasons.append("repriced_entry_price_min")
    if spec.entry_price_max is not None and repriced_entry_price > float(spec.entry_price_max):
        reasons.append("repriced_entry_price_max")
    if raw_edge is None:
        reasons.append("repriced_side_probability_missing")
    elif raw_edge < min_net_edge:
        reasons.append("repriced_net_edge_below_threshold")
    if roi_net is None:
        reasons.append("repriced_roi_missing")
    elif roi_net < roi_threshold:
        reasons.append("repriced_roi_below_threshold")

    return {
        "repriced_entry_price": float(repriced_entry_price),
        "repriced_effective_price": float(effective_price),
        "repriced_fee_rate": float(fee_rate),
        "repriced_raw_edge": raw_edge,
        "repriced_min_net_edge_required": float(min_net_edge),
        "repriced_roi_net": roi_net,
        "repriced_roi_threshold_required": float(roi_threshold),
    }, reasons
