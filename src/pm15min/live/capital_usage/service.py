from __future__ import annotations

from ..account import summarize_account_state_payload
from .context import (
    build_focus_market_usage,
    resolve_focus_market_context,
    resolve_last_iteration_execution,
    resolve_last_iteration_regime_state,
)
from .overview import build_account_overview, fallback_account_summary
from ..operator.utils import float_or_none
from ..profiles import resolve_live_profile_spec


def build_live_capital_usage_summary(
    *,
    canonical_scope: dict[str, object],
    latest_state_summary: dict[str, object],
    last_iteration: dict[str, object],
    decision_reject_diagnostics: dict[str, object] | None,
) -> dict[str, object] | None:
    profile = str(canonical_scope.get("profile") or "").strip()
    if not profile:
        return None
    spec = resolve_live_profile_spec(profile)
    account_state_payload = last_iteration.get("account_state_payload")
    account_summary = summarize_account_state_payload(account_state_payload if isinstance(account_state_payload, dict) else None)
    if account_summary is None:
        account_summary = fallback_account_summary(latest_state_summary=latest_state_summary)
    if account_summary is None:
        return None

    execution = resolve_last_iteration_execution(last_iteration=last_iteration)
    regime_state = resolve_last_iteration_regime_state(last_iteration=last_iteration, latest_state_summary=latest_state_summary)
    focus_context = resolve_focus_market_context(
        last_iteration=last_iteration,
        decision_reject_diagnostics=decision_reject_diagnostics,
    )
    focus_usage = build_focus_market_usage(
        account_state_payload=account_state_payload if isinstance(account_state_payload, dict) else None,
        focus_context=focus_context,
    )
    requested_notional = float_or_none(execution.get("requested_notional_usd"))
    max_notional = float_or_none(getattr(spec, "max_notional_usd", None))
    requested_vs_max_ratio = None
    if requested_notional is not None and max_notional not in (None, 0.0):
        requested_vs_max_ratio = float(requested_notional) / float(max_notional)

    defense_cap = int(getattr(spec, "regime_defense_max_trades_per_market", 0) or 0)
    trade_slots_remaining = None
    if defense_cap > 0 and (focus_usage.get("market_id") is not None or focus_usage.get("condition_id") is not None):
        trade_slots_remaining = max(0, defense_cap - int(focus_usage.get("active_trade_count") or 0))

    interpretation = None
    regime_name = str(regime_state.get("state") or "").strip().upper()
    if (
        regime_name == "DEFENSE"
        and defense_cap > 0
        and (focus_usage.get("market_id") is not None or focus_usage.get("condition_id") is not None)
        and int(focus_usage.get("active_trade_count") or 0) >= defense_cap
    ):
        interpretation = "defense_trade_cap_reached"
    elif bool(getattr(spec, "regime_apply_stake_scale", False)) and requested_notional is not None and requested_vs_max_ratio is not None and requested_vs_max_ratio < 1.0:
        interpretation = "stake_scaled_by_regime"

    execution_budget = {
        "stake_base_usd": float_or_none(execution.get("stake_base_usd")),
        "stake_multiplier": float_or_none(execution.get("stake_multiplier")),
        "stake_regime_state": execution.get("stake_regime_state"),
        "requested_notional_usd": requested_notional,
        "max_notional_usd": max_notional,
        "requested_vs_max_notional_ratio": requested_vs_max_ratio,
    }
    stake_source = execution.get("stake_source")
    if stake_source is not None:
        execution_budget["stake_source"] = stake_source
    execution_cash_balance = float_or_none(execution.get("cash_balance_usd"))
    if execution_cash_balance is not None:
        execution_budget["cash_balance_usd"] = execution_cash_balance

    return {
        "account_snapshot_ts": account_summary.get("snapshot_ts"),
        "coverage": {
            "open_orders_status": (
                ((account_state_payload.get("open_orders") or {}).get("status"))
                if isinstance(account_state_payload, dict)
                else ((latest_state_summary.get("open_orders") or {}).get("status"))
            ),
            "positions_status": (
                ((account_state_payload.get("positions") or {}).get("status"))
                if isinstance(account_state_payload, dict)
                else ((latest_state_summary.get("positions") or {}).get("status"))
            ),
            "uses_open_order_notional": True,
            "uses_position_current_value": True,
            "uses_account_cash_balance": bool(account_summary.get("cash_balance_available", False)),
            "is_full_account_equity_view": False,
        },
        "portfolio": {
            "visible_open_order_notional_usd": account_summary.get("visible_open_order_notional_usd"),
            "visible_position_mark_usd": account_summary.get("visible_position_mark_usd"),
            "visible_position_cash_pnl_usd": account_summary.get("visible_position_cash_pnl_usd"),
            "visible_capital_usage_usd": account_summary.get("visible_capital_usage_usd"),
            "cash_balance_usd": account_summary.get("cash_balance_usd"),
            "open_orders": account_summary.get("open_orders"),
            "positions": account_summary.get("positions"),
        },
        "account_overview": build_account_overview(
            account_summary=account_summary,
            account_state_payload=account_state_payload if isinstance(account_state_payload, dict) else None,
            latest_state_summary=latest_state_summary,
            focus_usage=focus_usage,
        ),
        "focus_market": focus_usage,
        "execution_budget": execution_budget,
        "regime_context": {
            "state": regime_state.get("state"),
            "pressure": regime_state.get("pressure"),
            "regime_apply_stake_scale": bool(getattr(spec, "regime_apply_stake_scale", False)),
            "defense_max_trades_per_market": defense_cap,
            "current_market_trade_slots_remaining": trade_slots_remaining,
        },
        "interpretation": interpretation,
        "notes": [
            "visible_capital_usage_usd only tracks open-order notional plus current_value from the latest account snapshot",
            (
                "cash balance is available when the live account snapshot was built with an authenticated trading gateway"
                if bool(account_summary.get("cash_balance_available", False))
                else "cash balance / total account equity is not available from the current live gateway contracts"
            ),
        ],
    }
