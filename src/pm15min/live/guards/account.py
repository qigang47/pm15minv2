from __future__ import annotations

from typing import Any

from ..profiles import LiveProfileSpec


def cash_balance_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    account_context: dict[str, Any],
) -> list[str]:
    threshold = float(getattr(profile_spec, "stop_trading_below_cash_usd", 0.0) or 0.0)
    if threshold <= 0.0:
        return []
    if not bool(account_context.get("cash_balance_available", False)):
        return ["cash_balance_unavailable"]
    balance = float_or_none(account_context.get("cash_balance_usd"))
    if balance is None or balance > threshold:
        return []
    return ["cash_balance_stop"]


def max_open_markets_guard_reasons(
    *,
    profile_spec: LiveProfileSpec,
    account_context: dict[str, Any],
) -> list[str]:
    cap = int(getattr(profile_spec, "max_open_markets", 0) or 0)
    if cap <= 0:
        return []
    if not bool(account_context.get("account_state_available", False)):
        return ["account_state_unavailable"]
    if bool(account_context.get("current_market_active", False)):
        return []
    active_market_count = int(account_context.get("active_market_count") or 0)
    if active_market_count < cap:
        return []
    return ["max_open_markets_reached"]


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
