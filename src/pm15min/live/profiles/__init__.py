from __future__ import annotations

import os
from dataclasses import replace
from functools import lru_cache

from dotenv import load_dotenv

from .catalog import DEFAULT_LIVE_PROFILE_SPEC, LIVE_PROFILE_SPECS
from .spec import LiveProfileSpec


def resolve_live_profile_spec(profile: str) -> LiveProfileSpec:
    _ensure_env_loaded()
    token = str(profile or "default").strip().lower()
    spec = LIVE_PROFILE_SPECS.get(token, DEFAULT_LIVE_PROFILE_SPEC)
    stop_trading_override, _ = _resolve_market_env(
        market=None,
        base_name="PM15MIN_STOP_TRADING_BELOW_CASH_USD",
        parser=_float_from_env,
    )
    if stop_trading_override is not None:
        spec = replace(spec, stop_trading_below_cash_usd=max(0.0, float(stop_trading_override)))
    return spec


def resolve_max_trades_per_market(
    *,
    profile_spec: LiveProfileSpec,
    market: str | None = None,
) -> tuple[int, str]:
    _ensure_env_loaded()
    override, source = _resolve_market_env(
        market=market,
        base_name="PM15MIN_MAX_TRADES_PER_MARKET",
        parser=_int_from_env,
    )
    if override is not None:
        return max(0, int(override)), str(source or "env")
    return max(0, int(profile_spec.max_trades_per_market)), "profile_spec"


@lru_cache(maxsize=1)
def _ensure_env_loaded() -> None:
    if os.getenv("PYTEST_CURRENT_TEST"):
        return
    load_dotenv()


def _float_from_env(name: str) -> float | None:
    raw = os.getenv(name)
    if raw in (None, ""):
        return None
    try:
        out = float(raw)
    except Exception:
        return None
    if out != out:
        return None
    return out


def _int_from_env(name: str) -> int | None:
    raw = os.getenv(name)
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except Exception:
        return None


def _resolve_market_env(
    *,
    market: str | None,
    base_name: str,
    parser,
) -> tuple[float | int | None, str | None]:
    market_slug = str(market or "").strip().upper()
    scoped_name = f"{base_name}_{market_slug}" if market_slug else None
    if scoped_name:
        scoped_value = parser(scoped_name)
        if scoped_value is not None:
            return scoped_value, scoped_name
    value = parser(base_name)
    if value is not None:
        return value, base_name
    return None, None
