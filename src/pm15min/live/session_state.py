from __future__ import annotations


def normalize_trade_side(value: object) -> str | None:
    token = str(value or "").strip().upper()
    if token in {"UP", "DOWN"}:
        return token
    return None


def build_market_offset_trade_count_key(*, market_id: object, offset: object) -> str | None:
    market_token = str(market_id or "").strip()
    offset_token = int_or_none(offset)
    if not market_token or offset_token is None:
        return None
    return f"{market_token}_{offset_token}"


def build_market_offset_side_trade_count_key(
    *,
    market_id: object,
    offset: object,
    side: object,
) -> str | None:
    base_key = build_market_offset_trade_count_key(market_id=market_id, offset=offset)
    side_token = normalize_trade_side(side)
    if base_key is None or side_token is None:
        return None
    return f"{base_key}_{side_token}"


def int_or_none(value: object) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None
