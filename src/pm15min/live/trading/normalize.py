from __future__ import annotations

from typing import Any

from .contracts import OpenOrderRecord, PositionRecord


def normalize_open_order_row(raw: dict[str, Any]) -> OpenOrderRecord:
    side = first_string(raw, "side")
    return OpenOrderRecord(
        order_id=first_string(raw, "order_id", "orderId", "id"),
        market_id=first_string(raw, "market_id", "marketId", "market"),
        token_id=first_string(raw, "token_id", "tokenId", "asset_id", "assetId", "asset"),
        side=None if side is None else side.upper(),
        status=first_string(raw, "status"),
        price=float_or_none(first_value(raw, "price", "limit_price", "limitPrice")),
        size=float_or_none(first_value(raw, "size", "original_size", "originalSize", "amount")),
        created_at=first_string(raw, "created_at", "createdAt"),
        raw=raw,
    )


def normalize_position_row(raw: dict[str, Any]) -> PositionRecord:
    outcome_index = int_or_none(first_value(raw, "outcomeIndex", "outcome_index"))
    index_set = int_or_none(first_value(raw, "index_set"))
    if index_set is None and outcome_index is not None and outcome_index >= 0:
        index_set = int(1 << outcome_index)
    return PositionRecord(
        market_id=first_string(raw, "marketId", "market_id"),
        condition_id=first_string(raw, "conditionId", "condition_id"),
        token_id=first_string(raw, "asset", "asset_id", "assetId", "token_id", "tokenId"),
        size=float_or_none(first_value(raw, "size")) or 0.0,
        redeemable=bool(raw.get("redeemable")),
        outcome_index=outcome_index,
        index_set=index_set,
        current_value=float_or_none(first_value(raw, "currentValue", "current_value")) or 0.0,
        cash_pnl=float_or_none(first_value(raw, "cashPnl", "cash_pnl")) or 0.0,
        raw=raw,
    )


def first_value(raw: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in raw and raw[key] not in (None, ""):
            return raw[key]
    return None


def first_string(raw: dict[str, Any], *keys: str) -> str | None:
    value = first_value(raw, *keys)
    if value is None:
        return None
    out = str(value).strip()
    return out or None


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
