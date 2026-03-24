from __future__ import annotations

from typing import Any

import pandas as pd


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


def timestamp_to_iso(value: object) -> str | None:
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    return ts.isoformat()


def minutes_left_to_market_end(*, decision_ts: object, cycle_end_ts: object) -> float | None:
    decision_dt = pd.to_datetime(decision_ts, utc=True, errors="coerce")
    cycle_end_dt = pd.to_datetime(cycle_end_ts, utc=True, errors="coerce")
    if decision_dt is None or cycle_end_dt is None or pd.isna(decision_dt) or pd.isna(cycle_end_dt):
        return None
    delta_minutes = (cycle_end_dt - decision_dt).total_seconds() / 60.0
    return max(0.0, float(delta_minutes))


def resolve_side_probability(*, selected_row: dict[str, Any], side: str) -> float | None:
    p_side = float_or_none(selected_row.get("p_eff_up") if side == "UP" else selected_row.get("p_eff_down"))
    if p_side is not None:
        return p_side
    p_side = float_or_none(selected_row.get("p_up") if side == "UP" else selected_row.get("p_down"))
    if p_side is None:
        p_side = float_or_none(selected_row.get("confidence"))
    return p_side


def quote_captured_ts_ms(*, quote_row: dict[str, Any], side: str) -> int | None:
    key = "quote_captured_ts_ms_up" if side == "UP" else "quote_captured_ts_ms_down"
    value = quote_row.get(key)
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def raw_snapshot_ts_ms(raw: dict[str, Any]) -> int | None:
    for key in ("captured_ts_ms", "orderbook_ts", "logged_at", "source_ts_ms"):
        value = raw.get(key)
        try:
            if value is None:
                continue
            if isinstance(value, (int, float)):
                return int(value)
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is None or pd.isna(ts):
                continue
            return int(ts.timestamp() * 1000)
        except Exception:
            continue
    return None


def normalize_levels(levels: object) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    if not isinstance(levels, list):
        return out
    for row in levels:
        try:
            if isinstance(row, dict):
                price = float(row.get("price"))
                size = float(row.get("size") or row.get("qty"))
            else:
                price = float(row[0])
                size = float(row[1])
        except Exception:
            continue
        if price <= 0 or size <= 0:
            continue
        out.append((price, size))
    out.sort(key=lambda item: item[0])
    return out
