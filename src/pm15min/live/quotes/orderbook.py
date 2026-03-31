from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.core.orderbook_index import (
    load_orderbook_index_frame,
    load_orderbook_index_frame_cached,
    orderbook_index_journal_path as _orderbook_index_journal_path,
    POST_DECISION_QUOTE_TOLERANCE_MS,
    resolve_orderbook_row,
    resolve_orderbook_row_within_window,
)
from pm15min.data.sources.orderbook_provider import OrderbookProvider
from pm15min.data.sources.polymarket_clob import normalize_book


ORDERBOOK_INDEX_COLUMNS = {
    "captured_ts_ms",
    "market_id",
    "token_id",
    "side",
    "best_ask",
    "best_bid",
    "ask_size_1",
    "bid_size_1",
}
DEFAULT_LATEST_FULL_SNAPSHOT_MAX_AGE_MS = 5_000
DEFAULT_LIVE_PROVIDER_ORDERBOOK_LEVELS = 20
DEFAULT_LIVE_PROVIDER_FUTURE_SKEW_TOLERANCE_MS = 250

def load_latest_full_snapshot_cached(
    *,
    snapshot_path: Path,
    cache: dict[str, dict[str, Any] | None] | None = None,
) -> dict[str, Any] | None:
    cache_key = str(snapshot_path)
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    payload: dict[str, Any] | None = None
    if snapshot_path.exists():
        try:
            raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
        except Exception:
            raw = None
        if isinstance(raw, dict):
            payload = dict(raw)
            payload["records"] = [dict(item) for item in list(raw.get("records") or []) if isinstance(item, dict)]
    if cache is not None:
        cache[cache_key] = payload
    return payload


def resolve_latest_full_snapshot_row(
    snapshot_payload: dict[str, Any] | None,
    *,
    market_id: str,
    token_id: str,
    side: str,
    reference_ts_ms: int | None,
    window_start_ts_ms: int | None,
    window_end_ts_ms: int | None,
    max_age_ms: int = DEFAULT_LATEST_FULL_SNAPSHOT_MAX_AGE_MS,
) -> dict[str, Any] | None:
    if not isinstance(snapshot_payload, dict):
        return None
    selected_market_id = str(market_id or "").strip()
    selected_token_id = str(token_id or "").strip()
    selected_side = str(side or "").strip().lower()
    if not selected_market_id or not selected_token_id or not selected_side:
        return None

    best_record: dict[str, Any] | None = None
    best_ts_ms: int | None = None
    best_pre_window_record: dict[str, Any] | None = None
    best_pre_window_ts_ms: int | None = None
    default_ts_ms = _int_or_none(snapshot_payload.get("captured_ts_ms"))
    for record in list(snapshot_payload.get("records") or []):
        if str(record.get("market_id") or "").strip() != selected_market_id:
            continue
        if str(record.get("token_id") or "").strip() != selected_token_id:
            continue
        if str(record.get("side") or "").strip().lower() != selected_side:
            continue
        captured_ts_ms = _record_snapshot_ts_ms(record, default_ts_ms=default_ts_ms)
        if captured_ts_ms is None:
            continue
        if window_end_ts_ms is not None and captured_ts_ms >= int(window_end_ts_ms):
            continue
        if reference_ts_ms is not None and captured_ts_ms > int(reference_ts_ms):
            continue
        if (
            reference_ts_ms is not None
            and max_age_ms > 0
            and int(reference_ts_ms) - captured_ts_ms > int(max_age_ms)
        ):
            continue
        if window_start_ts_ms is not None and captured_ts_ms < int(window_start_ts_ms):
            if best_pre_window_record is None or best_pre_window_ts_ms is None or captured_ts_ms > best_pre_window_ts_ms:
                best_pre_window_record = record
                best_pre_window_ts_ms = captured_ts_ms
            continue
        if best_record is None or best_ts_ms is None or captured_ts_ms > best_ts_ms:
            best_record = record
            best_ts_ms = captured_ts_ms
    if best_record is None or best_ts_ms is None:
        best_record = best_pre_window_record
        best_ts_ms = best_pre_window_ts_ms
    if best_record is None or best_ts_ms is None:
        return None

    asks = _normalize_depth_levels(best_record.get("asks"), reverse=False)
    bids = _normalize_depth_levels(best_record.get("bids"), reverse=True)
    best_ask = asks[0][0] if asks else None
    best_bid = bids[0][0] if bids else None
    ask_size_1 = asks[0][1] if asks else None
    bid_size_1 = bids[0][1] if bids else None
    spread = None
    if best_ask is not None and best_bid is not None:
        spread = round(float(best_ask) - float(best_bid), 8)
    return {
        "captured_ts_ms": int(best_ts_ms),
        "market_id": selected_market_id,
        "token_id": selected_token_id,
        "side": selected_side,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "ask_size_1": ask_size_1,
        "bid_size_1": bid_size_1,
        "spread": spread,
    }


def build_orderbook_frame_from_provider(
    *,
    provider: OrderbookProvider,
    market_id: str,
    token_up: str,
    token_down: str,
    timeout_sec: float = 1.2,
    levels: int = DEFAULT_LIVE_PROVIDER_ORDERBOOK_LEVELS,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for side, token_id in (("up", token_up), ("down", token_down)):
        try:
            payload = provider.get_orderbook_summary(
                str(token_id),
                levels=max(1, int(levels)),
                timeout=timeout_sec,
                force_refresh=False,
            )
        except Exception:
            payload = None
        if not isinstance(payload, dict):
            continue
        asks, bids, ts_ms = normalize_book(payload)
        if ts_ms is None:
            fetched_at = payload.get("__hub_fetched_at")
            if fetched_at not in (None, ""):
                ts_ms = int(pd.Timestamp(str(fetched_at), tz="UTC").timestamp() * 1000)
        if ts_ms is None:
            continue
        best_ask = asks[0]["price"] if asks else None
        best_bid = bids[0]["price"] if bids else None
        ask_size_1 = asks[0]["size"] if asks else None
        bid_size_1 = bids[0]["size"] if bids else None
        spread = None
        if best_ask is not None and best_bid is not None:
            spread = round(float(best_ask) - float(best_bid), 8)
        rows.append(
            {
                "captured_ts_ms": int(ts_ms),
                "market_id": str(market_id),
                "token_id": str(token_id),
                "side": side,
                "best_ask": best_ask,
                "best_bid": best_bid,
                "ask_size_1": ask_size_1,
                "bid_size_1": bid_size_1,
                "spread": spread,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def live_provider_orderbook_levels() -> int:
    raw = os.getenv("PM15MIN_LIVE_ORDERBOOK_PROVIDER_LEVELS")
    if raw in (None, ""):
        return DEFAULT_LIVE_PROVIDER_ORDERBOOK_LEVELS
    try:
        return max(1, int(raw))
    except Exception:
        return DEFAULT_LIVE_PROVIDER_ORDERBOOK_LEVELS


def live_provider_future_skew_tolerance_ms() -> int:
    raw = os.getenv("PM15MIN_LIVE_ORDERBOOK_PROVIDER_FUTURE_SKEW_TOLERANCE_MS")
    if raw in (None, ""):
        return DEFAULT_LIVE_PROVIDER_FUTURE_SKEW_TOLERANCE_MS
    try:
        return max(0, int(raw))
    except Exception:
        return DEFAULT_LIVE_PROVIDER_FUTURE_SKEW_TOLERANCE_MS


def _normalize_depth_levels(levels: object, *, reverse: bool) -> list[tuple[float, float]]:
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
    out.sort(key=lambda item: item[0], reverse=reverse)
    return out


def _record_snapshot_ts_ms(record: dict[str, Any], *, default_ts_ms: int | None = None) -> int | None:
    value = record.get("captured_ts_ms")
    parsed = _int_or_none(value)
    if parsed is not None:
        return parsed
    value = record.get("source_ts_ms")
    parsed = _int_or_none(value)
    if parsed is not None:
        return parsed
    return default_ts_ms


def _int_or_none(value: object) -> int | None:
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
