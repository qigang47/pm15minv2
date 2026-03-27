from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

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
POST_DECISION_QUOTE_TOLERANCE_MS = 120_000
DEFAULT_LATEST_FULL_SNAPSHOT_MAX_AGE_MS = 5_000


def _orderbook_index_journal_path(index_path: Path) -> Path:
    return index_path.with_name(f"{index_path.name}.journal.jsonl")


def _load_orderbook_index_journal_frame(journal_path: Path) -> pd.DataFrame:
    if not journal_path.exists():
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    try:
        with journal_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def resolve_orderbook_row(
    index_df: pd.DataFrame,
    *,
    market_id: str,
    token_id: str,
    side: str,
    decision_ts_ms: int | None,
):
    df = index_df.copy()
    df = df[
        (df["market_id"].astype(str) == str(market_id))
        & (df["token_id"].astype(str) == str(token_id))
        & (df["side"].astype(str).str.lower() == str(side).lower())
    ]
    df["captured_ts_ms"] = pd.to_numeric(df["captured_ts_ms"], errors="coerce")
    df = df.dropna(subset=["captured_ts_ms"])
    if df.empty:
        return None
    if decision_ts_ms is None:
        return df.sort_values("captured_ts_ms").iloc[-1].to_dict()

    past = df[df["captured_ts_ms"] <= int(decision_ts_ms)]
    if not past.empty:
        return past.sort_values("captured_ts_ms").iloc[-1].to_dict()

    future = df[
        (df["captured_ts_ms"] > int(decision_ts_ms))
        & (df["captured_ts_ms"] <= int(decision_ts_ms) + POST_DECISION_QUOTE_TOLERANCE_MS)
    ]
    if not future.empty:
        return future.sort_values("captured_ts_ms").iloc[0].to_dict()
    return None


def resolve_orderbook_row_within_window(
    index_df: pd.DataFrame,
    *,
    market_id: str,
    token_id: str,
    side: str,
    reference_ts_ms: int | None,
    window_start_ts_ms: int | None,
    window_end_ts_ms: int | None,
):
    df = index_df.copy()
    df = df[
        (df["market_id"].astype(str) == str(market_id))
        & (df["token_id"].astype(str) == str(token_id))
        & (df["side"].astype(str).str.lower() == str(side).lower())
    ]
    df["captured_ts_ms"] = pd.to_numeric(df["captured_ts_ms"], errors="coerce")
    df = df.dropna(subset=["captured_ts_ms"])
    if df.empty:
        return None
    if window_end_ts_ms is not None:
        df = df[df["captured_ts_ms"] < int(window_end_ts_ms)]
    if df.empty:
        return None
    if reference_ts_ms is not None:
        df = df[df["captured_ts_ms"] <= int(reference_ts_ms)]
        if df.empty:
            return None
    df = df.sort_values("captured_ts_ms")
    if window_start_ts_ms is None:
        return df.iloc[-1].to_dict()
    in_window = df[df["captured_ts_ms"] >= int(window_start_ts_ms)]
    if not in_window.empty:
        return in_window.iloc[-1].to_dict()
    return df.iloc[-1].to_dict()


def load_orderbook_index_frame(
    *,
    index_path: Path,
    recent_path: Path | None = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if recent_path is not None and recent_path.exists():
        frames.append(pd.read_parquet(recent_path))
    if index_path.exists():
        frames.append(pd.read_parquet(index_path))
    journal_df = _load_orderbook_index_journal_frame(_orderbook_index_journal_path(index_path))
    if not journal_df.empty:
        frames.append(journal_df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    if combined.empty:
        return combined
    if {"captured_ts_ms", "market_id", "token_id", "side"}.issubset(set(combined.columns)):
        combined["captured_ts_ms"] = pd.to_numeric(combined["captured_ts_ms"], errors="coerce")
        combined = combined.dropna(subset=["captured_ts_ms"])
        combined["captured_ts_ms"] = combined["captured_ts_ms"].astype("int64")
        combined = combined.sort_values(["captured_ts_ms", "market_id", "token_id", "side"])
        combined = combined.drop_duplicates(
            subset=["captured_ts_ms", "market_id", "token_id", "side"],
            keep="last",
        ).reset_index(drop=True)
    return combined


def load_orderbook_index_frame_cached(
    *,
    index_path: Path,
    recent_path: Path | None = None,
    cache: dict[tuple[str, str | None], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    cache_key = (str(index_path), None if recent_path is None else str(recent_path))
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    frame = load_orderbook_index_frame(index_path=index_path, recent_path=recent_path)
    if cache is not None:
        cache[cache_key] = frame
    return frame


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
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for side, token_id in (("up", token_up), ("down", token_down)):
        try:
            payload = provider.get_orderbook_summary(
                str(token_id),
                levels=0,
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
