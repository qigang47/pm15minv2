from __future__ import annotations

from pathlib import Path

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
