from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


POST_DECISION_QUOTE_TOLERANCE_MS = 120_000


def orderbook_index_journal_path(index_path: Path) -> Path:
    return index_path.with_name(f"{index_path.name}.journal.jsonl")


def load_orderbook_index_journal_frame(journal_path: Path) -> pd.DataFrame:
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
    journal_df = load_orderbook_index_journal_frame(orderbook_index_journal_path(index_path))
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
