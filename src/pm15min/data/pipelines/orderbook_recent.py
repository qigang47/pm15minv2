from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..io.parquet import read_parquet_if_exists, write_parquet_atomic


DEFAULT_RECENT_ORDERBOOK_WINDOW_MINUTES = 15


def update_recent_orderbook_index(
    *,
    path: Path,
    incoming: pd.DataFrame,
    now_ts_ms: int,
    window_minutes: int = DEFAULT_RECENT_ORDERBOOK_WINDOW_MINUTES,
) -> pd.DataFrame:
    existing = read_parquet_if_exists(path, recover_corrupt=True)
    if existing is None or existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)

    if combined.empty:
        write_parquet_atomic(combined, path)
        return combined

    combined = combined.copy()
    combined["captured_ts_ms"] = pd.to_numeric(combined["captured_ts_ms"], errors="coerce")
    combined = combined.dropna(subset=["captured_ts_ms"])
    combined["captured_ts_ms"] = combined["captured_ts_ms"].astype("int64")
    cutoff_ts_ms = int(now_ts_ms) - max(1, int(window_minutes)) * 60_000
    combined = combined[combined["captured_ts_ms"] >= cutoff_ts_ms]
    combined = combined.sort_values(["captured_ts_ms", "market_id", "token_id", "side"])
    combined = combined.drop_duplicates(
        subset=["captured_ts_ms", "market_id", "token_id", "side"],
        keep="last",
    ).reset_index(drop=True)
    write_parquet_atomic(combined, path)
    return combined


def load_recent_orderbook_index(path: Path) -> pd.DataFrame | None:
    return read_parquet_if_exists(path, recover_corrupt=True)
