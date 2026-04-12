from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from threading import Lock
import time
from typing import Any

import pandas as pd

from ..io.parquet import read_parquet_if_exists, write_parquet_atomic


DEFAULT_RECENT_ORDERBOOK_WINDOW_MINUTES = 15
DEFAULT_RECENT_ORDERBOOK_PERSIST_INTERVAL_SECONDS = 0.0
_RECENT_INDEX_STATE_LOCK = Lock()


@dataclass
class _RecentIndexState:
    loaded: bool = False
    rows_by_key: dict[tuple[int, str, str, str], dict[str, Any]] = field(default_factory=dict)
    last_persist_monotonic: float = 0.0


_RECENT_INDEX_STATE: dict[str, _RecentIndexState] = {}


def _recent_state(path: Path) -> _RecentIndexState:
    cache_key = str(path)
    with _RECENT_INDEX_STATE_LOCK:
        state = _RECENT_INDEX_STATE.get(cache_key)
        if state is None:
            state = _RecentIndexState()
            _RECENT_INDEX_STATE[cache_key] = state
        return state


def _row_key(row: dict[str, Any]) -> tuple[int, str, str, str] | None:
    try:
        captured_ts_ms = int(pd.to_numeric(row.get("captured_ts_ms"), errors="coerce"))
    except Exception:
        return None
    if captured_ts_ms <= 0:
        return None
    return (
        captured_ts_ms,
        str(row.get("market_id") or ""),
        str(row.get("token_id") or ""),
        str(row.get("side") or "").lower(),
    )


def _seed_recent_state_from_disk(*, state: _RecentIndexState, path: Path) -> None:
    if state.loaded:
        return
    existing = read_parquet_if_exists(path, recover_corrupt=True)
    if existing is not None and not existing.empty:
        for row in existing.to_dict("records"):
            key = _row_key(row)
            if key is not None:
                state.rows_by_key[key] = row
    state.loaded = True


def _build_recent_frame(*, state: _RecentIndexState) -> pd.DataFrame:
    if not state.rows_by_key:
        return pd.DataFrame()
    combined = pd.DataFrame(list(state.rows_by_key.values()))
    combined["captured_ts_ms"] = pd.to_numeric(combined["captured_ts_ms"], errors="coerce")
    combined = combined.dropna(subset=["captured_ts_ms"])
    if combined.empty:
        return combined
    combined["captured_ts_ms"] = combined["captured_ts_ms"].astype("int64")
    combined = combined.sort_values(["captured_ts_ms", "market_id", "token_id", "side"]).reset_index(drop=True)
    return combined


def update_recent_orderbook_index(
    *,
    path: Path,
    incoming: pd.DataFrame,
    now_ts_ms: int,
    window_minutes: int = DEFAULT_RECENT_ORDERBOOK_WINDOW_MINUTES,
    persist_interval_seconds: float = DEFAULT_RECENT_ORDERBOOK_PERSIST_INTERVAL_SECONDS,
    force_persist: bool = False,
) -> pd.DataFrame:
    state = _recent_state(path)
    _seed_recent_state_from_disk(state=state, path=path)

    for row in incoming.to_dict("records"):
        key = _row_key(row)
        if key is not None:
            state.rows_by_key[key] = row

    cutoff_ts_ms = int(now_ts_ms) - max(1, int(window_minutes)) * 60_000
    state.rows_by_key = {
        key: row for key, row in state.rows_by_key.items() if int(key[0]) >= cutoff_ts_ms
    }

    combined = _build_recent_frame(state=state)
    interval_seconds = max(0.0, float(persist_interval_seconds))
    should_persist = bool(force_persist or not path.exists())
    now_monotonic = time.monotonic()
    if not should_persist:
        if interval_seconds <= 0.0:
            should_persist = True
        else:
            should_persist = now_monotonic - float(state.last_persist_monotonic) >= interval_seconds
    if should_persist:
        write_parquet_atomic(combined, path)
        state.last_persist_monotonic = now_monotonic
    return combined


def load_recent_orderbook_index(path: Path) -> pd.DataFrame | None:
    return read_parquet_if_exists(path, recover_corrupt=True)
