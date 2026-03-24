from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from pm15min.data.io.parquet import read_parquet_if_exists
from ..operator.utils import read_json_path
from .orderbook import POST_DECISION_QUOTE_TOLERANCE_MS


def summarize_orderbook_hot_cache(
    *,
    recent_path: Path,
    state_path: Path,
    now: pd.Timestamp | None = None,
    stale_after_ms: int = POST_DECISION_QUOTE_TOLERANCE_MS,
) -> dict[str, Any]:
    now_ts = pd.Timestamp(now) if now is not None else pd.Timestamp.now(tz="UTC")
    now_ts = now_ts.tz_convert("UTC") if now_ts.tzinfo is not None else now_ts.tz_localize("UTC")
    out: dict[str, Any] = {
        "path": str(recent_path),
        "state_path": str(state_path),
        "exists": recent_path.exists(),
        "recorder_state_exists": state_path.exists(),
        "status": "missing",
        "reason": "recent_cache_missing",
        "row_count": 0,
        "market_count": 0,
        "token_count": 0,
        "latest_captured_ts_ms": None,
        "latest_captured_ts": None,
        "age_ms": None,
        "stale_after_ms": int(stale_after_ms),
        "provider": None,
        "recent_window_minutes": None,
    }
    state_payload = read_json_path(state_path)
    if isinstance(state_payload, dict):
        out["provider"] = state_payload.get("provider")
        out["recent_window_minutes"] = ((state_payload.get("last_summary") or {}).get("recent_window_minutes"))

    recent_df = read_parquet_if_exists(recent_path)
    if recent_df is None:
        return out
    if recent_df.empty:
        out["status"] = "empty"
        out["reason"] = "recent_cache_empty"
        return out

    recent_df = recent_df.copy()
    recent_df["captured_ts_ms"] = pd.to_numeric(recent_df["captured_ts_ms"], errors="coerce")
    recent_df = recent_df.dropna(subset=["captured_ts_ms"])
    if recent_df.empty:
        out["status"] = "empty"
        out["reason"] = "recent_cache_empty"
        return out

    recent_df["captured_ts_ms"] = recent_df["captured_ts_ms"].astype("int64")
    latest_captured_ts_ms = int(recent_df["captured_ts_ms"].max())
    latest_captured_ts = pd.Timestamp(latest_captured_ts_ms, unit="ms", tz="UTC")
    age_ms = int(now_ts.timestamp() * 1000) - latest_captured_ts_ms
    out.update(
        {
            "row_count": int(len(recent_df)),
            "market_count": int(recent_df["market_id"].astype(str).nunique()) if "market_id" in recent_df.columns else 0,
            "token_count": int(recent_df["token_id"].astype(str).nunique()) if "token_id" in recent_df.columns else 0,
            "latest_captured_ts_ms": latest_captured_ts_ms,
            "latest_captured_ts": latest_captured_ts.isoformat(),
            "age_ms": age_ms,
        }
    )
    if age_ms > int(stale_after_ms):
        out["status"] = "stale"
        out["reason"] = "recent_cache_stale"
    else:
        out["status"] = "ok"
        out["reason"] = None
    return out
