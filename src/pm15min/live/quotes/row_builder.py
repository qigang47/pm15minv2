from __future__ import annotations

import os
from typing import Any

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.sources.orderbook_provider import OrderbookProvider
from .market import resolve_market_row
from .orderbook import (
    ORDERBOOK_INDEX_COLUMNS,
    build_orderbook_frame_from_provider,
    float_or_none,
    live_provider_future_skew_tolerance_ms,
    live_provider_orderbook_levels,
    load_latest_full_snapshot_cached,
    load_orderbook_index_frame_cached,
    resolve_orderbook_row,
    resolve_latest_full_snapshot_row,
    resolve_orderbook_row_within_window,
)


def _provider_only_orderbooks_enabled() -> bool:
    raw = os.getenv("PM15MIN_LIVE_ORDERBOOK_PROVIDER_ONLY")
    if raw in (None, ""):
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def build_offset_quote_row_impl(
    *,
    data_cfg: DataConfig,
    market_table: pd.DataFrame,
    signal_row: dict[str, Any],
    target: str,
    now: pd.Timestamp,
    orderbook_provider: OrderbookProvider | None = None,
    provider_frame_cache: dict[tuple[str, str, str], pd.DataFrame] | None = None,
    index_frame_cache: dict[tuple[str, str | None], pd.DataFrame] | None = None,
    latest_full_snapshot_cache: dict[str, dict[str, object] | None] | None = None,
) -> dict[str, Any]:
    decision_ts = pd.to_datetime(signal_row.get("decision_ts"), utc=True, errors="coerce")
    window_start_ts = pd.to_datetime(
        signal_row.get("window_start_ts") or signal_row.get("decision_ts"),
        utc=True,
        errors="coerce",
    )
    window_end_ts = pd.to_datetime(signal_row.get("window_end_ts"), utc=True, errors="coerce")
    if pd.isna(window_end_ts) and not pd.isna(window_start_ts):
        duration_seconds = pd.to_numeric(signal_row.get("window_duration_seconds"), errors="coerce")
        if pd.isna(duration_seconds) or float(duration_seconds) <= 0.0:
            duration_seconds = 60.0
        window_end_ts = window_start_ts + pd.to_timedelta(float(duration_seconds), unit="s")
    cycle_start_ts = pd.to_datetime(signal_row.get("cycle_start_ts"), utc=True, errors="coerce")
    signal_cycle_end_ts = pd.to_datetime(signal_row.get("cycle_end_ts"), utc=True, errors="coerce")
    date_str = decision_ts.strftime("%Y-%m-%d") if not pd.isna(decision_ts) else None
    out = {
        "offset": int(signal_row["offset"]),
        "decision_ts": None if pd.isna(decision_ts) else decision_ts.isoformat(),
        "cycle_start_ts": None if pd.isna(cycle_start_ts) else cycle_start_ts.isoformat(),
        "cycle_end_ts": None,
        "feature_cycle_start_ts": None if pd.isna(cycle_start_ts) else cycle_start_ts.isoformat(),
        "feature_cycle_end_ts": None if pd.isna(signal_cycle_end_ts) else signal_cycle_end_ts.isoformat(),
        "trade_cycle_start_ts": None,
        "status": "missing_quote_inputs",
        "market_id": None,
        "condition_id": None,
        "question": None,
        "token_up": None,
        "token_down": None,
        "quote_up_ask": None,
        "quote_down_ask": None,
        "quote_up_bid": None,
        "quote_down_bid": None,
        "quote_up_ask_size_1": None,
        "quote_down_ask_size_1": None,
        "quote_up_bid_size_1": None,
        "quote_down_bid_size_1": None,
        "quote_captured_ts_ms_up": None,
        "quote_captured_ts_ms_down": None,
        "quote_age_ms_up": None,
        "quote_age_ms_down": None,
        "quote_source_path": None,
        "reasons": [],
    }
    if not pd.isna(window_start_ts) and now < window_start_ts:
        out["reasons"].append("signal_window_not_open")
        return out
    if not pd.isna(window_end_ts) and now >= window_end_ts:
        out["reasons"].append("signal_window_expired")
        return out
    if market_table.empty:
        out["reasons"].append("market_catalog_missing")
        return out
    market_row = resolve_market_row(
        market_table,
        decision_ts=decision_ts,
        cycle_start_ts=cycle_start_ts,
        signal_cycle_end_ts=signal_cycle_end_ts,
        target=target,
        now=now,
    )
    if market_row is None:
        out["reasons"].append("market_row_missing")
        return out

    out["market_id"] = str(market_row.get("market_id") or "")
    out["condition_id"] = str(market_row.get("condition_id") or "")
    out["question"] = str(market_row.get("question") or "")
    out["token_up"] = str(market_row.get("token_up") or "")
    out["token_down"] = str(market_row.get("token_down") or "")
    cycle_end_raw = market_row.get("cycle_end_ts")
    if isinstance(cycle_end_raw, str):
        cycle_end_ts = pd.to_datetime(cycle_end_raw, utc=True, errors="coerce")
    else:
        cycle_end_ts = pd.to_datetime(cycle_end_raw, utc=True, unit="s", errors="coerce")
    trade_cycle_start_raw = market_row.get("cycle_start_ts")
    if isinstance(trade_cycle_start_raw, str):
        trade_cycle_start_ts = pd.to_datetime(trade_cycle_start_raw, utc=True, errors="coerce")
    else:
        trade_cycle_start_ts = pd.to_datetime(trade_cycle_start_raw, utc=True, unit="s", errors="coerce")
    out["trade_cycle_start_ts"] = None if pd.isna(trade_cycle_start_ts) else trade_cycle_start_ts.isoformat()
    out["cycle_end_ts"] = None if pd.isna(cycle_end_ts) else cycle_end_ts.isoformat()
    if not date_str:
        out["reasons"].append("decision_ts_missing")
        return out
    decision_ts_ms = int(decision_ts.timestamp() * 1000) if not pd.isna(decision_ts) else None
    current_now_ts_ms = int(now.timestamp() * 1000)
    reference_ts_ms = current_now_ts_ms
    window_start_ts_ms = int(window_start_ts.timestamp() * 1000) if not pd.isna(window_start_ts) else decision_ts_ms
    window_end_ts_ms = int(window_end_ts.timestamp() * 1000) if not pd.isna(window_end_ts) else None
    if not pd.isna(trade_cycle_start_ts) and trade_cycle_start_ts > now:
        reference_ts_ms = None
    index_path = data_cfg.layout.orderbook_index_path(date_str)
    recent_path = data_cfg.layout.orderbook_recent_path
    latest_full_snapshot_path = data_cfg.layout.orderbook_latest_full_snapshot_path
    if (
        not latest_full_snapshot_path.exists()
        and not index_path.exists()
        and not recent_path.exists()
        and orderbook_provider is None
    ):
        out["reasons"].append("orderbook_index_missing")
        return out

    provider_key = (
        str(out["market_id"]),
        str(out["token_up"]),
        str(out["token_down"]),
    )
    provider_only = bool(orderbook_provider is not None and _provider_only_orderbooks_enabled())

    up_row = None
    down_row = None
    if orderbook_provider is not None:
        if provider_frame_cache is not None and provider_key in provider_frame_cache:
            index_df = provider_frame_cache[provider_key]
        else:
            provider_df = build_orderbook_frame_from_provider(
                provider=orderbook_provider,
                market_id=str(out["market_id"]),
                token_up=str(out["token_up"]),
                token_down=str(out["token_down"]),
                timeout_sec=data_cfg.orderbook_timeout_sec,
                levels=live_provider_orderbook_levels(),
            )
            if provider_frame_cache is not None:
                provider_frame_cache[provider_key] = provider_df
            index_df = provider_df
        if not index_df.empty:
            provider_latest_ts_ms = None
            if "captured_ts_ms" in index_df.columns:
                provider_ts_series = pd.to_numeric(index_df["captured_ts_ms"], errors="coerce").dropna()
                if not provider_ts_series.empty:
                    provider_latest_ts_ms = int(provider_ts_series.max())
            provider_reference_floor_ts_ms = current_now_ts_ms if reference_ts_ms is None else int(reference_ts_ms)
            provider_reference_ts_ms = provider_reference_floor_ts_ms
            if provider_latest_ts_ms is not None:
                future_skew_limit_ms = live_provider_future_skew_tolerance_ms()
                if provider_latest_ts_ms <= provider_reference_floor_ts_ms + future_skew_limit_ms:
                    provider_reference_ts_ms = max(provider_reference_floor_ts_ms, provider_latest_ts_ms)
            up_row = resolve_orderbook_row_within_window(
                index_df,
                market_id=out["market_id"],
                token_id=out["token_up"],
                side="up",
                reference_ts_ms=provider_reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            down_row = resolve_orderbook_row_within_window(
                index_df,
                market_id=out["market_id"],
                token_id=out["token_down"],
                side="down",
                reference_ts_ms=provider_reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            if up_row is not None and down_row is not None:
                out["quote_source_path"] = "provider"
                reference_ts_ms = provider_reference_ts_ms
    if not provider_only and (up_row is None or down_row is None):
        latest_snapshot = load_latest_full_snapshot_cached(
            snapshot_path=latest_full_snapshot_path,
            cache=latest_full_snapshot_cache,
        )
        if latest_snapshot is not None:
            up_row = resolve_latest_full_snapshot_row(
                latest_snapshot,
                market_id=out["market_id"],
                token_id=out["token_up"],
                side="up",
                reference_ts_ms=reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            down_row = resolve_latest_full_snapshot_row(
                latest_snapshot,
                market_id=out["market_id"],
                token_id=out["token_down"],
                side="down",
                reference_ts_ms=reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            if up_row is not None and down_row is not None:
                out["quote_source_path"] = str(latest_full_snapshot_path)
    if not provider_only and (up_row is None or down_row is None):
        local_index_df = load_orderbook_index_frame_cached(
            index_path=index_path,
            recent_path=recent_path,
            cache=index_frame_cache,
        )
        index_df = local_index_df
        if not index_df.empty:
            missing_cols = sorted(ORDERBOOK_INDEX_COLUMNS - set(index_df.columns))
            if missing_cols:
                out["reasons"].append(f"orderbook_index_missing_columns:{','.join(missing_cols)}")
                return out
            up_row = resolve_orderbook_row_within_window(
                index_df,
                market_id=out["market_id"],
                token_id=out["token_up"],
                side="up",
                reference_ts_ms=reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            down_row = resolve_orderbook_row_within_window(
                index_df,
                market_id=out["market_id"],
                token_id=out["token_down"],
                side="down",
                reference_ts_ms=reference_ts_ms,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
            )
            if out["quote_source_path"] is None and (up_row is not None or down_row is not None):
                out["quote_source_path"] = str(index_path)
    if up_row is None:
        out["reasons"].append("up_quote_missing")
    else:
        out["quote_up_ask"] = float_or_none(up_row.get("best_ask"))
        out["quote_up_bid"] = float_or_none(up_row.get("best_bid"))
        out["quote_up_ask_size_1"] = float_or_none(up_row.get("ask_size_1"))
        out["quote_up_bid_size_1"] = float_or_none(up_row.get("bid_size_1"))
        out["quote_captured_ts_ms_up"] = int(up_row["captured_ts_ms"])
        out["quote_age_ms_up"] = reference_ts_ms - int(up_row["captured_ts_ms"]) if reference_ts_ms is not None else None
    if down_row is None:
        out["reasons"].append("down_quote_missing")
    else:
        out["quote_down_ask"] = float_or_none(down_row.get("best_ask"))
        out["quote_down_bid"] = float_or_none(down_row.get("best_bid"))
        out["quote_down_ask_size_1"] = float_or_none(down_row.get("ask_size_1"))
        out["quote_down_bid_size_1"] = float_or_none(down_row.get("bid_size_1"))
        out["quote_captured_ts_ms_down"] = int(down_row["captured_ts_ms"])
        out["quote_age_ms_down"] = reference_ts_ms - int(down_row["captured_ts_ms"]) if reference_ts_ms is not None else None

    if not out["reasons"]:
        out["status"] = "ok"
    return out
