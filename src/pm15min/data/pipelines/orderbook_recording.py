from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ..config import DataConfig
from ..contracts import OrderbookIndexRow, OrderbookSnapshotRecord
from ..io.ndjson_zst import append_ndjson_zst, iter_ndjson_zst
from ..io.json_files import write_json_atomic
from ..io.parquet import read_parquet_if_exists, upsert_parquet
from .market_catalog import sync_market_catalog
from .orderbook_recent import update_recent_orderbook_index
from ..sources.orderbook_provider import DirectOrderbookProvider, OrderbookProvider
from ..sources.polymarket_clob import PolymarketClobClient, normalize_book
from ..sources.polymarket_gamma import GammaEventsClient


DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS = 300.0
DEFAULT_LIVE_MARKET_CATALOG_NO_LIVE_RETRY_SECONDS = 60.0
DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS = 24
DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS = 24

MARKET_TABLE_REQUIRED = [
    "market_id",
    "cycle_start_ts",
    "cycle_end_ts",
    "token_up",
    "token_down",
]

ORDERBOOK_INDEX_COLUMNS = [
    "captured_ts_ms",
    "market_id",
    "token_id",
    "side",
    "best_ask",
    "best_bid",
    "ask_size_1",
    "bid_size_1",
    "spread",
]


def _day_str_from_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).strftime("%Y-%m-%d")


def _select_markets(
    df: pd.DataFrame,
    *,
    now_ts: int,
    market_depth: int,
    market_start_offset: int = 0,
) -> pd.DataFrame:
    active = df[(df["cycle_start_ts"] <= now_ts) & (df["cycle_end_ts"] > now_ts)].copy()
    future = df[df["cycle_start_ts"] > now_ts].copy().sort_values(["cycle_start_ts", "market_id"])
    ordered = pd.concat(
        [
            active.sort_values(["cycle_start_ts", "market_id"]),
            future,
        ],
        ignore_index=True,
    )
    offset = max(0, int(market_start_offset))
    if offset > 0:
        ordered = ordered.iloc[offset:].copy()
    selected = ordered.head(max(1, int(market_depth))).copy()
    return selected.reset_index(drop=True)


def _snapshot_record(
    *,
    captured_ts_ms: int,
    market_id: str,
    token_id: str,
    side: str,
    asset: str,
    cycle: str,
    book: dict,
) -> OrderbookSnapshotRecord:
    asks, bids, source_ts_ms = normalize_book(book)
    return OrderbookSnapshotRecord(
        captured_ts_ms=captured_ts_ms,
        source_ts_ms=source_ts_ms,
        market_id=market_id,
        token_id=token_id,
        side=side,
        asset=asset,
        cycle=cycle,
        asks=asks,
        bids=bids,
        source="clob",
    )


def _index_row_from_snapshot(record: OrderbookSnapshotRecord) -> OrderbookIndexRow:
    best_ask = record.asks[0]["price"] if record.asks else None
    best_bid = record.bids[0]["price"] if record.bids else None
    ask_size_1 = record.asks[0]["size"] if record.asks else None
    bid_size_1 = record.bids[0]["size"] if record.bids else None
    spread = None
    if best_ask is not None and best_bid is not None:
        spread = round(float(best_ask) - float(best_bid), 8)
    return OrderbookIndexRow(
        captured_ts_ms=record.captured_ts_ms,
        market_id=record.market_id,
        token_id=record.token_id,
        side=record.side,
        best_ask=best_ask,
        best_bid=best_bid,
        ask_size_1=ask_size_1,
        bid_size_1=bid_size_1,
        spread=spread,
    )


def _load_market_table(
    cfg: DataConfig,
    *,
    captured_ts_ms: int | None = None,
    gamma_client: GammaEventsClient | None = None,
) -> pd.DataFrame:
    target_path = cfg.layout.market_catalog_table_path
    captured_ts_ms = int(captured_ts_ms or time.time() * 1000)
    now = datetime.fromtimestamp(captured_ts_ms / 1000.0, tz=timezone.utc)
    now_ts = int(captured_ts_ms // 1000)
    df = read_parquet_if_exists(target_path)
    if cfg.surface == "live" and _live_market_catalog_refresh_reason(
        target_path=target_path,
        frame=df,
        now_ts=now_ts,
        now_utc=now,
    ):
        sync_market_catalog(
            cfg,
            start_ts=int((now - pd.Timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKBACK_HOURS)).timestamp()),
            end_ts=int((now + pd.Timedelta(hours=DEFAULT_LIVE_MARKET_CATALOG_LOOKAHEAD_HOURS)).timestamp()),
            client=gamma_client,
            now=now,
        )
        df = read_parquet_if_exists(target_path)
    if df is None or df.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {target_path}. "
            "Run `pm15min data sync market-catalog` first."
        )
    missing = [column for column in MARKET_TABLE_REQUIRED if column not in df.columns]
    if missing:
        raise KeyError(f"Market catalog missing required columns: {missing}")
    if cfg.surface == "live" and not _has_active_or_future_rows(df, now_ts=now_ts):
        raise RuntimeError(
            f"live market catalog has no active or future markets: {target_path}"
        )
    return df.copy()


def _live_market_catalog_refresh_reason(
    *,
    target_path,
    frame: pd.DataFrame | None,
    now_ts: int,
    now_utc: datetime,
) -> str | None:
    if frame is None or frame.empty:
        return "missing_or_empty"
    age_seconds = _path_age_seconds(target_path=target_path, now_utc=now_utc)
    max_age_seconds = _env_float(
        "PM15MIN_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS",
        default=DEFAULT_LIVE_MARKET_CATALOG_MAX_AGE_SECONDS,
    )
    if age_seconds is None or age_seconds > max(0.0, float(max_age_seconds)):
        return "stale"
    if _has_active_or_future_rows(frame, now_ts=now_ts):
        return None
    no_live_retry_seconds = _env_float(
        "PM15MIN_ORDERBOOK_MARKET_CATALOG_NO_LIVE_RETRY_SECONDS",
        default=DEFAULT_LIVE_MARKET_CATALOG_NO_LIVE_RETRY_SECONDS,
    )
    if age_seconds >= max(0.0, float(no_live_retry_seconds)):
        return "no_active_or_future_rows"
    return None


def _has_active_or_future_rows(frame: pd.DataFrame, *, now_ts: int) -> bool:
    if frame.empty:
        return False
    cycle_end_ts = pd.to_numeric(frame.get("cycle_end_ts"), errors="coerce")
    return bool(cycle_end_ts.gt(int(now_ts)).fillna(False).any())


def _path_age_seconds(*, target_path, now_utc: datetime) -> float | None:
    try:
        if not target_path.exists():
            return None
        return max(0.0, float(now_utc.timestamp() - target_path.stat().st_mtime))
    except Exception:
        return None


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _parse_ts_ms(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        out = int(value)
        return out if out > 0 else None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.isdigit():
            out = int(text)
            return out if out > 0 else None
        dt = pd.to_datetime(text, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _normalize_levels(levels: object, *, reverse: bool) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    if not isinstance(levels, list):
        return out
    for row in levels:
        price = None
        size = None
        if isinstance(row, dict):
            try:
                price = float(row.get("price"))
                size = float(row.get("size"))
            except Exception:
                continue
        elif isinstance(row, (list, tuple)) and len(row) >= 2:
            try:
                price = float(row[0])
                size = float(row[1])
            except Exception:
                continue
        if price is None or size is None:
            continue
        if price != price or size != size:
            continue
        out.append((price, size))
    out.sort(key=lambda item: item[0], reverse=reverse)
    return out


def _index_row_from_raw_depth(raw: dict[str, Any]) -> dict[str, object] | None:
    market_id = str(raw.get("market_id") or "").strip()
    token_id = str(raw.get("token_id") or "").strip()
    side = str(raw.get("side") or "").strip().lower()
    if not market_id or not token_id or side not in {"up", "down"}:
        return None
    captured_ts_ms = (
        _parse_ts_ms(raw.get("captured_ts_ms"))
        or _parse_ts_ms(raw.get("orderbook_ts"))
        or _parse_ts_ms(raw.get("logged_at"))
        or _parse_ts_ms(raw.get("source_ts_ms"))
    )
    if captured_ts_ms is None:
        return None

    asks = _normalize_levels(raw.get("asks"), reverse=False)
    bids = _normalize_levels(raw.get("bids"), reverse=True)
    best_ask = asks[0][0] if asks else None
    ask_size_1 = asks[0][1] if asks else None
    best_bid = bids[0][0] if bids else None
    bid_size_1 = bids[0][1] if bids else None
    spread = None
    if best_ask is not None and best_bid is not None:
        spread = round(float(best_ask) - float(best_bid), 8)

    return {
        "captured_ts_ms": int(captured_ts_ms),
        "market_id": market_id,
        "token_id": token_id,
        "side": side,
        "best_ask": best_ask,
        "best_bid": best_bid,
        "ask_size_1": ask_size_1,
        "bid_size_1": bid_size_1,
        "spread": spread,
    }


def record_orderbooks_once(
    cfg: DataConfig,
    *,
    client: PolymarketClobClient | None = None,
    provider: OrderbookProvider | None = None,
    gamma_client: GammaEventsClient | None = None,
    captured_ts_ms: int | None = None,
    recent_window_minutes: int = 15,
) -> dict[str, object]:
    provider = provider or DirectOrderbookProvider(client=client or PolymarketClobClient())
    captured_ts_ms = int(captured_ts_ms or time.time() * 1000)
    now_ts = captured_ts_ms // 1000
    date_str = _day_str_from_ts_ms(captured_ts_ms)

    markets = _load_market_table(
        cfg,
        captured_ts_ms=captured_ts_ms,
        gamma_client=gamma_client,
    )
    selected = _select_markets(
        markets,
        now_ts=now_ts,
        market_depth=cfg.market_depth,
        market_start_offset=cfg.market_start_offset,
    )
    selected_tokens = sorted(
        {
            str(token_id).strip()
            for row in selected.to_dict("records")
            for token_id in (row.get("token_up"), row.get("token_down"))
            if str(token_id or "").strip()
        }
    )
    try:
        provider.sync_subscriptions(
            selected_tokens,
            replace=True,
            prefetch=True,
            levels=0,
            timeout=cfg.orderbook_timeout_sec,
        )
    except Exception:
        pass

    snapshots: list[OrderbookSnapshotRecord] = []
    for row in selected.to_dict("records"):
        for side, token_col in (("up", "token_up"), ("down", "token_down")):
            token_id = str(row.get(token_col) or "").strip()
            if not token_id:
                continue
            book = provider.get_orderbook_summary(
                token_id,
                levels=0,
                timeout=cfg.orderbook_timeout_sec,
                force_refresh=False,
            )
            if not isinstance(book, dict):
                continue
            snapshots.append(
                _snapshot_record(
                    captured_ts_ms=captured_ts_ms,
                    market_id=str(row["market_id"]),
                    token_id=token_id,
                    side=side,
                    asset=cfg.asset.slug,
                    cycle=cfg.cycle,
                    book=book,
                )
            )

    append_ndjson_zst(
        cfg.layout.orderbook_depth_path(date_str),
        [snapshot.to_row() for snapshot in snapshots],
        level=7,
    )

    index_df = pd.DataFrame(
        [_index_row_from_snapshot(snapshot).to_row() for snapshot in snapshots],
        columns=ORDERBOOK_INDEX_COLUMNS,
    )
    upsert_parquet(
        path=cfg.layout.orderbook_index_path(date_str),
        incoming=index_df,
        key_columns=["captured_ts_ms", "market_id", "token_id", "side"],
        sort_columns=["captured_ts_ms", "market_id", "token_id", "side"],
        recover_existing_read_errors=True,
    )
    recent_df = update_recent_orderbook_index(
        path=cfg.layout.orderbook_recent_path,
        incoming=index_df,
        now_ts_ms=captured_ts_ms,
        window_minutes=recent_window_minutes,
    )
    latest_full_snapshot_payload = {
        "dataset": "orderbook_latest_full_snapshot",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "captured_ts_ms": int(captured_ts_ms),
        "selected_markets": int(len(selected)),
        "market_start_offset": int(cfg.market_start_offset),
        "selected_market_ids": selected["market_id"].astype(str).tolist(),
        "records": [snapshot.to_row() for snapshot in snapshots],
    }
    write_json_atomic(latest_full_snapshot_payload, cfg.layout.orderbook_latest_full_snapshot_path)

    return {
        "dataset": "orderbook_depth",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "captured_ts_ms": captured_ts_ms,
        "selected_markets": int(len(selected)),
        "market_start_offset": int(cfg.market_start_offset),
        "selected_market_ids": selected["market_id"].astype(str).tolist(),
        "snapshot_rows": int(len(snapshots)),
        "depth_path": str(cfg.layout.orderbook_depth_path(date_str)),
        "index_path": str(cfg.layout.orderbook_index_path(date_str)),
        "recent_path": str(cfg.layout.orderbook_recent_path),
        "latest_full_snapshot_path": str(cfg.layout.orderbook_latest_full_snapshot_path),
        "recent_rows": int(len(recent_df)),
        "recent_window_minutes": int(recent_window_minutes),
    }


def build_orderbook_index_from_depth(
    cfg: DataConfig,
    *,
    date_str: str,
) -> dict[str, object]:
    depth_path = cfg.layout.orderbook_depth_path(date_str)
    if not depth_path.exists():
        raise FileNotFoundError(f"Missing canonical orderbook depth source: {depth_path}")

    rows = []
    skipped_rows = 0
    for raw in iter_ndjson_zst(depth_path):
        row = _index_row_from_raw_depth(raw)
        if row is None:
            skipped_rows += 1
            continue
        rows.append(row)

    index_df = pd.DataFrame(rows, columns=ORDERBOOK_INDEX_COLUMNS)
    canonical = upsert_parquet(
        path=cfg.layout.orderbook_index_path(date_str),
        incoming=index_df,
        key_columns=["captured_ts_ms", "market_id", "token_id", "side"],
        sort_columns=["captured_ts_ms", "market_id", "token_id", "side"],
        recover_existing_read_errors=True,
    )
    return {
        "dataset": "orderbook_index",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "date": date_str,
        "source_depth_path": str(depth_path),
        "index_path": str(cfg.layout.orderbook_index_path(date_str)),
        "rows_written": int(len(canonical)),
        "rows_parsed": int(len(rows)),
        "rows_skipped": int(skipped_rows),
    }
