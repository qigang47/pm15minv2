from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ..config import DataConfig
from ..contracts import OrderbookIndexRow, OrderbookSnapshotRecord
from ..io.ndjson_zst import append_ndjson_zst, iter_ndjson_zst
from ..io.parquet import read_parquet_if_exists, upsert_parquet
from .market_catalog import sync_market_catalog
from .orderbook_recent import update_recent_orderbook_index
from ..sources.orderbook_provider import DirectOrderbookProvider, OrderbookProvider
from ..sources.polymarket_clob import PolymarketClobClient, normalize_book
from ..sources.polymarket_gamma import GammaEventsClient


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


def _select_markets(df: pd.DataFrame, *, now_ts: int, market_depth: int) -> pd.DataFrame:
    active = df[(df["cycle_start_ts"] <= now_ts) & (df["cycle_end_ts"] > now_ts)].copy()
    future = df[df["cycle_start_ts"] > now_ts].copy().sort_values(["cycle_start_ts", "market_id"])

    if len(active) >= market_depth:
        selected = active.sort_values(["cycle_start_ts", "market_id"]).head(market_depth)
    else:
        needed = max(0, int(market_depth) - len(active))
        selected = pd.concat(
            [
                active.sort_values(["cycle_start_ts", "market_id"]),
                future.head(needed),
            ],
            ignore_index=True,
        )
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
    df = read_parquet_if_exists(cfg.layout.market_catalog_table_path)
    if (df is None or df.empty) and cfg.surface == "live":
        now = datetime.fromtimestamp((captured_ts_ms or int(time.time() * 1000)) / 1000.0, tz=timezone.utc)
        sync_market_catalog(
            cfg,
            start_ts=int((now - pd.Timedelta(hours=24)).timestamp()),
            end_ts=int((now + pd.Timedelta(hours=24)).timestamp()),
            client=gamma_client,
            now=now,
        )
        df = read_parquet_if_exists(cfg.layout.market_catalog_table_path)
    if df is None or df.empty:
        raise FileNotFoundError(
            f"Missing canonical market catalog: {cfg.layout.market_catalog_table_path}. "
            "Run `pm15min data sync market-catalog` first."
        )
    missing = [column for column in MARKET_TABLE_REQUIRED if column not in df.columns]
    if missing:
        raise KeyError(f"Market catalog missing required columns: {missing}")
    return df.copy()


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
    selected = _select_markets(markets, now_ts=now_ts, market_depth=cfg.market_depth)
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
    )
    recent_df = update_recent_orderbook_index(
        path=cfg.layout.orderbook_recent_path,
        incoming=index_df,
        now_ts_ms=captured_ts_ms,
        window_minutes=recent_window_minutes,
    )

    return {
        "dataset": "orderbook_depth",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "captured_ts_ms": captured_ts_ms,
        "selected_markets": int(len(selected)),
        "snapshot_rows": int(len(snapshots)),
        "depth_path": str(cfg.layout.orderbook_depth_path(date_str)),
        "index_path": str(cfg.layout.orderbook_index_path(date_str)),
        "recent_path": str(cfg.layout.orderbook_recent_path),
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
