from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd

from ..config import DataConfig
from ..io.parquet import read_parquet_if_exists, upsert_parquet
from ..sources.binance_spot import (
    BINANCE_KLINE_COLUMNS,
    BinanceSpotKlinesClient,
    BinanceSpotKlinesRequest,
)


NUMERIC_COLUMNS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_buy_base_volume",
    "taker_buy_quote_volume",
    "ignore",
]


def _normalize_klines_frame(df: pd.DataFrame, *, now: datetime) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=BINANCE_KLINE_COLUMNS)
    out = df.copy()
    out = out.loc[:, [column for column in BINANCE_KLINE_COLUMNS if column in out.columns]].copy()
    out["open_time"] = pd.to_datetime(pd.to_numeric(out["open_time"], errors="coerce"), unit="ms", utc=True)
    out["close_time"] = pd.to_datetime(pd.to_numeric(out["close_time"], errors="coerce"), unit="ms", utc=True)
    for column in NUMERIC_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    if "number_of_trades" in out.columns:
        out["number_of_trades"] = pd.to_numeric(out["number_of_trades"], errors="coerce").fillna(0).astype("int64")
    out = out.dropna(subset=["open_time", "close_time"]).copy()
    out = out[out["close_time"] <= pd.Timestamp(now)]
    out = out.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    return out


def _max_open_time(df: pd.DataFrame) -> pd.Timestamp | None:
    if df.empty or "open_time" not in df.columns:
        return None
    ts = pd.to_datetime(df["open_time"], utc=True, errors="coerce").dropna()
    if ts.empty:
        return None
    return ts.max()


def sync_binance_klines_1m(
    cfg: DataConfig,
    *,
    symbol: str | None = None,
    client: BinanceSpotKlinesClient | None = None,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    lookback_minutes: int = 1440,
    now: datetime | None = None,
    batch_limit: int = 1000,
) -> dict[str, object]:
    resolved_symbol = str(symbol or cfg.asset.binance_symbol).strip().upper()
    if cfg.cycle != "15m":
        raise ValueError("Binance 1m sync currently assumes feature inputs for cycle=15m runtime.")

    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    client = client or BinanceSpotKlinesClient(timeout_sec=max(2.0, cfg.orderbook_timeout_sec * 4.0))
    target_path = cfg.layout.binance_klines_path(symbol=resolved_symbol)
    existing = read_parquet_if_exists(target_path)
    existing = existing if existing is not None else pd.DataFrame(columns=BINANCE_KLINE_COLUMNS)

    effective_end_ms = int(end_time_ms if end_time_ms is not None else now_utc.timestamp() * 1000)
    if start_time_ms is None:
        last_open_time = _max_open_time(existing)
        if last_open_time is None:
            start_dt = now_utc - timedelta(minutes=max(10, int(lookback_minutes)))
        else:
            start_dt = last_open_time + pd.Timedelta(minutes=1)
        effective_start_ms = int(start_dt.timestamp() * 1000)
    else:
        effective_start_ms = int(start_time_ms)

    fetched_batches: list[pd.DataFrame] = []
    rows_fetched = 0
    cursor_ms = effective_start_ms
    while cursor_ms <= effective_end_ms:
        batch = client.fetch_klines(
            BinanceSpotKlinesRequest(
                symbol=resolved_symbol,
                interval="1m",
                start_time_ms=cursor_ms,
                end_time_ms=effective_end_ms,
                limit=batch_limit,
            )
        )
        batch = _normalize_klines_frame(batch, now=now_utc)
        if batch.empty:
            break
        fetched_batches.append(batch)
        rows_fetched += int(len(batch))
        last_open_time = batch["open_time"].max()
        next_cursor_ms = int(last_open_time.timestamp() * 1000) + 60_000
        if next_cursor_ms <= cursor_ms:
            break
        cursor_ms = next_cursor_ms
        if len(batch) < int(batch_limit):
            break

    incoming = (
        pd.concat(fetched_batches, ignore_index=True, sort=False)
        if fetched_batches
        else pd.DataFrame(columns=BINANCE_KLINE_COLUMNS)
    )
    canonical = upsert_parquet(
        path=target_path,
        incoming=incoming,
        key_columns=["open_time"],
        sort_columns=["open_time", "close_time"],
    )
    latest_open_time = _max_open_time(canonical)
    return {
        "dataset": "binance_klines_1m",
        "market": cfg.asset.slug,
        "surface": cfg.surface,
        "symbol": resolved_symbol,
        "rows_fetched": int(rows_fetched),
        "rows_written": int(len(canonical)),
        "start_time_ms": int(effective_start_ms),
        "end_time_ms": int(effective_end_ms),
        "latest_open_time": None if latest_open_time is None else latest_open_time.isoformat(),
        "target_path": str(target_path),
    }
