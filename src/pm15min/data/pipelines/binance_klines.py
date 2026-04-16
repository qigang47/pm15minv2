from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

import pandas as pd
import requests

from ..config import DataConfig
from ..io.json_files import write_json_atomic
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

_LATEST_TAIL_COLUMNS = [
    "open_time",
    "close_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_buy_quote_volume",
    "number_of_trades",
]

_COINBASE_PRODUCT_BY_SYMBOL = {
    "BTCUSDT": "BTC-USD",
    "ETHUSDT": "ETH-USD",
    "SOLUSDT": "SOL-USD",
    "XRPUSDT": "XRP-USD",
}
_COINBASE_MAX_BATCH_MINUTES = 300
_COINBASE_TIMEOUT_SEC = 30.0


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


def _empty_klines_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=BINANCE_KLINE_COLUMNS)


def _finalize_external_klines_frame(df: pd.DataFrame, *, now: datetime) -> pd.DataFrame:
    if df.empty:
        return _empty_klines_frame()
    out = df.copy()
    if "open_time" in out.columns:
        out["open_time"] = pd.to_datetime(out["open_time"], utc=True, errors="coerce")
    if "close_time" in out.columns:
        out["close_time"] = pd.to_datetime(out["close_time"], utc=True, errors="coerce")
    for column in NUMERIC_COLUMNS:
        if column in out.columns:
            out[column] = pd.to_numeric(out[column], errors="coerce")
    if "number_of_trades" in out.columns:
        out["number_of_trades"] = pd.to_numeric(out["number_of_trades"], errors="coerce").fillna(0).astype("int64")
    if "taker_buy_base_volume" not in out.columns:
        out["taker_buy_base_volume"] = 0.0
    if "taker_buy_quote_volume" not in out.columns:
        out["taker_buy_quote_volume"] = 0.0
    if "ignore" not in out.columns:
        out["ignore"] = 0.0
    if "quote_asset_volume" not in out.columns:
        out["quote_asset_volume"] = pd.to_numeric(out.get("close"), errors="coerce") * pd.to_numeric(
            out.get("volume"), errors="coerce"
        )
    out = out.loc[:, [column for column in BINANCE_KLINE_COLUMNS if column in out.columns]].copy()
    out = out.dropna(subset=["open_time", "close_time"]).copy()
    out = out[out["close_time"] <= pd.Timestamp(now)]
    out = out.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    return out


def _fetch_coinbase_klines_batch(
    *,
    symbol: str,
    start_time_ms: int,
    end_time_ms: int,
    timeout_sec: float,
) -> pd.DataFrame:
    product = _COINBASE_PRODUCT_BY_SYMBOL.get(str(symbol or "").strip().upper())
    if not product:
        return _empty_klines_frame()
    start_ts = pd.to_datetime(int(start_time_ms), unit="ms", utc=True)
    end_ts = pd.to_datetime(int(end_time_ms), unit="ms", utc=True)
    if end_ts < start_ts:
        return _empty_klines_frame()
    last_error: Exception | None = None
    response = None
    for _attempt in range(3):
        try:
            response = requests.Session().get(
                f"https://api.exchange.coinbase.com/products/{product}/candles",
                params={
                    "granularity": 60,
                    "start": start_ts.isoformat().replace("+00:00", "Z"),
                    "end": (end_ts + pd.Timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
                },
                timeout=max(_COINBASE_TIMEOUT_SEC, float(timeout_sec)),
            )
            response.raise_for_status()
            break
        except requests.RequestException as exc:
            last_error = exc
            response = None
    if response is None:
        assert last_error is not None
        raise last_error
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        return _empty_klines_frame()
    frame = pd.DataFrame(
        payload,
        columns=["open_time_sec", "low", "high", "open", "close", "volume"],
    )
    frame["open_time"] = pd.to_datetime(pd.to_numeric(frame["open_time_sec"], errors="coerce"), unit="s", utc=True)
    for column in ("open", "high", "low", "close", "volume"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["quote_asset_volume"] = frame["close"] * frame["volume"]
    frame["number_of_trades"] = 0
    frame["taker_buy_base_volume"] = 0.0
    frame["taker_buy_quote_volume"] = 0.0
    frame["close_time"] = frame["open_time"] + pd.Timedelta(minutes=1) - pd.Timedelta(milliseconds=1)
    frame["ignore"] = 0.0
    frame = frame.loc[
        frame["open_time"].ge(start_ts) & frame["open_time"].le(end_ts),
        [
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "number_of_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
    ].copy()
    return frame.sort_values("open_time").reset_index(drop=True)


def _latest_tail_path(cfg: DataConfig, *, symbol: str) -> Path:
    return (
        cfg.layout.surface_var_root
        / "state"
        / "binance_klines_1m"
        / f"symbol={str(symbol).strip().upper()}"
        / "latest_tail.json"
    )


def _env_int(name: str, *, default: int) -> int:
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _serialize_latest_tail_rows(frame: pd.DataFrame, *, max_rows: int) -> list[dict[str, object]]:
    if not isinstance(frame, pd.DataFrame) or frame.empty:
        return []
    rows = frame.tail(max(1, int(max_rows))).copy()
    payload_rows: list[dict[str, object]] = []
    for row in rows.to_dict("records"):
        payload: dict[str, object] = {}
        for column in _LATEST_TAIL_COLUMNS:
            value = row.get(column)
            if isinstance(value, pd.Timestamp):
                if value.tzinfo is None:
                    value = value.tz_localize("UTC")
                else:
                    value = value.tz_convert("UTC")
                payload[column] = value.isoformat()
            elif value is None or pd.isna(value):
                payload[column] = None
            elif column == "number_of_trades":
                payload[column] = int(value)
            else:
                payload[column] = float(value) if isinstance(value, (int, float)) else value
        payload_rows.append(payload)
    return payload_rows


def _write_latest_tail_marker(
    cfg: DataConfig,
    *,
    symbol: str,
    canonical: pd.DataFrame,
    now_utc: datetime,
) -> Path:
    latest_open_time = _max_open_time(canonical)
    tail_rows = _serialize_latest_tail_rows(
        canonical,
        max_rows=_env_int("PM15MIN_LIVE_BINANCE_TAIL_MARKER_ROWS", default=32),
    )
    return write_json_atomic(
        {
            "dataset": "binance_klines_1m_latest_tail",
            "market": cfg.asset.slug,
            "surface": cfg.surface,
            "symbol": str(symbol).strip().upper(),
            "snapshot_ts": pd.Timestamp(now_utc).isoformat(),
            "latest_open_time": None if latest_open_time is None else latest_open_time.isoformat(),
            "row_count": int(len(canonical)),
            "tail_rows": tail_rows,
        },
        _latest_tail_path(cfg, symbol=str(symbol).strip().upper()),
    )


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
    fallback_source: str | None = None
    while cursor_ms <= effective_end_ms:
        source_batch_limit = int(batch_limit)
        try:
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
        except Exception:
            source_batch_limit = min(int(batch_limit), _COINBASE_MAX_BATCH_MINUTES)
            fallback_end_ms = min(
                effective_end_ms,
                cursor_ms + ((source_batch_limit - 1) * 60_000),
            )
            batch = _finalize_external_klines_frame(
                _fetch_coinbase_klines_batch(
                    symbol=resolved_symbol,
                    start_time_ms=cursor_ms,
                    end_time_ms=fallback_end_ms,
                    timeout_sec=max(_COINBASE_TIMEOUT_SEC, float(getattr(client, "timeout_sec", 10.0))),
                ),
                now=now_utc,
            )
            if batch.empty:
                raise
            fallback_source = "coinbase"
        if batch.empty:
            break
        fetched_batches.append(batch)
        rows_fetched += int(len(batch))
        last_open_time = batch["open_time"].max()
        next_cursor_ms = int(last_open_time.timestamp() * 1000) + 60_000
        if next_cursor_ms <= cursor_ms:
            break
        cursor_ms = next_cursor_ms
        if len(batch) < source_batch_limit:
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
    latest_tail_path = _write_latest_tail_marker(
        cfg,
        symbol=resolved_symbol,
        canonical=canonical,
        now_utc=now_utc,
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
        "fallback_source": fallback_source,
        "target_path": str(target_path),
        "latest_tail_path": str(latest_tail_path),
    }
