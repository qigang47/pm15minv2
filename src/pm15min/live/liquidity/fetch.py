from __future__ import annotations

from typing import Any

import pandas as pd
import requests


SPOT_FALLBACK_BASES = (
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
)
PERP_FALLBACK_BASES = (
    "https://fapi.binance.com",
    "https://fapi1.binance.com",
    "https://fapi2.binance.com",
    "https://fapi3.binance.com",
)


def fetch_klines(base_url: str, path: str, symbol: str, limit: int, now: pd.Timestamp) -> pd.DataFrame:
    payload = request_json(
        base_url,
        path,
        {"symbol": symbol, "interval": "1m", "limit": int(limit)},
    )
    if not isinstance(payload, list) or not payload:
        raise RuntimeError(f"empty_kline_payload:{symbol}:{path}")
    rows: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, list) or len(row) < 9:
            continue
        try:
            rows.append(
                {
                    "open_time": pd.to_datetime(int(row[0]), unit="ms", utc=True),
                    "close_time": pd.to_datetime(int(row[6]), unit="ms", utc=True),
                    "quote_asset_volume": float(row[7]),
                    "number_of_trades": float(row[8]),
                }
            )
        except Exception:
            continue
    df = pd.DataFrame(rows).sort_values("open_time")
    if df.empty:
        raise RuntimeError(f"no_parsed_klines:{symbol}:{path}")
    closed = df[df["close_time"] < now]
    if closed.empty:
        raise RuntimeError(f"no_closed_kline:{symbol}:{path}")
    return closed


def fetch_book_ticker(base_url: str, path: str, symbol: str) -> tuple[float, float]:
    payload = request_json(base_url, path, {"symbol": symbol})
    if not isinstance(payload, dict):
        raise RuntimeError(f"book_ticker_invalid:{symbol}:{path}")
    bid = float(payload.get("bidPrice") or 0.0)
    ask = float(payload.get("askPrice") or 0.0)
    if bid <= 0 or ask <= 0:
        raise RuntimeError(f"book_ticker_non_positive:{symbol}:{path}")
    return bid, ask


def fetch_open_interest(base_url: str, symbol: str) -> float | None:
    payload = request_json(base_url, "/fapi/v1/openInterest", {"symbol": symbol})
    if not isinstance(payload, dict):
        return None
    try:
        return float(payload.get("openInterest") or 0.0)
    except Exception:
        return None


def request_json(base_url: str, path: str, params: dict[str, object]) -> object:
    timeout_seconds = 5.0
    max_retries = 2
    if "fapi" in base_url:
        candidates = [base_url] + [value for value in PERP_FALLBACK_BASES if value != base_url]
    else:
        candidates = [base_url] + [value for value in SPOT_FALLBACK_BASES if value != base_url]
    last_error: Exception | None = None
    with requests.Session() as session:
        for base in candidates:
            url = f"{base}{path}"
            for attempt in range(1, max_retries + 1):
                try:
                    response = session.get(url, params=params, timeout=timeout_seconds)
                    response.raise_for_status()
                    return response.json()
                except Exception as exc:
                    last_error = exc
                    if attempt < max_retries:
                        continue
    raise RuntimeError(f"request_failed:{path}:{params}:{last_error}")


def compute_ratio(
    series: pd.Series,
    *,
    lookback_minutes: int,
    baseline_minutes: int,
) -> tuple[float | None, float | None, float | None]:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty or len(values) < int(lookback_minutes) + 2:
        return None, None, None
    recent = values.tail(int(lookback_minutes))
    baseline_pool = values.iloc[:-int(lookback_minutes)]
    if baseline_pool.empty:
        return None, None, None
    baseline_pool = baseline_pool.tail(int(baseline_minutes))
    if baseline_pool.empty:
        return None, None, None
    recent_avg = float(recent.mean())
    baseline_median = float(baseline_pool.median())
    if baseline_median <= 0.0:
        return None, recent_avg, baseline_median
    return recent_avg / baseline_median, recent_avg, baseline_median


def window_sum(series: pd.Series, *, lookback_minutes: int) -> float | None:
    values = pd.to_numeric(series, errors="coerce").dropna()
    if values.empty or len(values) < int(lookback_minutes):
        return None
    return float(values.tail(int(lookback_minutes)).sum())


def spread_bps(bid: float, ask: float) -> tuple[float | None, float | None]:
    if bid <= 0 or ask <= 0:
        return None, None
    mid = 0.5 * (bid + ask)
    if mid <= 0:
        return None, None
    return ((ask - bid) / mid * 10000.0), mid


def normalize_now(value: object) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC")
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")
