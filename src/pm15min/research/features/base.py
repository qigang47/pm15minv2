from __future__ import annotations

import numpy as np
import pandas as pd


_REQUIRED_COLUMNS = (
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "quote_asset_volume",
    "taker_buy_quote_volume",
    "number_of_trades",
)


def prepare_klines(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        raise ValueError("kline dataset is empty")
    if "open_time" not in raw.columns:
        raise ValueError("kline dataset must contain open_time")

    frame = raw.copy()
    frame["open_time"] = pd.to_datetime(frame["open_time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["open_time"]).sort_values("open_time")
    frame = frame.drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    if frame.empty:
        raise ValueError("kline dataset has no valid open_time rows")

    if "close" not in frame.columns:
        raise ValueError("kline dataset must contain close")

    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    frame = frame.dropna(subset=["close"]).copy()
    if frame.empty:
        raise ValueError("kline dataset has no valid close rows")

    for column in _REQUIRED_COLUMNS:
        if column not in frame.columns:
            if column in {"open", "high", "low"}:
                frame[column] = frame["close"]
            else:
                frame[column] = 0.0
        if column != "open_time":
            frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["open"] = frame["open"].fillna(frame["close"])
    frame["high"] = frame["high"].fillna(frame["close"])
    frame["low"] = frame["low"].fillna(frame["close"])
    for column in ("volume", "quote_asset_volume", "taker_buy_quote_volume", "number_of_trades"):
        frame[column] = frame[column].fillna(0.0)
    return frame.reset_index(drop=True)


def decision_reference_ts(frame: pd.DataFrame) -> pd.Series:
    decision_ts = pd.to_datetime(frame.get("decision_ts"), utc=True, errors="coerce")
    if isinstance(decision_ts, pd.Series) and bool(decision_ts.notna().any()):
        return decision_ts
    open_time = pd.to_datetime(frame.get("open_time"), utc=True, errors="coerce")
    return open_time + pd.Timedelta(minutes=1)


def compute_log_returns(close: pd.Series, window: int) -> pd.Series:
    return np.log(close / close.shift(int(window)))


def ema(series: pd.Series, span: int) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").ewm(span=int(span), adjust=False).mean()


def realized_volatility(close: pd.Series, window: int = 30) -> pd.Series:
    return compute_log_returns(close, 1).rolling(int(window)).std(ddof=0)


def rolling_zscore(series: pd.Series, window: int = 100) -> pd.Series:
    mean = series.rolling(int(window)).mean()
    std = series.rolling(int(window)).std(ddof=0)
    return (series - mean) / std.replace(0.0, np.nan)


def relative_strength_index(close: pd.Series, window: int = 14) -> pd.Series:
    delta = pd.to_numeric(close, errors="coerce").diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.rolling(int(window)).mean()
    avg_loss = loss.rolling(int(window)).mean()
    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    return 100.0 - (100.0 / (1.0 + rs))


def normalize_feature_frame(frame: pd.DataFrame, *, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = np.nan
    return out[columns]
