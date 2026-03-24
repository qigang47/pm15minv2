from __future__ import annotations

import numpy as np
import pandas as pd


def _pandas_cycle_freq(cycle: str) -> str:
    text = str(cycle).strip().lower()
    if text.endswith("m") and text[:-1].isdigit():
        return f"{text[:-1]}min"
    return text


def append_cycle_features(frame: pd.DataFrame, *, cycle: str = "15m") -> pd.DataFrame:
    out = frame.copy()
    ts = pd.DatetimeIndex(out["open_time"])
    freq = _pandas_cycle_freq(cycle)
    cycle_start = ts.floor(freq)
    minute_in_cycle = pd.Series(((ts - cycle_start).total_seconds() // 60).astype(int), index=out.index, dtype=int)

    cycle_open = out.groupby(cycle_start)["close"].transform("first")
    cycle_high = out.groupby(cycle_start)["close"].cummax()
    cycle_low = out.groupby(cycle_start)["close"].cummin()

    out["ret_from_cycle_open"] = out["close"] / cycle_open - 1.0
    out["pullback_from_cycle_high"] = out["close"] / cycle_high - 1.0
    out["rebound_from_cycle_low"] = out["close"] / cycle_low - 1.0
    rng = (cycle_high - cycle_low).replace(0.0, np.nan)
    out["cycle_range_pos"] = (out["close"] - cycle_low) / rng
    out["move_z"] = out["ret_from_cycle_open"] / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    out["cycle_start_ts"] = cycle_start
    out["cycle_end_ts"] = cycle_start + pd.Timedelta(freq)
    out["offset"] = ((ts - cycle_start).total_seconds() // 60).astype(int)
    first_half_close = out["close"].where(minute_in_cycle == 7).groupby(cycle_start).ffill().fillna(out["close"])
    out["first_half_ret"] = first_half_close / cycle_open - 1.0
    second_half_anchor = first_half_close.where(minute_in_cycle >= 7, out["close"])
    out["second_half_ret_proxy"] = out["close"] / second_half_anchor - 1.0
    return out
