from __future__ import annotations

import numpy as np
import pandas as pd

from pm15min.research.features.base import decision_reference_ts


def _pandas_cycle_freq(cycle: str) -> str:
    text = str(cycle).strip().lower()
    if text.endswith("m") and text[:-1].isdigit():
        return f"{text[:-1]}min"
    return text


def append_decision_cycle_metadata(frame: pd.DataFrame, *, cycle: str = "15m") -> pd.DataFrame:
    out = frame.copy()
    decision_ts = decision_reference_ts(out)
    freq = _pandas_cycle_freq(cycle)
    cycle_start = decision_ts.dt.floor(freq)
    cycle_end = cycle_start + pd.Timedelta(freq)
    offset_seconds = (decision_ts - cycle_start).dt.total_seconds()
    offset = pd.Series(pd.NA, index=out.index, dtype="Int64")
    valid = decision_ts.notna() & cycle_start.notna() & offset_seconds.notna()
    if bool(valid.any()):
        offset.loc[valid] = (offset_seconds.loc[valid] // 60).astype("int64")
    out["cycle_start_ts"] = cycle_start
    out["cycle_end_ts"] = cycle_end
    out["offset"] = offset
    return out


def append_cycle_features(frame: pd.DataFrame, *, cycle: str = "15m") -> pd.DataFrame:
    out = frame.copy()
    decision_ts = decision_reference_ts(out)
    freq = _pandas_cycle_freq(cycle)
    cycle_start = decision_ts.dt.floor(freq)
    cycle_end = cycle_start + pd.Timedelta(freq)
    offset_seconds = (decision_ts - cycle_start).dt.total_seconds()
    minute_in_cycle = pd.Series(pd.NA, index=out.index, dtype="Int64")
    valid_cycle = decision_ts.notna() & cycle_start.notna() & offset_seconds.notna()
    if bool(valid_cycle.any()):
        minute_in_cycle.loc[valid_cycle] = (offset_seconds.loc[valid_cycle] // 60).astype("int64")

    close = pd.to_numeric(out["close"], errors="coerce")
    cycle_open = close.groupby(cycle_start).transform("first")
    cycle_high = close.groupby(cycle_start).cummax()
    cycle_low = close.groupby(cycle_start).cummin()

    out["ret_from_cycle_open"] = close / cycle_open - 1.0
    out["pullback_from_cycle_high"] = close / cycle_high - 1.0
    out["rebound_from_cycle_low"] = close / cycle_low - 1.0
    rng = (cycle_high - cycle_low).replace(0.0, np.nan)
    out["cycle_range_pos"] = (close - cycle_low) / rng
    out["move_z"] = out["ret_from_cycle_open"] / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    out["cycle_start_ts"] = cycle_start
    out["cycle_end_ts"] = cycle_end
    out["offset"] = minute_in_cycle
    first_half_close = close.where(minute_in_cycle.eq(7)).groupby(cycle_start).ffill().fillna(close)
    out["first_half_ret"] = first_half_close / cycle_open - 1.0
    second_half_anchor = first_half_close.where(minute_in_cycle.ge(7), close)
    out["second_half_ret_proxy"] = close / second_half_anchor - 1.0
    return out
