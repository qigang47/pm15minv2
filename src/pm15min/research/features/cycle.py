from __future__ import annotations

import numpy as np
import pandas as pd

from pm15min.core.cycle_contracts import resolve_cycle_contract
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


def append_cycle_features(
    frame: pd.DataFrame,
    *,
    cycle: str = "15m",
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    out = frame.copy()
    requested = None if requested_columns is None else {str(column) for column in requested_columns}

    def needs(*columns: str) -> bool:
        if requested is None:
            return True
        return any(str(column) in requested for column in columns)

    decision_ts = decision_reference_ts(out)
    freq = _pandas_cycle_freq(cycle)
    contract = resolve_cycle_contract(cycle)
    anchor_offset = contract.first_half_anchor_offset
    cycle_start = decision_ts.dt.floor(freq)
    cycle_end = cycle_start + pd.Timedelta(freq)
    offset_seconds = (decision_ts - cycle_start).dt.total_seconds()
    minute_in_cycle = pd.Series(pd.NA, index=out.index, dtype="Int64")
    valid_cycle = decision_ts.notna() & cycle_start.notna() & offset_seconds.notna()
    if bool(valid_cycle.any()):
        minute_in_cycle.loc[valid_cycle] = (offset_seconds.loc[valid_cycle] // 60).astype("int64")
    out["cycle_start_ts"] = cycle_start
    out["cycle_end_ts"] = cycle_end
    out["offset"] = minute_in_cycle

    if not needs(
        "ret_from_cycle_open",
        "pullback_from_cycle_high",
        "rebound_from_cycle_low",
        "cycle_range_pos",
        "cycle_range_vs_rv",
        "move_z",
        "first_half_ret",
        "second_half_ret_proxy",
    ):
        return out

    close = pd.to_numeric(out["close"], errors="coerce")
    cycle_open = close.groupby(cycle_start).transform("first")
    if needs("ret_from_cycle_open", "move_z", "first_half_ret", "second_half_ret_proxy"):
        out["ret_from_cycle_open"] = close / cycle_open - 1.0
    if needs("pullback_from_cycle_high", "cycle_range_pos", "cycle_range_vs_rv"):
        cycle_high = close.groupby(cycle_start).cummax()
    else:
        cycle_high = None
    if needs("rebound_from_cycle_low", "cycle_range_pos", "cycle_range_vs_rv"):
        cycle_low = close.groupby(cycle_start).cummin()
    else:
        cycle_low = None
    if needs("pullback_from_cycle_high") and cycle_high is not None:
        out["pullback_from_cycle_high"] = close / cycle_high - 1.0
    if needs("rebound_from_cycle_low") and cycle_low is not None:
        out["rebound_from_cycle_low"] = close / cycle_low - 1.0
    if needs("cycle_range_pos") and cycle_high is not None and cycle_low is not None:
        rng = (cycle_high - cycle_low).replace(0.0, np.nan)
        out["cycle_range_pos"] = (close - cycle_low) / rng
    if needs("cycle_range_vs_rv") and cycle_high is not None and cycle_low is not None:
        cycle_range_ret = cycle_high / cycle_low.replace(0.0, np.nan) - 1.0
        out["cycle_range_vs_rv"] = cycle_range_ret / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    if needs("move_z"):
        out["move_z"] = out["ret_from_cycle_open"] / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    if needs("first_half_ret", "second_half_ret_proxy"):
        first_half_close = close.where(minute_in_cycle.eq(anchor_offset)).groupby(cycle_start).ffill().fillna(close)
        if needs("first_half_ret"):
            out["first_half_ret"] = first_half_close / cycle_open - 1.0
        if needs("second_half_ret_proxy"):
            second_half_anchor = first_half_close.where(minute_in_cycle.ge(anchor_offset), close)
            out["second_half_ret_proxy"] = close / second_half_anchor - 1.0
    return out
