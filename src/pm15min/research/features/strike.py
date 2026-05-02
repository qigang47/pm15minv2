from __future__ import annotations

from math import erf, sqrt

import numpy as np
import pandas as pd

from pm15min.research.features.cycle import _pandas_cycle_freq


STRIKE_FEATURE_COLUMNS = frozenset(
    {
        "ret_from_strike",
        "basis_bp",
        "has_oracle_strike",
        "has_cl_strike",
        "move_z_strike",
        "strike_abs_z",
        "strike_flip_count_cycle",
        "q_bs_up_strike",
        "q_bs_up_strike_centered",
        "up_move_remaining_per_minute",
        "up_move_remaining_z_per_minute",
        "first_up_cross_offset",
        "minutes_since_first_up_cross",
        "up_hold_minutes",
    }
)


def _normal_cdf(values: pd.Series) -> pd.Series:
    arr = pd.to_numeric(values, errors="coerce").to_numpy(dtype=float, copy=False)
    out = np.full(arr.shape, np.nan, dtype=float)
    mask = np.isfinite(arr)
    if bool(mask.any()):
        out[mask] = [0.5 * (1.0 + erf(float(v) / sqrt(2.0))) for v in arr[mask]]
    return pd.Series(out, index=values.index, dtype=float)


def recompute_strike_features(
    frame: pd.DataFrame,
    *,
    price_to_beat: pd.Series | None = None,
    cycle: str = "15m",
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    strike = (
        pd.to_numeric(price_to_beat, errors="coerce")
        if price_to_beat is not None
        else pd.to_numeric(frame.get("price_to_beat"), errors="coerce")
    )
    return _append_strike_feature_columns(
        frame,
        strike=strike,
        cycle=cycle,
        requested_columns=requested_columns,
    )


def append_strike_features(
    frame: pd.DataFrame,
    *,
    oracle_prices: pd.DataFrame,
    cycle: str = "15m",
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    strike = pd.Series(np.nan, index=frame.index, dtype=float)
    if not oracle_prices.empty:
        oracle = oracle_prices.copy()
        oracle["cycle_start_ts"] = pd.to_numeric(oracle["cycle_start_ts"], errors="coerce")
        oracle["price_to_beat"] = pd.to_numeric(oracle["price_to_beat"], errors="coerce")
        oracle = oracle.dropna(subset=["cycle_start_ts", "price_to_beat"]).copy()
        if not oracle.empty:
            strike_map = oracle.drop_duplicates(subset=["cycle_start_ts"], keep="last").set_index("cycle_start_ts")[
                "price_to_beat"
            ]
            cycle_start_sec = (
                pd.to_datetime(frame["cycle_start_ts"], utc=True, errors="coerce").astype("int64") // 10**9
            ).astype("int64")
            strike = cycle_start_sec.map(strike_map).astype(float)
    return _append_strike_feature_columns(
        frame,
        strike=strike,
        cycle=cycle,
        requested_columns=requested_columns,
    )


def _append_strike_feature_columns(
    frame: pd.DataFrame,
    *,
    strike: pd.Series,
    cycle: str,
    requested_columns: set[str] | None = None,
) -> pd.DataFrame:
    out = frame.copy()
    requested = None if requested_columns is None else {str(column) for column in requested_columns}

    def needs(*columns: str) -> bool:
        if requested is None:
            return True
        return any(str(column) in requested for column in columns)

    if not needs(*sorted(STRIKE_FEATURE_COLUMNS)):
        return out

    freq = _pandas_cycle_freq(cycle)
    if needs("has_oracle_strike", "has_cl_strike"):
        out["has_oracle_strike"] = 0
    if needs("basis_bp"):
        out["basis_bp"] = 0.0

    ret_cycle = pd.to_numeric(out["ret_from_cycle_open"], errors="coerce")
    denom = (1.0 + ret_cycle).replace(0.0, np.nan)
    cycle_open_close = pd.to_numeric(out["close"], errors="coerce") / denom
    basis_ratio = (cycle_open_close / strike) - 1.0
    valid = strike.notna() & np.isfinite(strike) & (strike > 0) & basis_ratio.notna() & np.isfinite(basis_ratio)
    basis_ratio = basis_ratio.where(valid)

    if needs("has_oracle_strike", "has_cl_strike"):
        out["has_oracle_strike"] = valid.fillna(False).astype(int)
        if needs("has_cl_strike"):
            out["has_cl_strike"] = out["has_oracle_strike"].astype(int)
    if needs("basis_bp"):
        out["basis_bp"] = (basis_ratio.fillna(0.0) * 1e4).astype(float)
    if needs(
        "ret_from_strike",
        "move_z_strike",
        "strike_abs_z",
        "strike_flip_count_cycle",
        "q_bs_up_strike",
        "q_bs_up_strike_centered",
        "up_move_remaining_per_minute",
        "up_move_remaining_z_per_minute",
        "first_up_cross_offset",
        "minutes_since_first_up_cross",
        "up_hold_minutes",
    ):
        out["ret_from_strike"] = ((1.0 + ret_cycle) * (1.0 + basis_ratio.fillna(0.0)) - 1.0).astype(float)
    if needs("move_z_strike"):
        out["move_z_strike"] = out["ret_from_strike"] / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    if needs("strike_abs_z"):
        out["strike_abs_z"] = out["ret_from_strike"].abs() / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
    cycle_key = pd.to_datetime(out["cycle_start_ts"], utc=True, errors="coerce")
    entity_key = _strike_entity_key(out, strike=strike)
    if needs("strike_flip_count_cycle"):
        strike_side = pd.Series(np.nan, index=out.index, dtype=float)
        strike_ret = pd.to_numeric(out["ret_from_strike"], errors="coerce")
        strike_side.loc[strike_ret.gt(0.0)] = 1.0
        strike_side.loc[strike_ret.lt(0.0)] = -1.0
        active_side = strike_side.groupby([cycle_key, entity_key]).ffill()
        previous_side = active_side.groupby([cycle_key, entity_key]).shift(1)
        flipped = active_side.notna() & previous_side.notna() & active_side.ne(previous_side)
        out["strike_flip_count_cycle"] = flipped.astype(int).groupby([cycle_key, entity_key]).cumsum().astype(float)
    if needs(
        "up_move_remaining_per_minute",
        "up_move_remaining_z_per_minute",
        "first_up_cross_offset",
        "minutes_since_first_up_cross",
        "up_hold_minutes",
    ):
        decision_ts = pd.to_datetime(out["decision_ts"], utc=True, errors="coerce")
        if "cycle_end_ts" in out.columns:
            cycle_end = pd.to_datetime(out["cycle_end_ts"], utc=True, errors="coerce")
        else:
            cycle_end = decision_ts.dt.floor(_pandas_cycle_freq(cycle)) + pd.Timedelta(_pandas_cycle_freq(cycle))
        minutes_left = ((cycle_end - decision_ts).dt.total_seconds() / 60.0).clip(lower=0.0)
        strike_ret = pd.to_numeric(out["ret_from_strike"], errors="coerce")
        valid_strike = valid.fillna(False)
        up_gap = (-strike_ret).clip(lower=0.0).where(valid_strike)
        if needs("up_move_remaining_per_minute", "up_move_remaining_z_per_minute"):
            up_gap_per_minute = up_gap / minutes_left.replace(0.0, np.nan)
            if needs("up_move_remaining_per_minute"):
                out["up_move_remaining_per_minute"] = up_gap_per_minute.astype(float)
            if needs("up_move_remaining_z_per_minute"):
                out["up_move_remaining_z_per_minute"] = (
                    up_gap_per_minute / pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
                ).astype(float)
        positive_side = valid_strike & strike_ret.gt(0.0)
        minute_in_cycle = pd.to_numeric(out.get("offset"), errors="coerce")
        first_cross_offset = minute_in_cycle.where(positive_side)
        first_cross_offset = first_cross_offset.groupby([cycle_key, entity_key]).cummin()
        first_cross_offset = first_cross_offset.groupby([cycle_key, entity_key]).ffill()
        if needs("first_up_cross_offset"):
            out["first_up_cross_offset"] = first_cross_offset.astype(float)
        if needs("minutes_since_first_up_cross"):
            out["minutes_since_first_up_cross"] = (minute_in_cycle - first_cross_offset).where(first_cross_offset.notna()).astype(float)
        if needs("up_hold_minutes"):
            reset_counter = (~positive_side).astype(int).groupby([cycle_key, entity_key]).cumsum()
            hold_counts = positive_side.astype(int).groupby([cycle_key, entity_key, reset_counter]).cumsum().astype(float)
            out["up_hold_minutes"] = hold_counts.where(positive_side, 0.0)

    if needs("q_bs_up_strike", "q_bs_up_strike_centered"):
        decision_ts = pd.to_datetime(out["decision_ts"], utc=True, errors="coerce")
        if "cycle_end_ts" in out.columns:
            cycle_end = pd.to_datetime(out["cycle_end_ts"], utc=True, errors="coerce")
        else:
            cycle_end = decision_ts.dt.floor(freq) + pd.Timedelta(freq)
        minutes_left = ((cycle_end - decision_ts).dt.total_seconds() / 60.0).clip(lower=0.0)
        log_moneyness = pd.Series(np.nan, index=out.index, dtype=float)
        valid_ret = pd.to_numeric(out["ret_from_strike"], errors="coerce") > -1.0
        if bool(valid_ret.any()):
            log_moneyness.loc[valid_ret] = np.log1p(pd.to_numeric(out.loc[valid_ret, "ret_from_strike"], errors="coerce"))
        rv_30 = pd.to_numeric(out["rv_30"], errors="coerce").replace(0.0, np.nan)
        v_bs = rv_30 * np.sqrt(minutes_left)
        d_bs = pd.Series(np.nan, index=out.index, dtype=float)
        mask = log_moneyness.notna() & np.isfinite(log_moneyness) & v_bs.notna() & np.isfinite(v_bs) & (v_bs > 1e-12)
        if bool(mask.any()):
            d_bs.loc[mask] = ((log_moneyness.loc[mask] - 0.5 * np.square(v_bs.loc[mask])) / v_bs.loc[mask]).astype(float)
        q_bs = _normal_cdf(d_bs).clip(lower=0.0, upper=1.0)
        if needs("q_bs_up_strike"):
            out["q_bs_up_strike"] = q_bs.fillna(0.5).astype(float)
        if needs("q_bs_up_strike_centered"):
            out["q_bs_up_strike_centered"] = (q_bs.fillna(0.5) - 0.5).astype(float)
    return out


def _strike_entity_key(frame: pd.DataFrame, *, strike: pd.Series) -> pd.Series:
    market_id = (
        frame["market_id"].astype("string").fillna("").str.strip()
        if "market_id" in frame.columns
        else pd.Series("", index=frame.index, dtype="string")
    )
    condition_id = (
        frame["condition_id"].astype("string").fillna("").str.strip()
        if "condition_id" in frame.columns
        else pd.Series("", index=frame.index, dtype="string")
    )
    strike_key = pd.to_numeric(strike, errors="coerce").round(10).astype("string").fillna("")
    entity_key = market_id.where(market_id.ne(""), condition_id)
    return entity_key.where(entity_key.ne(""), strike_key)
