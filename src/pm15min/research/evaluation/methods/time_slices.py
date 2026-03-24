from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd


def add_time_bucket(
    df: pd.DataFrame,
    *,
    ts_col: str,
    slice: Literal["day", "week"],
    bucket_col: str = "time_bucket",
) -> tuple[pd.DataFrame, str]:
    """Add a normalized UTC day or week bucket derived from a datetime column."""

    if ts_col not in df.columns:
        raise KeyError(f"Missing timestamp col: {ts_col}")

    out = df.copy()
    ts = out[ts_col]
    if not pd.api.types.is_datetime64_any_dtype(ts):
        raise TypeError(f"{ts_col} must be datetime64 dtype (got {ts.dtype})")

    if slice == "day":
        out[bucket_col] = ts.dt.floor("D")
        return out, f"{bucket_col} <- floor(day)"
    if slice == "week":
        day = ts.dt.floor("D")
        out[bucket_col] = day - pd.to_timedelta(day.dt.dayofweek, unit="D")
        return out, f"{bucket_col} <- week_start(Mon)"
    raise ValueError("slice must be one of: day, week")


def time_slice_metrics(
    df: pd.DataFrame,
    *,
    bucket_col: str,
    prob_col: str,
    outcome_col: str,
    scope_name: str,
) -> pd.DataFrame:
    """Compute per-bucket scoring diagnostics for a binary prediction series."""

    for column in [bucket_col, prob_col, outcome_col]:
        if column not in df.columns:
            raise KeyError(f"Missing column: {column}")

    rows: list[dict[str, object]] = []
    for bucket, group in df.groupby(bucket_col, dropna=False):
        probs = pd.to_numeric(group[prob_col], errors="coerce").to_numpy(dtype=float)
        obs = pd.to_numeric(group[outcome_col], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(probs) & np.isfinite(obs)
        if not mask.any():
            continue
        probs = probs[mask]
        obs = obs[mask]

        empirical_rate = float(np.mean(obs))
        baseline_brier = float(empirical_rate * (1.0 - empirical_rate))
        brier = float(np.mean((probs - obs) ** 2))
        avg_pred = float(np.mean(probs))
        rows.append(
            {
                "scope": scope_name,
                "time_bucket": bucket,
                "n": int(mask.sum()),
                "brier": brier,
                "baseline_brier": baseline_brier,
                "delta_vs_baseline": brier - baseline_brier,
                "avg_pred": avg_pred,
                "empirical_rate": empirical_rate,
                "bias": avg_pred - empirical_rate,
            }
        )

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out = out.sort_values(["time_bucket", "scope"]).reset_index(drop=True)
    if pd.api.types.is_datetime64_any_dtype(out["time_bucket"]):
        out["time_label"] = out["time_bucket"].dt.strftime("%Y-%m-%d")
    else:
        out["time_label"] = out["time_bucket"].astype(str)
    columns = ["scope", "time_bucket", "time_label"] + [
        column for column in out.columns if column not in {"scope", "time_bucket", "time_label"}
    ]
    return out[columns]
