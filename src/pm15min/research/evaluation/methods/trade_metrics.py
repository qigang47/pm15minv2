from __future__ import annotations

import pandas as pd

from pm15min.research.evaluation.methods.binary_metrics import brier_by_group
from pm15min.research.evaluation.methods.time_slices import add_time_bucket, time_slice_metrics


def summarize_trade_groups(
    trades: pd.DataFrame,
    *,
    group_col: str,
    prob_col: str = "predicted_prob",
    outcome_col: str = "win",
    pnl_col: str = "pnl",
) -> pd.DataFrame:
    """Summarize grouped trade outcomes using migrated binary scoring helpers."""

    columns = [group_col, "trades", "win_rate", "avg_pred", "pnl_sum"]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    grouped = brier_by_group(
        trades,
        prob_col=prob_col,
        outcome_col=outcome_col,
        group_cols=[group_col],
    ).rename(
        columns={
            "n": "trades",
            "empirical_rate": "win_rate",
        }
    )
    pnl = trades.groupby(group_col, dropna=False).agg(pnl_sum=(pnl_col, "sum")).reset_index()
    out = grouped.merge(pnl, on=group_col, how="left")
    out = out.sort_values(group_col, na_position="last").reset_index(drop=True)
    return out[[group_col, "trades", "win_rate", "avg_pred", "pnl_sum"]]


def summarize_trade_drift_slices(
    trades: pd.DataFrame,
    *,
    ts_col: str = "decision_ts",
    prob_col: str = "predicted_prob",
    outcome_col: str = "win",
    pnl_col: str = "pnl",
    slice: str = "day",
) -> pd.DataFrame:
    """Summarize time-sliced trade drift while preserving the existing runner artifact shape."""

    columns = ["date", "trades", "win_rate", "avg_pred", "pnl_sum", "cumulative_pnl"]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    frame = trades.copy()
    frame[ts_col] = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")
    frame, _ = add_time_bucket(frame, ts_col=ts_col, slice=slice, bucket_col="time_bucket")
    metrics = time_slice_metrics(
        frame,
        bucket_col="time_bucket",
        prob_col=prob_col,
        outcome_col=outcome_col,
        scope_name="all",
    )
    if metrics.empty:
        return pd.DataFrame(columns=columns)

    pnl = frame.groupby("time_bucket", dropna=False).agg(pnl_sum=(pnl_col, "sum")).reset_index()
    out = metrics.merge(pnl, on="time_bucket", how="left")
    out = out.sort_values("time_bucket", na_position="last").reset_index(drop=True)
    out["cumulative_pnl"] = out["pnl_sum"].fillna(0.0).cumsum()
    out = out.rename(
        columns={
            "time_label": "date",
            "n": "trades",
            "empirical_rate": "win_rate",
        }
    )
    return out[["date", "trades", "win_rate", "avg_pred", "pnl_sum", "cumulative_pnl"]]
