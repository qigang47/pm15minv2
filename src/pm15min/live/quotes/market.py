from __future__ import annotations

import pandas as pd

from pm15min.data.config import DataConfig


def read_market_table(data_cfg: DataConfig) -> pd.DataFrame:
    path = data_cfg.layout.market_catalog_table_path
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return df
    for col in ("cycle_start_ts", "cycle_end_ts"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def resolve_market_row(
    market_table: pd.DataFrame,
    *,
    decision_ts: pd.Timestamp,
    cycle_start_ts: pd.Timestamp,
    signal_cycle_end_ts: pd.Timestamp,
    target: str,
    now: pd.Timestamp,
):
    table = market_table.copy()
    if (
        str(target or "").strip().lower() == "direction"
        and not pd.isna(signal_cycle_end_ts)
        and signal_cycle_end_ts <= now
        and "cycle_start_ts" in table.columns
    ):
        next_cycle_start_sec = int(signal_cycle_end_ts.timestamp())
        next_market = table[table["cycle_start_ts"] == next_cycle_start_sec]
        if not next_market.empty:
            return next_market.sort_values(["cycle_start_ts", "market_id"]).iloc[-1].to_dict()
    if not pd.isna(cycle_start_ts) and "cycle_start_ts" in table.columns:
        cycle_start_sec = int(cycle_start_ts.timestamp())
        exact = table[table["cycle_start_ts"] == cycle_start_sec]
        if not exact.empty:
            return exact.sort_values(["cycle_start_ts", "market_id"]).iloc[-1].to_dict()
    if not pd.isna(decision_ts) and {"cycle_start_ts", "cycle_end_ts"} <= set(table.columns):
        decision_sec = int(decision_ts.timestamp())
        active = table[(table["cycle_start_ts"] <= decision_sec) & (table["cycle_end_ts"] > decision_sec)]
        if not active.empty:
            return active.sort_values(["cycle_start_ts", "market_id"]).iloc[-1].to_dict()
    return None
