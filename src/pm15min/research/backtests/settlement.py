from __future__ import annotations

import numpy as np
import pandas as pd


def settle_fill_frame(fills: pd.DataFrame) -> pd.DataFrame:
    if fills.empty:
        return pd.DataFrame(columns=["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "market_id", "condition_id", "predicted_side", "predicted_prob", "entry_price", "entry_price_source", "price_cap", "stake", "shares", "winner_side", "win", "fee_rate", "fee_paid", "payout", "pnl", "roi_pct", "fill_model", "decision_source"])
    trades = fills.copy()
    trades["winner_side"] = _string_series(trades, "winner_side").str.upper()
    trades["predicted_side"] = _string_series(trades, "predicted_side").str.upper()
    trades["win"] = trades["predicted_side"] == trades["winner_side"]
    trades["stake"] = pd.to_numeric(trades.get("stake"), errors="coerce").fillna(0.0)
    trades["shares"] = pd.to_numeric(trades.get("shares"), errors="coerce").fillna(0.0)
    trades["fee_rate"] = pd.to_numeric(trades.get("fee_rate"), errors="coerce").fillna(0.0)
    trades["fee_paid"] = pd.to_numeric(trades.get("fee_paid"), errors="coerce").fillna(trades["stake"] * trades["fee_rate"])
    gross_payout = np.where(trades["win"], trades["shares"], 0.0)
    trades["payout"] = gross_payout - np.where(trades["win"], trades["fee_paid"], 0.0)
    trades["pnl"] = trades["payout"] - trades["stake"]
    trades["roi_pct"] = np.where(trades["stake"].gt(0.0), trades["pnl"] / trades["stake"] * 100.0, 0.0)
    keep = [
        "decision_ts",
        "cycle_start_ts",
        "cycle_end_ts",
        "offset",
        "market_id",
        "condition_id",
        "decision_source",
        "predicted_side",
        "predicted_prob",
        "winner_side",
        "entry_price",
        "entry_price_source",
        "price_cap",
        "stake",
        "shares",
        "win",
        "fee_rate",
        "fee_paid",
        "payout",
        "pnl",
        "roi_pct",
        "fill_model",
    ]
    optional = [
        column
        for column in (
            "policy_reason",
            "primary_reason",
            "secondary_reason",
            "model_source",
            "stake_base",
            "stake_multiplier",
            "stake_regime_state",
            "regime_state",
            "regime_pressure",
            "depth_status",
            "depth_reason",
            "depth_fill_ratio",
            "depth_source_path",
            "depth_avg_price",
            "depth_best_price",
            "depth_max_price",
        )
        if column in trades.columns
    ]
    return trades.reindex(columns=[*keep, *optional]).reset_index(drop=True)


def settle_trade_fills(fills: pd.DataFrame) -> pd.DataFrame:
    return settle_fill_frame(fills)


def build_equity_curve(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["decision_ts", "trade_number", "cumulative_pnl", "cumulative_stake", "cumulative_trades", "cumulative_roi_pct"])
    ordered = trades.sort_values(["decision_ts", "offset"]).reset_index(drop=True)
    ordered["trade_number"] = np.arange(1, len(ordered) + 1)
    ordered["cumulative_pnl"] = pd.to_numeric(ordered["pnl"], errors="coerce").fillna(0.0).cumsum()
    ordered["cumulative_stake"] = pd.to_numeric(ordered["stake"], errors="coerce").fillna(0.0).cumsum()
    ordered["cumulative_trades"] = ordered["trade_number"]
    ordered["cumulative_roi_pct"] = np.where(
        ordered["cumulative_stake"].gt(0.0),
        ordered["cumulative_pnl"] / ordered["cumulative_stake"] * 100.0,
        0.0,
    )
    return ordered[["decision_ts", "trade_number", "cumulative_pnl", "cumulative_stake", "cumulative_trades", "cumulative_roi_pct"]]


def build_market_summary(trades: pd.DataFrame) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["cycle_start_ts", "cycle_end_ts", "market_id", "condition_id", "trades", "wins", "pnl_sum", "stake_sum", "avg_roi_pct"])
    grouped = (
        trades.groupby(["cycle_start_ts", "cycle_end_ts", "market_id", "condition_id"], dropna=False)
        .agg(
            trades=("predicted_side", "size"),
            wins=("win", "sum"),
            pnl_sum=("pnl", "sum"),
            stake_sum=("stake", "sum"),
            avg_roi_pct=("roi_pct", "mean"),
        )
        .reset_index()
        .sort_values(["cycle_start_ts", "market_id"])
        .reset_index(drop=True)
    )
    return grouped


def settlement_summary(trades: pd.DataFrame) -> dict[str, float | int]:
    if trades.empty:
        return {"trades": 0, "wins": 0, "losses": 0, "pnl_sum": 0.0, "stake_sum": 0.0, "roi_pct": 0.0}
    pnl_sum = float(_numeric_series(trades, "pnl").fillna(0.0).sum())
    stake_sum = float(_numeric_series(trades, "stake").fillna(0.0).sum())
    wins = int(_bool_series(trades, "win").sum())
    return {
        "trades": int(len(trades)),
        "wins": wins,
        "losses": int(len(trades) - wins),
        "pnl_sum": pnl_sum,
        "stake_sum": stake_sum,
        "roi_pct": float((pnl_sum / stake_sum) * 100.0) if stake_sum else 0.0,
    }


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(values, errors="coerce")


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)
