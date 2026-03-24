from __future__ import annotations

import pandas as pd

from pm15min.research.backtests.replay_loader import REPLAY_KEY_COLUMNS


def apply_hybrid_fallback(
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    *,
    fallback_reasons: list[str] | tuple[str, ...],
) -> pd.DataFrame:
    keys = list(REPLAY_KEY_COLUMNS)
    primary_rows = primary.copy().set_index(keys)
    secondary_rows = secondary.copy().set_index(keys)
    out = primary_rows.copy()

    for key, row in primary_rows.iterrows():
        primary_reason = str(row.get("policy_reason", row.get("reject_reason", "")))
        if str(row.get("policy_action", "reject")) == "trade" or primary_reason not in set(fallback_reasons):
            continue
        if key not in secondary_rows.index:
            continue
        secondary_row = secondary_rows.loc[key]
        if isinstance(secondary_row, pd.DataFrame):
            secondary_row = secondary_row.iloc[-1]
        if str(secondary_row.get("policy_action", "reject")) != "trade":
            continue
        chosen = secondary_row.copy()
        chosen["decision_source"] = str(secondary_row.get("decision_source", "secondary"))
        chosen["model_source"] = chosen["decision_source"]
        chosen["policy_action"] = "trade"
        chosen["policy_reason"] = "hybrid_fallback_trade"
        chosen["reject_reason"] = ""
        chosen["primary_reason"] = primary_reason
        chosen["secondary_reason"] = str(secondary_row.get("policy_reason", secondary_row.get("reject_reason", "")))
        out.loc[key] = chosen

    return out.reset_index()


def apply_hybrid_score_fallback(
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    *,
    fallback_reasons: tuple[str, ...],
) -> pd.DataFrame:
    keys = list(REPLAY_KEY_COLUMNS)
    primary_rows = primary.copy().set_index(keys)
    secondary_rows = secondary.copy().set_index(keys)
    out = primary_rows.copy()

    for key, row in primary_rows.iterrows():
        primary_reason = str(row.get("reject_reason", ""))
        if bool(row.get("trade_decision", False)) or primary_reason not in set(fallback_reasons):
            continue
        if key not in secondary_rows.index:
            continue
        secondary_row = secondary_rows.loc[key]
        if isinstance(secondary_row, pd.DataFrame):
            secondary_row = secondary_row.iloc[-1]
        if not bool(secondary_row.get("trade_decision", False)):
            continue
        chosen = secondary_row.copy()
        chosen["model_source"] = "secondary"
        chosen["decision_source"] = "secondary"
        chosen["primary_reason"] = primary_reason
        chosen["secondary_reason"] = str(secondary_row.get("reject_reason", ""))
        out.loc[key] = chosen

    return out.reset_index()
