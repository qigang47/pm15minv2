from __future__ import annotations

import pandas as pd


def build_reject_frame(rows: pd.DataFrame, *, available_offsets: list[int]) -> pd.DataFrame:
    frame = rows.copy()
    if frame.empty:
        return pd.DataFrame(columns=["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "reason", "decision_source"])

    fill_valid = _bool_series(frame, "fill_valid")
    resolved = _bool_series(frame, "resolved")
    winner_side = _string_series(frame, "winner_side").str.upper()
    score_valid = _bool_series(frame, "score_valid")
    score_reason = _string_series(frame, "score_reason")
    fill_reason = _string_series(frame, "fill_reason")
    decision_source = _string_series(frame, "decision_source")

    tradeable = fill_valid & resolved & winner_side.isin(["UP", "DOWN"])
    rejected = frame.loc[~tradeable].copy()
    if rejected.empty:
        return pd.DataFrame(columns=["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "reason", "decision_source"])

    reasons = pd.Series("", index=rejected.index, dtype="string")
    offset_values = pd.to_numeric(rejected.get("offset"), errors="coerce")
    reasons.loc[~offset_values.isin([int(value) for value in available_offsets])] = "bundle_offset_missing"
    reasons.loc[reasons.eq("") & ~resolved.loc[rejected.index]] = "unresolved_label"
    reasons.loc[reasons.eq("") & ~score_valid.loc[rejected.index]] = score_reason.loc[rejected.index].replace("", "score_invalid")
    reasons.loc[reasons.eq("") & ~fill_valid.loc[rejected.index]] = fill_reason.loc[rejected.index].replace("", "fill_invalid")
    reasons.loc[reasons.eq("")] = "not_traded"
    rejected["reason"] = reasons.astype(str)
    rejected["decision_source"] = decision_source.loc[rejected.index]
    return rejected[["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "reason", "decision_source"]].reset_index(drop=True)


def summarize_reject_reasons(rejects: pd.DataFrame) -> dict[str, int]:
    if rejects.empty or "reason" not in rejects.columns:
        return {}
    counts = rejects["reason"].astype("string").fillna("").value_counts().sort_index()
    counts = counts[counts.index != ""]
    return {str(key): int(value) for key, value in counts.items()}


def _bool_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(False, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(False).astype(bool)


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)
