from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from pm15min.research.backtests.replay_loader import REPLAY_KEY_COLUMNS


@dataclass(frozen=True)
class BacktestPolicyConfig:
    prob_floor: float = 0.5
    prob_gap_floor: float = 0.0
    decision_source: str = "primary"


@dataclass(frozen=True)
class DecisionPolicyConfig:
    min_confidence: float = 0.5
    min_probability_gap: float = 0.0
    decision_source: str = "primary"


def apply_decision_policy(
    rows: pd.DataFrame,
    *,
    cfg: DecisionPolicyConfig | None = None,
) -> pd.DataFrame:
    config = cfg or DecisionPolicyConfig()
    return _apply_policy_frame(
        rows,
        min_confidence=config.min_confidence,
        min_probability_gap=config.min_probability_gap,
        decision_source=config.decision_source,
    )


def build_policy_decisions(
    replay: pd.DataFrame,
    *,
    config: BacktestPolicyConfig | None = None,
    model_source: str = "primary",
) -> pd.DataFrame:
    cfg = config or BacktestPolicyConfig()
    return _apply_policy_frame(
        replay,
        min_confidence=cfg.prob_floor,
        min_probability_gap=cfg.prob_gap_floor,
        decision_source=model_source or cfg.decision_source,
    )


def split_policy_decisions(decisions: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted = decisions.loc[decisions.get("policy_action", "reject").astype(str).eq("trade")].copy()
    rejected = build_policy_reject_frame(decisions)
    return accepted.reset_index(drop=True), rejected.reset_index(drop=True)


def build_policy_reject_frame(decisions: pd.DataFrame) -> pd.DataFrame:
    rejected = decisions.loc[decisions.get("policy_action", "reject").astype(str).ne("trade")].copy()
    if rejected.empty:
        return pd.DataFrame(columns=[*REPLAY_KEY_COLUMNS, "decision_source", "policy_reason", "reason"])
    frame = pd.DataFrame(index=rejected.index)
    for column in REPLAY_KEY_COLUMNS:
        if column in rejected.columns:
            frame[column] = rejected[column]
        else:
            frame[column] = pd.NA
    frame["decision_source"] = _decision_source_series(rejected)
    frame["policy_reason"] = rejected.get("policy_reason", rejected.get("reject_reason", "")).astype(str)
    frame["reason"] = frame["policy_reason"]
    for column in (
        "pre_submit_orderbook_retry_armed",
        "pre_submit_orderbook_retry_reason",
        "pre_submit_orderbook_retry_interval_sec",
        "pre_submit_orderbook_retry_max",
        "pre_submit_orderbook_retry_state_key",
    ):
        if column in rejected.columns:
            frame[column] = rejected[column]
    return frame.reset_index(drop=True)


def _apply_policy_frame(
    rows: pd.DataFrame,
    *,
    min_confidence: float,
    min_probability_gap: float,
    decision_source: str,
) -> pd.DataFrame:
    out = rows.copy()
    p_up = pd.to_numeric(out.get("p_up"), errors="coerce")
    p_down = pd.to_numeric(out.get("p_down"), errors="coerce")
    out["predicted_side"] = np.where(p_up >= p_down, "UP", "DOWN")
    out["predicted_prob"] = pd.concat([p_up, p_down], axis=1).max(axis=1)
    out["probability_gap"] = (p_up - p_down).abs()
    out["trade_decision"] = False
    out["reject_reason"] = ""
    out["decision_source"] = str(decision_source)
    out["model_source"] = out["decision_source"]

    offset_available = _bool_series(out, "bundle_offset_available", default=True)
    score_present = _bool_series(out, "score_present", default=True)
    resolved = _bool_series(out, "resolved", default=False)
    score_valid = _bool_series(out, "score_valid", default=False)
    winner_side = _string_series(out, "winner_side").str.upper()
    score_reason = _string_series(out, "score_reason")
    decision_engine_action = _string_series(out, "decision_engine_action").str.lower()
    decision_engine_reason = _string_series(out, "decision_engine_reason")
    decision_engine_side = _string_series(out, "decision_engine_side").str.upper()
    decision_engine_prob = _numeric_series(out, "decision_engine_prob")
    decision_engine_gap = _numeric_series(out, "decision_engine_probability_gap")

    decision_engine_trade = decision_engine_action.eq("trade") & decision_engine_side.isin(["UP", "DOWN"])
    out.loc[decision_engine_trade, "predicted_side"] = decision_engine_side.loc[decision_engine_trade]
    out.loc[decision_engine_trade & decision_engine_prob.notna(), "predicted_prob"] = decision_engine_prob.loc[
        decision_engine_trade & decision_engine_prob.notna()
    ]
    out.loc[decision_engine_trade & decision_engine_gap.notna(), "probability_gap"] = decision_engine_gap.loc[
        decision_engine_trade & decision_engine_gap.notna()
    ]

    out.loc[~offset_available, "reject_reason"] = "bundle_offset_missing"
    out.loc[out["reject_reason"].eq("") & ~score_present, "reject_reason"] = "score_missing"
    out.loc[out["reject_reason"].eq("") & ~resolved, "reject_reason"] = "unresolved_label"

    score_invalid = out["reject_reason"].eq("") & ~score_valid
    out.loc[score_invalid, "reject_reason"] = score_reason.loc[score_invalid].replace("", "score_invalid")

    out.loc[out["reject_reason"].eq("") & ~winner_side.isin(["UP", "DOWN"]), "reject_reason"] = "unsupported_winner_side"
    decision_engine_reject = out["reject_reason"].eq("") & decision_engine_action.eq("reject")
    out.loc[decision_engine_reject, "reject_reason"] = decision_engine_reason.loc[decision_engine_reject].replace(
        "",
        "decision_engine_reject",
    )
    out.loc[
        out["reject_reason"].eq("") & out["predicted_prob"].lt(float(min_confidence)),
        "reject_reason",
    ] = "policy_low_confidence"
    out.loc[
        out["reject_reason"].eq("") & out["probability_gap"].lt(float(min_probability_gap)),
        "reject_reason",
    ] = "policy_small_probability_gap"

    out.loc[out["reject_reason"].eq(""), "trade_decision"] = True
    out["policy_action"] = np.where(out["trade_decision"], "trade", "reject")
    out["policy_reason"] = np.where(out["trade_decision"], "trade", out["reject_reason"])
    return out


def _bool_series(frame: pd.DataFrame, column: str, *, default: bool) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(default, index=frame.index, dtype="boolean")
    return values.astype("boolean").fillna(default).astype(bool)


def _string_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series("", index=frame.index, dtype="string")
    return values.astype("string").fillna("").astype(str)


def _decision_source_series(frame: pd.DataFrame) -> pd.Series:
    if "decision_source" in frame.columns:
        return _string_series(frame, "decision_source")
    if "model_source" in frame.columns:
        return _string_series(frame, "model_source")
    return pd.Series("primary", index=frame.index, dtype="string").astype(str)


def _numeric_series(frame: pd.DataFrame, column: str) -> pd.Series:
    values = frame[column] if column in frame.columns else pd.Series(np.nan, index=frame.index, dtype=float)
    return pd.to_numeric(values, errors="coerce")
