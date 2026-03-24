from __future__ import annotations

import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from pm15min.research.bundles.loader import read_bundle_config
from pm15min.research.training.calibration import (
    lcb_from_bins,
    load_reliability_bins,
    ucb_from_bins,
)


def _clip_probability(value: object) -> float:
    try:
        out = float(value)
    except Exception:
        return 0.5
    return max(0.0, min(1.0, out))


def _resolve_blend_reliability_bins(offset_dir: Path, *, w_lgb: float, w_lr: float) -> list[dict[str, float | int]]:
    calibration_dir = offset_dir / "calibration"
    candidates = [calibration_dir / "reliability_bins_blend.json"]
    if abs(float(w_lgb) - 0.5) > 1e-9 or abs(float(w_lr) - 0.5) > 1e-9:
        candidates.insert(0, calibration_dir / "reliability_bins_blend_weighted.json")
    for path in candidates:
        if not path.exists():
            continue
        try:
            bins = load_reliability_bins(path)
        except Exception:
            continue
        if bins:
            return bins
    return []


def _direction_probability_views(
    p_signal: np.ndarray,
    *,
    reliability_bins: list[dict[str, float | int]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    p_up_raw = np.asarray([_clip_probability(value) for value in p_signal], dtype=float)
    p_down_raw = 1.0 - p_up_raw
    if not reliability_bins:
        return p_up_raw, p_down_raw, p_up_raw, p_down_raw
    p_up_eff = np.asarray([lcb_from_bins(value, reliability_bins) for value in p_up_raw], dtype=float)
    p_down_eff = np.asarray([1.0 - ucb_from_bins(value, reliability_bins) for value in p_up_raw], dtype=float)
    return p_up_raw, p_down_raw, p_up_eff, p_down_eff


def _reversal_probability_views(
    rows: pd.DataFrame,
    p_signal: np.ndarray,
    *,
    reliability_bins: list[dict[str, float | int]],
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, pd.Series, pd.Series]:
    current_ret = None
    for candidate in ("ret_from_strike", "ret_from_cycle_open"):
        if candidate in rows.columns:
            current_ret = pd.to_numeric(rows[candidate], errors="coerce")
            break
    if current_ret is None:
        valid = pd.Series(False, index=rows.index, dtype=bool)
        reason = pd.Series("missing_reversal_anchor", index=rows.index, dtype=object)
        neutral = np.full(len(rows), 0.5, dtype=float)
        return neutral, neutral, neutral, neutral, valid, reason

    curr_up = pd.Series(np.nan, index=rows.index, dtype=float)
    curr_up.loc[current_ret > 0.0] = 1.0
    curr_up.loc[current_ret < 0.0] = 0.0
    valid = curr_up.notna()
    reason = pd.Series("", index=rows.index, dtype=object)
    reason.loc[~valid] = "missing_reversal_anchor"

    p_reversal_raw = np.asarray([_clip_probability(value) for value in p_signal], dtype=float)
    if reliability_bins:
        p_reversal_lcb = np.asarray([lcb_from_bins(value, reliability_bins) for value in p_reversal_raw], dtype=float)
        p_reversal_ucb = np.asarray([ucb_from_bins(value, reliability_bins) for value in p_reversal_raw], dtype=float)
    else:
        p_reversal_lcb = p_reversal_raw
        p_reversal_ucb = p_reversal_raw

    p_up_raw = np.full(len(rows), 0.5, dtype=float)
    p_down_raw = np.full(len(rows), 0.5, dtype=float)
    p_up_eff = np.full(len(rows), 0.5, dtype=float)
    p_down_eff = np.full(len(rows), 0.5, dtype=float)

    mask_up = curr_up == 1.0
    mask_down = curr_up == 0.0

    p_up_raw[mask_up.to_numpy()] = 1.0 - p_reversal_raw[mask_up.to_numpy()]
    p_down_raw[mask_up.to_numpy()] = p_reversal_raw[mask_up.to_numpy()]
    p_up_eff[mask_up.to_numpy()] = 1.0 - p_reversal_ucb[mask_up.to_numpy()]
    p_down_eff[mask_up.to_numpy()] = p_reversal_lcb[mask_up.to_numpy()]

    p_up_raw[mask_down.to_numpy()] = p_reversal_raw[mask_down.to_numpy()]
    p_down_raw[mask_down.to_numpy()] = 1.0 - p_reversal_raw[mask_down.to_numpy()]
    p_up_eff[mask_down.to_numpy()] = p_reversal_lcb[mask_down.to_numpy()]
    p_down_eff[mask_down.to_numpy()] = 1.0 - p_reversal_ucb[mask_down.to_numpy()]

    return p_up_raw, p_down_raw, p_up_eff, p_down_eff, valid, reason


def score_bundle_offset(bundle_dir: Path, features: pd.DataFrame, *, offset: int) -> pd.DataFrame:
    bundle_cfg = read_bundle_config(bundle_dir, offset=offset)
    offset_dir = bundle_dir / "offsets" / f"offset={int(offset)}"
    feature_columns = list(bundle_cfg.get("feature_columns") or [])
    if not feature_columns:
        raise ValueError(f"Bundle has no feature columns for offset={offset}: {offset_dir}")

    model_lgb = joblib.load(offset_dir / "models" / "lgbm_sigmoid.joblib")
    model_lr = joblib.load(offset_dir / "models" / "logreg_sigmoid.joblib")
    weights = json.loads((offset_dir / "calibration" / "blend_weights.json").read_text(encoding="utf-8"))
    w_lgb = float(weights.get("w_lgb", 0.5))
    w_lr = float(weights.get("w_lr", 0.5))
    total = w_lgb + w_lr
    if total <= 0:
        w_lgb, w_lr = 0.5, 0.5
    else:
        w_lgb, w_lr = w_lgb / total, w_lr / total
    reliability_bins = _resolve_blend_reliability_bins(offset_dir, w_lgb=w_lgb, w_lr=w_lr)

    rows = features.copy()
    rows = rows[pd.to_numeric(rows.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return pd.DataFrame(columns=["decision_ts", "offset", "p_lgb", "p_lr", "p_up", "p_down", "score_valid", "score_reason"])

    X = rows.copy()
    for column in feature_columns:
        if column not in X.columns:
            X[column] = 0.0
        X[column] = pd.to_numeric(X[column], errors="coerce")
    X = X[feature_columns].replace([np.inf, -np.inf], np.nan).fillna(float(bundle_cfg.get("missing_feature_fill_value", 0.0)))
    fill_value = float(bundle_cfg.get("missing_feature_fill_value", 0.0))
    for column in bundle_cfg.get("allowed_blacklist_columns") or []:
        if column in X.columns:
            X[column] = fill_value

    p_lgb = model_lgb.predict_proba(X)[:, 1].astype(float)
    p_lr = model_lr.predict_proba(X)[:, 1].astype(float)
    p_signal = w_lgb * p_lgb + w_lr * p_lr

    signal_target = str(bundle_cfg.get("signal_target") or "direction").strip().lower()
    if signal_target == "reversal":
        p_up_raw, p_down_raw, p_up, p_down, score_valid, score_reason = _reversal_probability_views(
            rows,
            p_signal,
            reliability_bins=reliability_bins,
        )
    else:
        p_up_raw, p_down_raw, p_up, p_down = _direction_probability_views(
            p_signal,
            reliability_bins=reliability_bins,
        )
        score_valid = pd.Series(True, index=rows.index, dtype=bool)
        score_reason = pd.Series("", index=rows.index, dtype=object)

    return pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(rows["decision_ts"], utc=True, errors="coerce"),
            "cycle_start_ts": pd.to_datetime(rows["cycle_start_ts"], utc=True, errors="coerce"),
            "cycle_end_ts": pd.to_datetime(rows["cycle_end_ts"], utc=True, errors="coerce"),
            "offset": rows["offset"].astype(int),
            "p_lgb": p_lgb,
            "p_lr": p_lr,
            "p_signal": p_signal,
            "p_up_raw": p_up_raw,
            "p_down_raw": p_down_raw,
            "p_eff_up": p_up,
            "p_eff_down": p_down,
            "p_up": p_up,
            "p_down": p_down,
            "probability_mode": "conservative_reliability_bin" if reliability_bins else "raw_blend",
            "score_valid": score_valid.values,
            "score_reason": score_reason.values,
        }
    )
