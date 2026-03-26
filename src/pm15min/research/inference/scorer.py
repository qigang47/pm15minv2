from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
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


@dataclass(frozen=True)
class _OffsetScoringRuntime:
    bundle_cfg: dict[str, object]
    feature_columns: tuple[str, ...]
    allowed_blacklist_columns: tuple[str, ...]
    missing_feature_fill_value: float
    signal_target: str
    model_lgb: object
    model_lr: object
    w_lgb: float
    w_lr: float
    reliability_bins: tuple[dict[str, float | int], ...]


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


def load_offset_model_context(offset_dir: Path, *, top_n: int = 5) -> dict[str, object] | None:
    signature = _offset_model_context_signature(offset_dir)
    return _load_offset_model_context_cached(str(Path(offset_dir)), int(top_n), signature)


@lru_cache(maxsize=512)
def _load_offset_model_context_cached(
    offset_dir_str: str,
    top_n: int,
    signature: tuple[tuple[str, int | None], ...],
) -> dict[str, object] | None:
    del signature
    offset_dir = Path(offset_dir_str)
    diagnostics_dir = offset_dir / "diagnostics"

    logreg_payload = _read_optional_json(diagnostics_dir / "logreg_coefficients.json") if diagnostics_dir.exists() else None
    lgb_payload = _read_optional_json(diagnostics_dir / "lgb_feature_importance.json") if diagnostics_dir.exists() else None
    factor_payload = _read_optional_json(diagnostics_dir / "factor_direction_summary.json") if diagnostics_dir.exists() else None
    computed_context = None
    if logreg_payload is None or lgb_payload is None or factor_payload is None:
        computed_context = _compute_model_context_from_artifacts(offset_dir, top_n=top_n)
    if logreg_payload is None and lgb_payload is None and factor_payload is None and computed_context is None:
        return None

    top_logreg_coefficients = _top_rows(
        logreg_payload,
        row_keys=("rows",),
        keys=("feature", "coefficient", "abs_coefficient", "direction", "rank"),
        top_n=top_n,
    ) or list((computed_context or {}).get("top_logreg_coefficients") or [])
    top_lgb_feature_importance = _top_rows(
        lgb_payload,
        row_keys=("rows",),
        keys=("feature", "gain_importance", "gain_share", "split_importance", "split_share", "rank"),
        top_n=top_n,
    ) or list((computed_context or {}).get("top_lgb_feature_importance") or [])
    top_positive_factors = _top_rows(
        factor_payload,
        row_keys=("top_positive_factors", "rows"),
        keys=("feature", "direction_score", "direction", "target_correlation", "logreg_coefficient", "lgb_gain_importance", "rank"),
        top_n=top_n,
        predicate=lambda row: _safe_float(row.get("direction_score")) is not None and float(row.get("direction_score")) > 0.0,
    ) or list((computed_context or {}).get("top_positive_factors") or [])
    top_negative_factors = _top_rows(
        factor_payload,
        row_keys=("top_negative_factors", "rows"),
        keys=("feature", "direction_score", "direction", "target_correlation", "logreg_coefficient", "lgb_gain_importance", "rank"),
        top_n=top_n,
        predicate=lambda row: _safe_float(row.get("direction_score")) is not None and float(row.get("direction_score")) < 0.0,
        reverse=False,
    ) or list((computed_context or {}).get("top_negative_factors") or [])

    return {
        "logreg_intercept": None if logreg_payload is None else _safe_float(logreg_payload.get("intercept")),
        "top_logreg_coefficients": top_logreg_coefficients,
        "top_lgb_feature_importance": top_lgb_feature_importance,
        "top_positive_factors": top_positive_factors,
        "top_negative_factors": top_negative_factors,
    }


def _compute_model_context_from_artifacts(offset_dir: Path, *, top_n: int) -> dict[str, object] | None:
    try:
        bundle_dir = offset_dir.parents[1]
        offset = int(offset_dir.name.split("=", 1)[1])
        bundle_cfg = read_bundle_config(bundle_dir, offset=offset)
        feature_names = list(bundle_cfg.get("feature_columns") or [])
        if not feature_names:
            return None
        from pm15min.research.training.explainability import (
            build_lgb_feature_importance,
            build_logreg_coefficients,
        )

        logreg_payload = None
        lgb_payload = None
        logreg_path = offset_dir / "models" / "logreg_sigmoid.joblib"
        lgb_path = offset_dir / "models" / "lgbm_sigmoid.joblib"
        if logreg_path.exists():
            resolved_logreg = _resolve_base_estimator(joblib.load(logreg_path))
            logreg_payload = build_logreg_coefficients(feature_names=feature_names, model=resolved_logreg)
        if lgb_path.exists():
            resolved_lgb = _resolve_base_estimator(joblib.load(lgb_path))
            if hasattr(resolved_lgb, "booster_"):
                lgb_payload = build_lgb_feature_importance(feature_names=feature_names, model=resolved_lgb)
    except Exception:
        return None

    top_logreg_coefficients = _top_rows(
        logreg_payload,
        row_keys=("rows",),
        keys=("feature", "coefficient", "abs_coefficient", "direction", "rank"),
        top_n=top_n,
    )
    top_lgb_feature_importance = _top_rows(
        lgb_payload,
        row_keys=("rows",),
        keys=("feature", "gain_importance", "gain_share", "split_importance", "split_share", "rank"),
        top_n=top_n,
    )
    if not top_logreg_coefficients and not top_lgb_feature_importance:
        return None

    lgb_gain_map = {
        str(row.get("feature")): _safe_float(row.get("gain_importance")) or 0.0
        for row in top_lgb_feature_importance
        if row.get("feature") is not None
    }
    positive_rows = []
    negative_rows = []
    for row in top_logreg_coefficients:
        feature = str(row.get("feature") or "")
        coefficient = _safe_float(row.get("coefficient"))
        if not feature or coefficient is None or abs(coefficient) <= 0.0:
            continue
        factor_row = {
            "feature": feature,
            "direction_score": float(coefficient),
            "direction": "positive" if coefficient > 0.0 else "negative",
            "logreg_coefficient": float(coefficient),
            "lgb_gain_importance": float(lgb_gain_map.get(feature, 0.0)),
            "rank": row.get("rank"),
        }
        if coefficient > 0.0:
            positive_rows.append(factor_row)
        else:
            negative_rows.append(factor_row)
    return {
        "logreg_intercept": None if logreg_payload is None else _safe_float(logreg_payload.get("intercept")),
        "top_logreg_coefficients": top_logreg_coefficients,
        "top_lgb_feature_importance": top_lgb_feature_importance,
        "top_positive_factors": positive_rows[: max(0, int(top_n))],
        "top_negative_factors": negative_rows[: max(0, int(top_n))],
    }


def _resolve_base_estimator(model):
    calibrated_items = list(getattr(model, "calibrated_classifiers_", []) or [])
    for item in calibrated_items:
        for name in ("estimator", "base_estimator", "classifier", "classifier_"):
            candidate = getattr(item, name, None)
            if candidate is not None:
                return candidate
    for name in ("estimator", "base_estimator", "classifier", "classifier_"):
        candidate = getattr(model, name, None)
        if candidate is not None:
            return candidate
    return model


def _read_optional_json(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _top_rows(
    payload: dict[str, object] | None,
    *,
    row_keys: tuple[str, ...],
    keys: tuple[str, ...],
    top_n: int,
    predicate=None,
    reverse: bool = True,
) -> list[dict[str, object]]:
    if not isinstance(payload, dict):
        return []
    rows: list[dict[str, object]] = []
    for row_key in row_keys:
        rows = [dict(row) for row in list(payload.get(row_key) or []) if isinstance(row, dict)]
        if rows:
            break
    if predicate is not None:
        rows = [row for row in rows if predicate(row)]
    if not reverse:
        rows = sorted(rows, key=lambda row: _safe_float(row.get("direction_score")) or 0.0)
    out: list[dict[str, object]] = []
    for row in rows[: max(0, int(top_n))]:
        item = {key: row.get(key) for key in keys if key in row}
        out.append(item)
    return out


def _safe_float(value: object) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def score_bundle_offset(
    bundle_dir: Path,
    features: pd.DataFrame,
    *,
    offset: int,
    include_model_context: bool = False,
) -> pd.DataFrame:
    offset_dir = bundle_dir / "offsets" / f"offset={int(offset)}"
    runtime = _load_offset_scoring_runtime(offset_dir)
    bundle_cfg = runtime.bundle_cfg
    feature_columns = list(runtime.feature_columns)
    if not feature_columns:
        raise ValueError(f"Bundle has no feature columns for offset={offset}: {offset_dir}")

    model_context = load_offset_model_context(offset_dir) if include_model_context else None

    rows = features.copy()
    rows = rows[pd.to_numeric(rows.get("offset"), errors="coerce") == int(offset)].copy()
    if rows.empty:
        return pd.DataFrame(
            columns=[
                "decision_ts",
                "offset",
                "p_lgb",
                "p_lr",
                "p_signal",
                "w_lgb",
                "w_lr",
                "p_up",
                "p_down",
                "probability_mode",
                "model_context",
                "score_valid",
                "score_reason",
            ]
        )

    X = rows.copy()
    for column in feature_columns:
        if column not in X.columns:
            X[column] = 0.0
        X[column] = pd.to_numeric(X[column], errors="coerce")
    fill_value = float(runtime.missing_feature_fill_value)
    X = X[feature_columns].replace([np.inf, -np.inf], np.nan).fillna(fill_value)
    for column in runtime.allowed_blacklist_columns:
        if column in X.columns:
            X[column] = fill_value

    p_lgb = runtime.model_lgb.predict_proba(X)[:, 1].astype(float)
    p_lr = runtime.model_lr.predict_proba(X)[:, 1].astype(float)
    p_signal = runtime.w_lgb * p_lgb + runtime.w_lr * p_lr

    signal_target = runtime.signal_target
    if signal_target == "reversal":
        p_up_raw, p_down_raw, p_up, p_down, score_valid, score_reason = _reversal_probability_views(
            rows,
            p_signal,
            reliability_bins=list(runtime.reliability_bins),
        )
    else:
        p_up_raw, p_down_raw, p_up, p_down = _direction_probability_views(
            p_signal,
            reliability_bins=list(runtime.reliability_bins),
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
            "w_lgb": [runtime.w_lgb] * len(rows),
            "w_lr": [runtime.w_lr] * len(rows),
            "p_up_raw": p_up_raw,
            "p_down_raw": p_down_raw,
            "p_eff_up": p_up,
            "p_eff_down": p_down,
            "p_up": p_up,
            "p_down": p_down,
            "probability_mode": ["conservative_reliability_bin" if runtime.reliability_bins else "raw_blend"] * len(rows),
            "model_context": [model_context] * len(rows),
            "score_valid": score_valid.values,
            "score_reason": score_reason.values,
        }
    )


def _load_offset_scoring_runtime(offset_dir: Path) -> _OffsetScoringRuntime:
    signature = _offset_scoring_runtime_signature(offset_dir)
    return _load_offset_scoring_runtime_cached(str(Path(offset_dir)), signature)


@lru_cache(maxsize=256)
def _load_offset_scoring_runtime_cached(
    offset_dir_str: str,
    signature: tuple[tuple[str, int | None], ...],
) -> _OffsetScoringRuntime:
    del signature
    offset_dir = Path(offset_dir_str)
    bundle_dir = offset_dir.parents[1]
    offset = int(offset_dir.name.split("=", 1)[1])
    bundle_cfg = read_bundle_config(bundle_dir, offset=offset)
    feature_columns = tuple(str(value) for value in (bundle_cfg.get("feature_columns") or []) if str(value))
    allowed_blacklist_columns = tuple(
        str(value) for value in (bundle_cfg.get("allowed_blacklist_columns") or []) if str(value)
    )
    missing_feature_fill_value = float(bundle_cfg.get("missing_feature_fill_value", 0.0))
    signal_target = str(bundle_cfg.get("signal_target") or "direction").strip().lower()

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
    reliability_bins = tuple(_resolve_blend_reliability_bins(offset_dir, w_lgb=w_lgb, w_lr=w_lr))

    return _OffsetScoringRuntime(
        bundle_cfg=dict(bundle_cfg),
        feature_columns=feature_columns,
        allowed_blacklist_columns=allowed_blacklist_columns,
        missing_feature_fill_value=missing_feature_fill_value,
        signal_target=signal_target,
        model_lgb=model_lgb,
        model_lr=model_lr,
        w_lgb=float(w_lgb),
        w_lr=float(w_lr),
        reliability_bins=reliability_bins,
    )


def _offset_scoring_runtime_signature(offset_dir: Path) -> tuple[tuple[str, int | None], ...]:
    return _path_mtime_signature(
        offset_dir / "bundle_config.json",
        offset_dir / "models" / "lgbm_sigmoid.joblib",
        offset_dir / "models" / "logreg_sigmoid.joblib",
        offset_dir / "calibration" / "blend_weights.json",
        offset_dir / "calibration" / "reliability_bins_blend.json",
        offset_dir / "calibration" / "reliability_bins_blend_weighted.json",
    )


def _offset_model_context_signature(offset_dir: Path) -> tuple[tuple[str, int | None], ...]:
    return _path_mtime_signature(
        offset_dir / "bundle_config.json",
        offset_dir / "diagnostics" / "logreg_coefficients.json",
        offset_dir / "diagnostics" / "lgb_feature_importance.json",
        offset_dir / "diagnostics" / "factor_direction_summary.json",
        offset_dir / "models" / "lgbm_sigmoid.joblib",
        offset_dir / "models" / "logreg_sigmoid.joblib",
    )


def _path_mtime_signature(*paths: Path) -> tuple[tuple[str, int | None], ...]:
    return tuple((str(path), _path_mtime_ns(path)) for path in paths)


def _path_mtime_ns(path: Path) -> int | None:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return None
    except Exception:
        return None
