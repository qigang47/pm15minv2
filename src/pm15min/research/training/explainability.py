from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd


def build_logreg_coefficients(*, feature_names: Sequence[str], model) -> dict[str, object]:
    classifier = model.named_steps.get("clf") if hasattr(model, "named_steps") else model
    raw = np.asarray(getattr(classifier, "coef_", np.zeros((1, len(feature_names)), dtype=float)), dtype=float).reshape(-1)
    intercept_raw = np.asarray(getattr(classifier, "intercept_", np.zeros(1, dtype=float)), dtype=float).reshape(-1)
    rows = [
        {
            "feature": str(feature),
            "coefficient": float(coefficient),
            "abs_coefficient": float(abs(coefficient)),
            "direction": _direction_label(float(coefficient)),
        }
        for feature, coefficient in sorted(
            zip(feature_names, raw, strict=False),
            key=lambda item: (abs(float(item[1])), str(item[0])),
            reverse=True,
        )
    ]
    for index, row in enumerate(rows, start=1):
        row["rank"] = int(index)
    return {
        "intercept": float(intercept_raw[0]) if len(intercept_raw) else 0.0,
        "feature_count": int(len(rows)),
        "rows": rows,
    }


def build_lgb_feature_importance(*, feature_names: Sequence[str], model) -> dict[str, object]:
    booster = model.booster_
    gain = np.asarray(booster.feature_importance(importance_type="gain"), dtype=float).reshape(-1)
    split = np.asarray(booster.feature_importance(importance_type="split"), dtype=float).reshape(-1)
    gain_total = float(gain.sum())
    split_total = float(split.sum())
    rows = [
        {
            "feature": str(feature),
            "gain_importance": float(gain_value),
            "gain_share": float(gain_value / gain_total) if gain_total > 0.0 else 0.0,
            "split_importance": int(split_value),
            "split_share": float(split_value / split_total) if split_total > 0.0 else 0.0,
        }
        for feature, gain_value, split_value in sorted(
            zip(feature_names, gain, split, strict=False),
            key=lambda item: (float(item[1]), float(item[2]), str(item[0])),
            reverse=True,
        )
    ]
    for index, row in enumerate(rows, start=1):
        row["rank"] = int(index)
    return {
        "feature_count": int(len(rows)),
        "gain_total": gain_total,
        "split_total": split_total,
        "rows": rows,
    }


def build_factor_correlation_frame(
    *,
    X: pd.DataFrame,
    y: pd.Series,
    p_lgb: np.ndarray,
    p_lr: np.ndarray,
    p_blend: np.ndarray,
) -> pd.DataFrame:
    frame = X.copy()
    frame["target_y"] = pd.Series(np.asarray(y, dtype=float), index=X.index, dtype=float)
    frame["prediction_lgbm"] = pd.Series(np.asarray(p_lgb, dtype=float), index=X.index, dtype=float)
    frame["prediction_logreg"] = pd.Series(np.asarray(p_lr, dtype=float), index=X.index, dtype=float)
    frame["prediction_blend"] = pd.Series(np.asarray(p_blend, dtype=float), index=X.index, dtype=float)
    corr = frame.corr(numeric_only=True).fillna(0.0)
    rows: list[dict[str, object]] = []
    for left_index, left_feature in enumerate(corr.index):
        for right_index in range(left_index + 1, len(corr.columns)):
            right_feature = corr.columns[right_index]
            correlation = float(corr.iloc[left_index, right_index])
            rows.append(
                {
                    "left_feature": str(left_feature),
                    "right_feature": str(right_feature),
                    "correlation": correlation,
                    "abs_correlation": float(abs(correlation)),
                }
            )
    if not rows:
        return pd.DataFrame(columns=["left_feature", "right_feature", "correlation", "abs_correlation"])
    return pd.DataFrame(rows).sort_values(
        ["abs_correlation", "left_feature", "right_feature"],
        ascending=[False, True, True],
    ).reset_index(drop=True)


def build_factor_direction_summary(
    *,
    X: pd.DataFrame,
    y: pd.Series,
    logreg_coefficients: dict[str, object],
    lgb_importance: dict[str, object],
) -> dict[str, object]:
    target = pd.Series(np.asarray(y, dtype=float), index=X.index, dtype=float)
    positive_mask = target.ge(0.5)
    negative_mask = ~positive_mask
    logreg_map = {
        str(row.get("feature")): float(row.get("coefficient", 0.0))
        for row in list(logreg_coefficients.get("rows") or [])
        if isinstance(row, dict) and row.get("feature") is not None
    }
    gain_map = {
        str(row.get("feature")): float(row.get("gain_importance", 0.0))
        for row in list(lgb_importance.get("rows") or [])
        if isinstance(row, dict) and row.get("feature") is not None
    }
    split_map = {
        str(row.get("feature")): int(row.get("split_importance", 0))
        for row in list(lgb_importance.get("rows") or [])
        if isinstance(row, dict) and row.get("feature") is not None
    }
    rows: list[dict[str, object]] = []
    for feature in X.columns:
        values = pd.to_numeric(X[feature], errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(0.0)
        target_corr = values.corr(target)
        positive_mean = float(values[positive_mask].mean()) if positive_mask.any() else 0.0
        negative_mean = float(values[negative_mask].mean()) if negative_mask.any() else 0.0
        mean_gap = positive_mean - negative_mean
        direction_score = _direction_score(
            target_correlation=0.0 if pd.isna(target_corr) else float(target_corr),
            mean_gap=mean_gap,
            logreg_coefficient=logreg_map.get(str(feature), 0.0),
        )
        rows.append(
            {
                "feature": str(feature),
                "target_correlation": 0.0 if pd.isna(target_corr) else float(target_corr),
                "positive_class_mean": positive_mean,
                "negative_class_mean": negative_mean,
                "mean_gap": mean_gap,
                "logreg_coefficient": float(logreg_map.get(str(feature), 0.0)),
                "abs_logreg_coefficient": float(abs(logreg_map.get(str(feature), 0.0))),
                "lgb_gain_importance": float(gain_map.get(str(feature), 0.0)),
                "lgb_split_importance": int(split_map.get(str(feature), 0)),
                "direction_score": float(direction_score),
                "direction": _direction_label(direction_score),
            }
        )
    rows = sorted(
        rows,
        key=lambda row: (
            abs(float(row["direction_score"])),
            float(row["lgb_gain_importance"]),
            float(row["abs_logreg_coefficient"]),
            str(row["feature"]),
        ),
        reverse=True,
    )
    for index, row in enumerate(rows, start=1):
        row["rank"] = int(index)
    positive_rows = [
        {"feature": str(row["feature"]), "direction_score": float(row["direction_score"])}
        for row in rows
        if float(row["direction_score"]) > 0.0
    ][:5]
    negative_rows = [
        {"feature": str(row["feature"]), "direction_score": float(row["direction_score"])}
        for row in sorted(rows, key=lambda row: float(row["direction_score"]))
        if float(row["direction_score"]) < 0.0
    ][:5]
    return {
        "feature_count": int(len(rows)),
        "rows": rows,
        "top_positive_factors": positive_rows,
        "top_negative_factors": negative_rows,
    }


def _direction_score(*, target_correlation: float, mean_gap: float, logreg_coefficient: float) -> float:
    if abs(float(target_correlation)) > 1e-12:
        return float(target_correlation)
    if abs(float(mean_gap)) > 1e-12:
        return float(mean_gap)
    return float(logreg_coefficient)


def _direction_label(value: float) -> str:
    if value > 0.0:
        return "positive"
    if value < 0.0:
        return "negative"
    return "flat"
