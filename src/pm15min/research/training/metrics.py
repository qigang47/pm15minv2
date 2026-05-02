from __future__ import annotations

import math

import numpy as np
import pandas as pd
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score


def classification_metrics(y_true: np.ndarray, prob: np.ndarray) -> dict[str, float | None]:
    y = np.asarray(y_true, dtype=int)
    p = np.asarray(prob, dtype=float)
    metrics: dict[str, float | None] = {
        "n": float(len(y)),
        "positive_rate": float(y.mean()) if len(y) else None,
        "brier": None,
        "logloss": None,
        "auc": None,
    }
    if len(y) == 0:
        return metrics

    clipped = np.clip(p, 1e-9, 1.0 - 1e-9)
    metrics["brier"] = float(brier_score_loss(y, clipped))
    metrics["logloss"] = float(log_loss(y, clipped, labels=[0, 1]))
    if len(np.unique(y)) >= 2:
        metrics["auc"] = float(roc_auc_score(y, clipped))
    return metrics


def blend_weights_from_brier(
    *,
    brier_lgb: float | None,
    brier_lr: float | None,
    brier_catboost: float | None = None,
) -> dict[str, float]:
    values = {
        "w_lgb": brier_lgb,
        "w_lr": brier_lr,
    }
    if brier_catboost is not None:
        values["w_catboost"] = brier_catboost
    finite_values = {
        key: float(value)
        for key, value in values.items()
        if value is not None and math.isfinite(float(value))
    }
    if len(finite_values) < len(values):
        equal_weight = 1.0 / max(1, len(values))
        return {key: float(equal_weight) for key in values}
    inverse = {key: 1.0 / max(value, 1e-9) for key, value in finite_values.items()}
    total = sum(inverse.values())
    if total <= 0:
        equal_weight = 1.0 / max(1, len(values))
        return {key: float(equal_weight) for key in values}
    return {key: float(value / total) for key, value in inverse.items()}


def feature_schema_rows(X: pd.DataFrame) -> list[dict[str, str]]:
    return [{"name": str(column), "dtype": str(X[column].dtype)} for column in X.columns]
