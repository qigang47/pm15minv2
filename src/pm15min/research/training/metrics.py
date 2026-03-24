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


def blend_weights_from_brier(*, brier_lgb: float | None, brier_lr: float | None) -> dict[str, float]:
    if brier_lgb is None or brier_lr is None or not math.isfinite(brier_lgb) or not math.isfinite(brier_lr):
        return {"w_lgb": 0.5, "w_lr": 0.5}
    inv_lgb = 1.0 / max(float(brier_lgb), 1e-9)
    inv_lr = 1.0 / max(float(brier_lr), 1e-9)
    total = inv_lgb + inv_lr
    if total <= 0:
        return {"w_lgb": 0.5, "w_lr": 0.5}
    return {"w_lgb": float(inv_lgb / total), "w_lr": float(inv_lr / total)}


def feature_schema_rows(X: pd.DataFrame) -> list[dict[str, str]]:
    return [{"name": str(column), "dtype": str(X[column].dtype)} for column in X.columns]
