from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


def _wilson_interval(k: int, n: int, *, alpha: float = 0.1) -> tuple[float, float]:
    if n <= 0:
        return 0.0, 1.0
    p = float(k) / float(n)
    if abs(alpha - 0.2) < 1e-9:
        z = 1.2815515655446004
    elif abs(alpha - 0.05) < 1e-9:
        z = 1.959963984540054
    else:
        z = 1.6448536269514722
    denom = 1.0 + (z * z / float(n))
    center = (p + (z * z / (2.0 * float(n)))) / denom
    margin = (z * np.sqrt((p * (1.0 - p) / float(n)) + (z * z / (4.0 * float(n) * float(n))))) / denom
    return max(0.0, float(center - margin)), min(1.0, float(center + margin))


def build_reliability_bins(
    y_true: np.ndarray,
    prob: np.ndarray,
    *,
    n_bins: int = 10,
    alpha: float = 0.1,
) -> list[dict[str, float | int]]:
    frame = pd.DataFrame({"y": np.asarray(y_true, dtype=int), "p": np.asarray(prob, dtype=float)}).dropna()
    if frame.empty:
        return []
    frame["p"] = frame["p"].clip(1e-9, 1.0 - 1e-9)
    edges = np.linspace(0.0, 1.0, int(n_bins) + 1)
    frame["bin"] = pd.cut(frame["p"], bins=edges, include_lowest=True, right=True)
    rows: list[dict[str, float | int]] = []
    for bucket, group in frame.groupby("bin", observed=False):
        n = int(len(group))
        positives = int(group["y"].sum())
        observed = float(group["y"].mean()) if n else math.nan
        predicted = float(group["p"].mean()) if n else math.nan
        lower, upper = _wilson_interval(positives, n, alpha=float(alpha))
        rows.append(
            {
                "left": float(bucket.left),
                "right": float(bucket.right),
                "n": n,
                "predicted_mean": predicted,
                "observed_rate": observed,
                "gap": float(predicted - observed) if n else math.nan,
                "alpha": float(alpha),
                "positives": positives,
                "lower": lower,
                "upper": upper,
            }
        )
    return rows


def load_reliability_bins(path: str | Path) -> list[dict[str, float | int]]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(data, dict):
        bins = data.get("bins")
        if isinstance(bins, list):
            return list(bins)
        return []
    if isinstance(data, list):
        return list(data)
    return []


def lcb_from_bins(probability: float, bins: list[dict[str, float | int]]) -> float:
    p = max(0.0, min(1.0, float(probability)))
    for row in bins:
        left = float(row.get("left", 0.0))
        right = float(row.get("right", 1.0))
        if left <= p <= right:
            n = int(row.get("n", 0) or 0)
            return float(row.get("lower", p)) if n > 0 else p
    return p


def ucb_from_bins(probability: float, bins: list[dict[str, float | int]]) -> float:
    p = max(0.0, min(1.0, float(probability)))
    for row in bins:
        left = float(row.get("left", 0.0))
        right = float(row.get("right", 1.0))
        if left <= p <= right:
            n = int(row.get("n", 0) or 0)
            return float(row.get("upper", p)) if n > 0 else p
    return p
