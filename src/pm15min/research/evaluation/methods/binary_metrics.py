from __future__ import annotations

import math
from typing import Iterable

import numpy as np
import pandas as pd


def _coerce_finite_pairs(
    probabilities: Iterable[float],
    outcomes: Iterable[float],
) -> tuple[np.ndarray, np.ndarray]:
    probs = np.asarray(list(probabilities), dtype=np.float64)
    obs = np.asarray(list(outcomes), dtype=np.float64)
    if probs.shape != obs.shape:
        raise ValueError(f"Shape mismatch: p{probs.shape} vs o{obs.shape}")
    mask = np.isfinite(probs) & np.isfinite(obs)
    return probs[mask], obs[mask]


def brier_score(probabilities: Iterable[float], outcomes: Iterable[float]) -> float:
    """Return the mean squared error between binary outcomes and predicted probabilities."""

    probs, obs = _coerce_finite_pairs(probabilities, outcomes)
    if probs.size == 0:
        raise ValueError("No finite samples for brier_score")
    diff = probs - obs
    return float(np.mean(diff * diff))


def summarize_binary_predictions(
    probabilities: Iterable[float],
    outcomes: Iterable[float],
) -> dict[str, float | int | None]:
    """Summarize binary prediction quality using the legacy poly_eval scoring conventions."""

    probs, obs = _coerce_finite_pairs(probabilities, outcomes)
    if probs.size == 0:
        return {
            "count": 0,
            "brier": None,
            "avg_pred": None,
            "empirical_rate": None,
            "baseline_brier": None,
            "delta_vs_baseline": None,
            "bias": None,
        }
    empirical_rate = float(np.mean(obs))
    avg_pred = float(np.mean(probs))
    brier = float(np.mean((probs - obs) ** 2))
    baseline_brier = float(empirical_rate * (1.0 - empirical_rate))
    return {
        "count": int(probs.size),
        "brier": brier,
        "avg_pred": avg_pred,
        "empirical_rate": empirical_rate,
        "baseline_brier": baseline_brier,
        "delta_vs_baseline": float(brier - baseline_brier),
        "bias": float(avg_pred - empirical_rate),
    }


def brier_by_group(
    df: pd.DataFrame,
    *,
    prob_col: str,
    outcome_col: str,
    group_cols: list[str],
) -> pd.DataFrame:
    """Compute Brier score and binary summaries per group."""

    if not group_cols:
        raise ValueError("group_cols must be non-empty")
    for col in [prob_col, outcome_col, *group_cols]:
        if col not in df.columns:
            raise KeyError(f"Missing column: {col}")

    def _agg(group: pd.DataFrame) -> pd.Series:
        probs = pd.to_numeric(group[prob_col], errors="coerce").to_numpy(dtype=float)
        obs = pd.to_numeric(group[outcome_col], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(probs) & np.isfinite(obs)
        if not mask.any():
            return pd.Series(
                {
                    "n": 0,
                    "brier": math.nan,
                    "avg_pred": math.nan,
                    "empirical_rate": math.nan,
                }
            )
        probs = probs[mask]
        obs = obs[mask]
        return pd.Series(
            {
                "n": int(mask.sum()),
                "brier": float(np.mean((probs - obs) ** 2)),
                "avg_pred": float(np.mean(probs)),
                "empirical_rate": float(np.mean(obs)),
            }
        )

    grouped = df.groupby(group_cols, dropna=False)
    try:
        out = grouped.apply(_agg, include_groups=False).reset_index()
    except TypeError:
        out = grouped.apply(_agg).reset_index()
    return out


def calibration_bins(
    df: pd.DataFrame,
    *,
    prob_col: str,
    outcome_col: str,
    n_bins: int = 10,
) -> pd.DataFrame:
    """Build equal-width calibration bins over the closed unit interval."""

    if n_bins <= 0:
        raise ValueError("n_bins must be positive")
    if prob_col not in df.columns or outcome_col not in df.columns:
        raise KeyError(f"Missing {prob_col=} or {outcome_col=}")

    probs = pd.to_numeric(df[prob_col], errors="coerce")
    obs = pd.to_numeric(df[outcome_col], errors="coerce")
    mask = probs.notna() & obs.notna()
    probs = probs[mask].astype(float)
    obs = obs[mask].astype(float)

    bins = np.linspace(0.0, 1.0, int(n_bins) + 1)
    clipped_probs = probs.clip(0.0, 1.0)
    categories = pd.cut(clipped_probs, bins=bins, include_lowest=True, right=True)
    frame = pd.DataFrame({"bin": categories, "p": clipped_probs, "o": obs})

    grouped = (
        frame.groupby("bin", dropna=False, observed=False)
        .agg(count=("o", "size"), avg_pred=("p", "mean"), empirical_rate=("o", "mean"))
        .reset_index()
    )

    def _left(interval: object) -> float:
        try:
            return max(0.0, float(interval.left))
        except Exception:
            return math.nan

    def _right(interval: object) -> float:
        try:
            return min(1.0, float(interval.right))
        except Exception:
            return math.nan

    grouped["bin_left"] = grouped["bin"].map(_left)
    grouped["bin_right"] = grouped["bin"].map(_right)
    grouped["bin_label"] = grouped.apply(
        lambda row: f"[{row['bin_left']:.2f}, {row['bin_right']:.2f}]",
        axis=1,
    )
    columns = ["bin_label", "count", "avg_pred", "empirical_rate", "bin_left", "bin_right"]
    return grouped[columns].sort_values(["bin_left", "bin_right"]).reset_index(drop=True)
