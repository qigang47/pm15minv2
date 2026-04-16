from __future__ import annotations

import numpy as np
import pandas as pd


def compute_sample_weights(
    frame: pd.DataFrame,
    y: pd.Series,
    *,
    balance_classes: bool = True,
    weight_by_vol: bool = True,
    inverse_vol: bool = False,
    contrarian_weight: float = 1.0,
    contrarian_quantile: float = 0.8,
    contrarian_return_col: str = "ret_from_strike",
    winner_in_band_weight: float = 1.0,
) -> pd.Series:
    weights = pd.Series(1.0, index=frame.index, dtype=float)

    if balance_classes and len(y):
        counts = y.value_counts(dropna=False)
        total = float(len(y))
        for klass, count in counts.items():
            if int(count) <= 0:
                continue
            weights.loc[y.eq(klass)] *= total / (len(counts) * float(count))

    if weight_by_vol and "rv_30" in frame.columns:
        vol = pd.to_numeric(frame["rv_30"], errors="coerce").replace([np.inf, -np.inf], np.nan)
        vol = vol.fillna(vol.median() if vol.notna().any() else 1.0).clip(lower=1e-6)
        if inverse_vol:
            weights *= 1.0 / vol
        else:
            weights *= vol / float(vol.median() if float(vol.median()) > 0 else 1.0)

    if contrarian_weight > 1.0 and contrarian_return_col in frame.columns:
        returns = pd.to_numeric(frame[contrarian_return_col], errors="coerce")
        threshold = returns.abs().quantile(float(contrarian_quantile))
        contrarian_mask = returns.abs().ge(float(threshold)) & returns.notna()
        weights.loc[contrarian_mask] *= float(contrarian_weight)

    if winner_in_band_weight > 1.0 and "winner_in_band" in frame.columns:
        winner_in_band = frame["winner_in_band"].astype("boolean").fillna(False).astype(bool)
        weights.loc[winner_in_band] *= float(winner_in_band_weight)

    return weights.clip(lower=1e-6)
