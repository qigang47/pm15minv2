from __future__ import annotations

import hashlib
import json

import numpy as np
import pandas as pd


def build_final_model_probe(
    *,
    X: pd.DataFrame,
    y: pd.Series,
    p_lgb: np.ndarray,
    p_lr: np.ndarray,
    p_blend: np.ndarray,
) -> dict[str, object]:
    probe_n = max(1, min(128, int(len(X))))
    tail_index = X.index[-probe_n:]
    tail_slice = slice(len(X) - probe_n, len(X))
    return {
        "probe_rows": int(probe_n),
        "feature_count": int(X.shape[1]),
        "feature_columns_sha256": hashlib.sha256(
            json.dumps(list(X.columns), ensure_ascii=False).encode("utf-8")
        ).hexdigest(),
        "probe_y_head": [int(value) for value in y.loc[tail_index].astype(int).head(8).tolist()],
        "probe_p_lgb_head": [float(value) for value in np.asarray(p_lgb, dtype=float)[tail_slice][:8]],
        "probe_p_lr_head": [float(value) for value in np.asarray(p_lr, dtype=float)[tail_slice][:8]],
        "probe_p_blend_head": [float(value) for value in np.asarray(p_blend, dtype=float)[tail_slice][:8]],
    }
