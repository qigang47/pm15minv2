from __future__ import annotations

import numpy as np
import pandas as pd


def resolve_reversal_anchor(frame: pd.DataFrame) -> tuple[pd.Series, str]:
    for column in ("ret_from_strike", "ret_from_cycle_open"):
        if column in frame.columns:
            return pd.to_numeric(frame[column], errors="coerce").astype(float), column
    raise ValueError("reversal target requires ret_from_strike or ret_from_cycle_open in feature frame")


def build_reversal_target(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series, str]:
    direction = pd.to_numeric(frame.get("direction_up"), errors="coerce").astype(float)
    current_ret, current_ret_col = resolve_reversal_anchor(frame)

    current_up = pd.Series(np.nan, index=frame.index, dtype=float)
    current_up.loc[current_ret > 0.0] = 1.0
    current_up.loc[current_ret < 0.0] = 0.0

    target = pd.Series(np.nan, index=frame.index, dtype=float)
    valid = direction.notna() & current_up.notna()
    if bool(valid.any()):
        target.loc[valid] = (direction.loc[valid] != current_up.loc[valid]).astype(float)
    return target, current_up, current_ret_col
