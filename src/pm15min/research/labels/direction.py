from __future__ import annotations

import pandas as pd


def build_direction_target(frame: pd.DataFrame) -> pd.Series:
    return pd.to_numeric(frame.get("direction_up"), errors="coerce").astype(float)
