from __future__ import annotations

import numpy as np
import pandas as pd


def build_purged_time_series_splits(
    decision_ts: pd.Series,
    *,
    n_splits: int,
    purge_minutes: int = 0,
    embargo_minutes: int = 0,
) -> list[tuple[np.ndarray, np.ndarray]]:
    ordered_ts = pd.to_datetime(decision_ts, utc=True, errors="coerce")
    valid_mask = ordered_ts.notna()
    if int(valid_mask.sum()) < 8:
        return []

    valid_positions = np.flatnonzero(valid_mask.to_numpy())
    chunks = np.array_split(valid_positions, max(2, min(int(n_splits), len(valid_positions) // 2)))
    purge_delta = pd.Timedelta(minutes=max(0, int(purge_minutes)))
    embargo_delta = pd.Timedelta(minutes=max(0, int(embargo_minutes)))
    splits: list[tuple[np.ndarray, np.ndarray]] = []

    for test_positions in chunks[1:]:
        if len(test_positions) == 0:
            continue
        test_start = ordered_ts.iloc[int(test_positions[0])]
        test_end = ordered_ts.iloc[int(test_positions[-1])]
        if pd.isna(test_start) or pd.isna(test_end):
            continue
        train_cutoff = test_start - purge_delta
        test_open = test_start + embargo_delta
        train_idx = valid_positions[ordered_ts.iloc[valid_positions] <= train_cutoff]
        test_idx = valid_positions[
            (ordered_ts.iloc[valid_positions] >= test_open) & (ordered_ts.iloc[valid_positions] <= test_end)
        ]
        if len(train_idx) == 0 or len(test_idx) == 0:
            continue
        splits.append((train_idx, test_idx))
    return splits
