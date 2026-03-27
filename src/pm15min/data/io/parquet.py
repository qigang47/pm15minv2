from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import time

import pandas as pd


def read_parquet_if_exists(path: Path, *, recover_corrupt: bool = False) -> pd.DataFrame | None:
    if not path.exists():
        return None
    try:
        return pd.read_parquet(path)
    except Exception:
        if not recover_corrupt:
            raise
        _quarantine_corrupt_parquet(path)
        return None


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)
    return path


def upsert_parquet(
    *,
    path: Path,
    incoming: pd.DataFrame,
    key_columns: list[str],
    sort_columns: list[str],
    recover_existing_read_errors: bool = False,
) -> pd.DataFrame:
    existing = read_parquet_if_exists(path, recover_corrupt=recover_existing_read_errors)
    if existing is None or existing.empty:
        combined = incoming.copy()
    elif incoming.empty:
        combined = existing.copy()
    else:
        combined = pd.concat([existing, incoming], ignore_index=True, sort=False)

    if combined.empty:
        return combined

    combined = combined.sort_values(sort_columns).drop_duplicates(subset=key_columns, keep="last")
    combined = combined.reset_index(drop=True)
    write_parquet_atomic(combined, path)
    return combined


def _quarantine_corrupt_parquet(path: Path) -> Path | None:
    if not path.exists():
        return None
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    target = path.with_name(f"{path.name}.corrupt.{timestamp}")
    suffix = 1
    while target.exists():
        target = path.with_name(f"{path.name}.corrupt.{timestamp}.{suffix}")
        suffix += 1
    try:
        path.replace(target)
        return target
    except Exception:
        return None
