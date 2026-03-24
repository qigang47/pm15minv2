from __future__ import annotations

from pathlib import Path

import pandas as pd


def read_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    return pd.read_parquet(path) if path.exists() else None


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(path)
    return path


def upsert_parquet(
    *,
    path: Path,
    incoming: pd.DataFrame,
    key_columns: list[str],
    sort_columns: list[str],
) -> pd.DataFrame:
    existing = read_parquet_if_exists(path)
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
