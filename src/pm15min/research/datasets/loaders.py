from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pm15min.research.config import ResearchConfig
from pm15min.research.labels.sources import normalize_label_set


def _parquet_schema_columns(path: Path) -> list[str]:
    return [str(name) for name in pq.ParquetFile(path).schema.names]


def _read_required_parquet(
    path: Path,
    *,
    kind: str,
    columns: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {kind}: {path}")
    if columns is None:
        return pd.read_parquet(path)

    requested = [str(column) for column in columns if str(column)]
    if not requested:
        return pd.read_parquet(path, columns=[])

    available = set(_parquet_schema_columns(path))
    selected = [column for column in requested if column in available]
    return pd.read_parquet(path, columns=selected)


def load_feature_frame(
    cfg: ResearchConfig,
    *,
    feature_set: str | None = None,
    columns: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame:
    selected = feature_set or cfg.feature_set
    return _read_required_parquet(
        cfg.layout.feature_frame_path(selected, source_surface=cfg.source_surface),
        kind="feature_frame parquet",
        columns=columns,
    )


def load_label_frame(
    cfg: ResearchConfig,
    *,
    label_set: str | None = None,
    columns: list[str] | tuple[str, ...] | None = None,
) -> pd.DataFrame:
    requested = label_set or cfg.label_set
    selected = normalize_label_set(requested)
    primary = cfg.layout.label_frame_path(selected)
    if primary.exists():
        return _read_required_parquet(primary, kind="label_frame parquet", columns=columns)
    legacy = cfg.layout.label_frame_path(str(requested))
    if legacy != primary and legacy.exists():
        return _read_required_parquet(legacy, kind="label_frame parquet", columns=columns)
    return _read_required_parquet(primary, kind="label_frame parquet", columns=columns)
