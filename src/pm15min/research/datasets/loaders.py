from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pyarrow.types as patypes

from pm15min.research.config import ResearchConfig
from pm15min.research.labels.sources import normalize_label_set


def _parquet_schema_columns(path: Path) -> list[str]:
    return [str(name) for name in pq.ParquetFile(path).schema.names]


def _parquet_schema_types(path: Path) -> dict[str, object]:
    schema = pq.ParquetFile(path).schema_arrow
    return {str(field.name): field.type for field in schema}


def _read_required_parquet(
    path: Path,
    *,
    kind: str,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing {kind}: {path}")
    normalized_filters = _normalize_parquet_filters(path, filters)
    if columns is None:
        return pd.read_parquet(path, filters=normalized_filters)

    requested = [str(column) for column in columns if str(column)]
    if not requested:
        return pd.read_parquet(path, columns=[], filters=normalized_filters)

    available = set(_parquet_schema_columns(path))
    selected = [column for column in requested if column in available]
    return pd.read_parquet(path, columns=selected, filters=normalized_filters)


def _normalize_parquet_filters(
    path: Path,
    filters: list[tuple[str, str, object]] | None,
) -> list[tuple[str, str, object]] | None:
    if not filters:
        return filters
    schema_types = _parquet_schema_types(path)
    normalized: list[tuple[str, str, object]] = []
    for column, operator, value in filters:
        field_type = schema_types.get(str(column))
        normalized.append((column, operator, _coerce_filter_value(value=value, field_type=field_type)))
    return normalized


def _coerce_filter_value(*, value: object, field_type: object) -> object:
    if isinstance(value, pd.Timestamp) and field_type is not None:
        if patypes.is_string(field_type) or patypes.is_large_string(field_type):
            return _timestamp_filter_string(value)
    return value


def _timestamp_filter_string(value: pd.Timestamp) -> str:
    ts = value if value.tzinfo is not None else value.tz_localize("UTC")
    return ts.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def load_feature_frame(
    cfg: ResearchConfig,
    *,
    feature_set: str | None = None,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    selected = feature_set or cfg.feature_set
    return _read_required_parquet(
        cfg.layout.feature_frame_path(selected, source_surface=cfg.source_surface),
        kind="feature_frame parquet",
        columns=columns,
        filters=filters,
    )


def load_label_frame(
    cfg: ResearchConfig,
    *,
    label_set: str | None = None,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    requested = label_set or cfg.label_set
    selected = normalize_label_set(requested)
    primary = cfg.layout.label_frame_path(selected)
    if primary.exists():
        return _read_required_parquet(primary, kind="label_frame parquet", columns=columns, filters=filters)
    legacy = cfg.layout.label_frame_path(str(requested))
    if legacy != primary and legacy.exists():
        return _read_required_parquet(legacy, kind="label_frame parquet", columns=columns, filters=filters)
    return _read_required_parquet(primary, kind="label_frame parquet", columns=columns, filters=filters)
