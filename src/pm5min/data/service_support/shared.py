from __future__ import annotations

from typing import Any

import pandas as pd


def duplicate_count(df: pd.DataFrame, *, key_columns: list[str]) -> int:
    usable = [column for column in key_columns if column in df.columns]
    if not usable or df.empty:
        return 0
    try:
        return int(df.duplicated(subset=usable).sum())
    except Exception:
        return 0


def null_key_count(df: pd.DataFrame, *, key_columns: list[str]) -> int:
    usable = [column for column in key_columns if column in df.columns]
    if not usable or df.empty:
        return 0
    try:
        return int(df[usable].isna().any(axis=1).sum())
    except Exception:
        return 0


def time_range(df: pd.DataFrame, *, columns: list[str]) -> dict[str, str | None]:
    for column in columns:
        if column not in df.columns or df.empty:
            continue
        series = coerce_utc_series(df[column]).dropna()
        if series.empty:
            continue
        return {
            "min": series.min().isoformat(),
            "max": series.max().isoformat(),
        }
    return {"min": None, "max": None}


def coerce_utc_series(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        cleaned = numeric.dropna()
        if cleaned.empty:
            return pd.to_datetime(numeric, utc=True, errors="coerce")
        max_abs = float(cleaned.abs().max())
        if max_abs >= 1e14:
            unit = "ns"
        elif max_abs >= 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(numeric, utc=True, errors="coerce", unit=unit)
    normalized = series.astype("string").str.strip().str.replace(
        r"^(\d{4}-\d{2}-\d{2}T\d{2})-(\d{2})-(\d{2})Z$",
        r"\1:\2:\3Z",
        regex=True,
    )
    parsed = pd.to_datetime(normalized, utc=True, errors="coerce", format="ISO8601")
    if parsed.notna().any():
        return parsed
    return pd.to_datetime(normalized, utc=True, errors="coerce", format="mixed")


def normalize_utc_timestamp(value: pd.Timestamp | None) -> pd.Timestamp:
    if value is None:
        return pd.Timestamp.now(tz="UTC").floor("s")
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def latest_freshness_timestamp(item: dict[str, Any]) -> pd.Timestamp | None:
    return parse_iso_timestamp((item.get("freshness_range") or {}).get("max")) or latest_semantic_timestamp(item)


def latest_semantic_timestamp(item: dict[str, Any]) -> pd.Timestamp | None:
    return parse_iso_timestamp((item.get("time_range") or {}).get("max"))


def latest_partition_date(item: dict[str, Any]) -> pd.Timestamp | None:
    value = (item.get("date_range") or {}).get("max")
    if value is None:
        return None
    try:
        return pd.Timestamp(str(value), tz="UTC")
    except Exception:
        return None


def parse_iso_timestamp(value: Any) -> pd.Timestamp | None:
    if value in {None, ""}:
        return None
    try:
        ts = pd.Timestamp(value)
    except Exception:
        return None
    if ts.tzinfo is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def ratio(numerator: int, denominator: int) -> float:
    if int(denominator) <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 4)


def optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except Exception:
        return None


def dataset_storage_path(item: dict[str, Any]) -> str | None:
    path = item.get("path") or item.get("root")
    if path in {None, ""}:
        return None
    return str(path)


def dataset_layer(name: str) -> str:
    if name.endswith("_table"):
        return "tables"
    if name.endswith("_source"):
        return "sources"
    return "unknown"


def dataset_issue_codes(*, dataset_name: str, issues: list[dict[str, Any]]) -> list[str]:
    return sorted(
        {
            str(issue.get("code") or "")
            for issue in issues
            if str(issue.get("target") or "") == dataset_name and str(issue.get("code") or "")
        }
    )
