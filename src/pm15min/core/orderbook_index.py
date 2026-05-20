from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator

import pandas as pd


POST_DECISION_QUOTE_TOLERANCE_MS = 120_000
ORDERBOOK_INDEX_DEDUP_COLUMNS = ("captured_ts_ms", "market_id", "token_id", "side")
ParquetFilters = list[tuple[str, str, object]] | list[list[tuple[str, str, object]]]


def orderbook_index_journal_path(index_path: Path) -> Path:
    return index_path.with_name(f"{index_path.name}.journal.jsonl")


def load_orderbook_index_journal_frame(
    journal_path: Path,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: ParquetFilters | None = None,
) -> pd.DataFrame:
    if not journal_path.exists():
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    try:
        with journal_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                text = line.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except Exception:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
    except Exception:
        return pd.DataFrame()
    frame = pd.DataFrame(rows)
    frame = _apply_basic_filters(frame, filters)
    if columns is None:
        return frame
    selected = [str(column) for column in columns if str(column) in frame.columns]
    return frame.loc[:, selected].copy()


def resolve_orderbook_row(
    index_df: pd.DataFrame,
    *,
    market_id: str,
    token_id: str,
    side: str,
    decision_ts_ms: int | None,
):
    df = index_df.copy()
    df = df[
        (df["market_id"].astype(str) == str(market_id))
        & (df["token_id"].astype(str) == str(token_id))
        & (df["side"].astype(str).str.lower() == str(side).lower())
    ]
    df["captured_ts_ms"] = pd.to_numeric(df["captured_ts_ms"], errors="coerce")
    df = df.dropna(subset=["captured_ts_ms"])
    if df.empty:
        return None
    if decision_ts_ms is None:
        return df.sort_values("captured_ts_ms").iloc[-1].to_dict()

    past = df[df["captured_ts_ms"] <= int(decision_ts_ms)]
    if not past.empty:
        return past.sort_values("captured_ts_ms").iloc[-1].to_dict()

    future = df[
        (df["captured_ts_ms"] > int(decision_ts_ms))
        & (df["captured_ts_ms"] <= int(decision_ts_ms) + POST_DECISION_QUOTE_TOLERANCE_MS)
    ]
    if not future.empty:
        return future.sort_values("captured_ts_ms").iloc[0].to_dict()
    return None


def resolve_orderbook_row_within_window(
    index_df: pd.DataFrame,
    *,
    market_id: str,
    token_id: str,
    side: str,
    reference_ts_ms: int | None,
    window_start_ts_ms: int | None,
    window_end_ts_ms: int | None,
):
    df = index_df.copy()
    df = df[
        (df["market_id"].astype(str) == str(market_id))
        & (df["token_id"].astype(str) == str(token_id))
        & (df["side"].astype(str).str.lower() == str(side).lower())
    ]
    df["captured_ts_ms"] = pd.to_numeric(df["captured_ts_ms"], errors="coerce")
    df = df.dropna(subset=["captured_ts_ms"])
    if df.empty:
        return None
    if window_end_ts_ms is not None:
        df = df[df["captured_ts_ms"] < int(window_end_ts_ms)]
    if df.empty:
        return None
    if reference_ts_ms is not None:
        df = df[df["captured_ts_ms"] <= int(reference_ts_ms)]
        if df.empty:
            return None
    df = df.sort_values("captured_ts_ms")
    if window_start_ts_ms is None:
        return df.iloc[-1].to_dict()
    in_window = df[df["captured_ts_ms"] >= int(window_start_ts_ms)]
    if not in_window.empty:
        return in_window.iloc[-1].to_dict()
    return df.iloc[-1].to_dict()


def load_orderbook_index_frame(
    *,
    index_path: Path,
    recent_path: Path | None = None,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: ParquetFilters | None = None,
) -> pd.DataFrame:
    requested_columns = _normalize_columns(columns)
    read_columns = _read_columns_for_orderbook_index(requested_columns, filters=filters)
    frames: list[pd.DataFrame] = []
    if recent_path is not None and recent_path.exists():
        frames.append(_read_orderbook_index_parquet(recent_path, columns=read_columns, filters=filters))
    if index_path.exists():
        frames.append(_read_orderbook_index_parquet(index_path, columns=read_columns, filters=filters))
    journal_df = load_orderbook_index_journal_frame(
        orderbook_index_journal_path(index_path),
        columns=read_columns,
        filters=filters,
    )
    if not journal_df.empty:
        frames.append(journal_df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    if combined.empty:
        return combined
    if {"captured_ts_ms", "market_id", "token_id", "side"}.issubset(set(combined.columns)):
        combined["captured_ts_ms"] = pd.to_numeric(combined["captured_ts_ms"], errors="coerce")
        combined = combined.dropna(subset=["captured_ts_ms"])
        combined["captured_ts_ms"] = combined["captured_ts_ms"].astype("int64")
        combined = combined.sort_values(["captured_ts_ms", "market_id", "token_id", "side"])
        combined = combined.drop_duplicates(
            subset=["captured_ts_ms", "market_id", "token_id", "side"],
            keep="last",
        ).reset_index(drop=True)
    if requested_columns is not None:
        selected = [column for column in requested_columns if column in combined.columns]
        combined = combined.loc[:, selected].copy()
    return combined


def load_orderbook_index_frame_cached(
    *,
    index_path: Path,
    recent_path: Path | None = None,
    cache: dict[tuple[str, str | None], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    cache_key = (str(index_path), None if recent_path is None else str(recent_path))
    if cache is not None and cache_key in cache:
        return cache[cache_key]
    frame = load_orderbook_index_frame(index_path=index_path, recent_path=recent_path)
    if cache is not None:
        cache[cache_key] = frame
    return frame


def iter_orderbook_index_record_batches(
    *,
    index_path: Path,
    columns: list[str] | tuple[str, ...],
    filters: ParquetFilters | None = None,
    batch_size: int = 100_000,
) -> Iterator[pd.DataFrame]:
    requested_columns = _normalize_columns(columns) or []
    read_columns = _read_columns_for_orderbook_index(requested_columns, filters=filters)
    if not index_path.exists():
        return
    available = _parquet_schema_columns(index_path)
    if filters and available is not None:
        missing_filter_columns = [column for column in _filter_columns(filters) if column not in available]
        if missing_filter_columns:
            return
    selected = [column for column in (read_columns or []) if available is None or column in available]
    try:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(index_path)
        for batch in parquet_file.iter_batches(batch_size=max(1, int(batch_size)), columns=selected or None):
            frame = batch.to_pandas()
            frame = _apply_basic_filters(frame, filters)
            if frame.empty:
                continue
            if requested_columns:
                output_columns = [column for column in requested_columns if column in frame.columns]
                frame = frame.loc[:, output_columns].copy()
            yield frame
    except Exception:
        frame = _read_orderbook_index_parquet(index_path, columns=read_columns, filters=filters)
        if frame.empty:
            return
        if requested_columns:
            output_columns = [column for column in requested_columns if column in frame.columns]
            frame = frame.loc[:, output_columns].copy()
        chunk_size = max(1, int(batch_size))
        for start in range(0, len(frame), chunk_size):
            yield frame.iloc[start : start + chunk_size].copy()


def _normalize_columns(columns: list[str] | tuple[str, ...] | None) -> list[str] | None:
    if columns is None:
        return None
    return list(dict.fromkeys(str(column) for column in columns if str(column)))


def _read_columns_for_orderbook_index(
    requested_columns: list[str] | None,
    *,
    filters: ParquetFilters | None,
) -> list[str] | None:
    if requested_columns is None:
        return None
    required = [*ORDERBOOK_INDEX_DEDUP_COLUMNS, *_filter_columns(filters)]
    return list(dict.fromkeys([*requested_columns, *required]))


def _read_orderbook_index_parquet(
    path: Path,
    *,
    columns: list[str] | None,
    filters: ParquetFilters | None,
) -> pd.DataFrame:
    if columns is None:
        return pd.read_parquet(path, filters=filters)
    available = _parquet_schema_columns(path)
    selected = [column for column in columns if available is None or column in available]
    if filters and available is not None:
        missing_filter_columns = [column for column in _filter_columns(filters) if column not in available]
        if missing_filter_columns:
            return pd.DataFrame(columns=selected)
    return pd.read_parquet(path, columns=selected, filters=filters)


def _parquet_schema_columns(path: Path) -> set[str] | None:
    try:
        import pyarrow.parquet as pq

        return {str(name) for name in pq.ParquetFile(path).schema.names}
    except Exception:
        return None


def _filter_columns(filters: ParquetFilters | None) -> list[str]:
    if not filters:
        return []
    columns: list[str] = []
    for item in filters:
        if isinstance(item, tuple):
            columns.append(str(item[0]))
            continue
        if isinstance(item, list):
            columns.extend(_filter_columns(item))
    return list(dict.fromkeys(columns))


def _apply_basic_filters(frame: pd.DataFrame, filters: ParquetFilters | None) -> pd.DataFrame:
    if frame.empty or not filters:
        return frame
    first = filters[0]
    if isinstance(first, tuple):
        return _apply_filter_group(frame, filters)  # type: ignore[arg-type]
    masks = [
        _filter_group_mask(frame, group)
        for group in filters
        if isinstance(group, list)
    ]
    if not masks:
        return frame
    combined = masks[0]
    for mask in masks[1:]:
        combined = combined | mask
    return frame.loc[combined].copy()


def _apply_filter_group(frame: pd.DataFrame, filters: list[tuple[str, str, object]]) -> pd.DataFrame:
    return frame.loc[_filter_group_mask(frame, filters)].copy()


def _filter_group_mask(frame: pd.DataFrame, filters: list[tuple[str, str, object]]) -> pd.Series:
    mask = pd.Series(True, index=frame.index)
    for column, operator, value in filters:
        column_name = str(column)
        if column_name not in frame.columns:
            return pd.Series(False, index=frame.index)
        series = frame[column_name]
        op = str(operator).strip().lower()
        if op in {"=", "=="}:
            current = series == value
        elif op == "!=":
            current = series != value
        elif op == "in":
            current = series.isin(list(value if isinstance(value, (list, tuple, set, frozenset)) else [value]))
        elif op == "not in":
            current = ~series.isin(list(value if isinstance(value, (list, tuple, set, frozenset)) else [value]))
        elif op in {"<", "<=", ">", ">="}:
            left = pd.to_numeric(series, errors="coerce")
            right = float(value)
            if op == "<":
                current = left < right
            elif op == "<=":
                current = left <= right
            elif op == ">":
                current = left > right
            else:
                current = left >= right
        else:
            current = pd.Series(True, index=frame.index)
        mask = mask & current.fillna(False)
    return mask
