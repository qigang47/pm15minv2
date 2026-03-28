from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path
from typing import Sequence

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import iter_ndjson_zst
from pm15min.data.queries.loaders import load_market_catalog


MIN_DEPTH_BYTES_FOR_INDEX_SANITY = 1_000_000
MIN_INDEX_ROWS_FOR_LARGE_DEPTH = 1_000
TINY_DEPTH_BYTES_FOR_EMPTY_SOURCE = 64


def load_market_catalog_with_fallback(data_cfg: DataConfig) -> pd.DataFrame:
    primary = load_market_catalog(data_cfg)
    if not primary.empty or data_cfg.surface == "live":
        return primary
    return load_market_catalog(live_surface_cfg(data_cfg))


def resolve_orderbook_depth_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary = data_cfg.layout.orderbook_depth_path(date_str)
    if data_cfg.surface == "live":
        return primary
    if primary.exists() and not _depth_source_is_definitely_empty(primary):
        return primary
    live_path = live_surface_cfg(data_cfg).layout.orderbook_depth_path(date_str)
    if live_path.exists() and not _depth_source_is_definitely_empty(live_path):
        return live_path
    return primary


def resolve_orderbook_index_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary = data_cfg.layout.orderbook_index_path(date_str)
    if data_cfg.surface == "live":
        return primary
    primary_depth = data_cfg.layout.orderbook_depth_path(date_str)
    if primary.exists() and not _depth_source_is_definitely_empty(primary_depth):
        return primary
    live_cfg = live_surface_cfg(data_cfg)
    live_index = live_cfg.layout.orderbook_index_path(date_str)
    live_depth = live_cfg.layout.orderbook_depth_path(date_str)
    if live_index.exists() and not _depth_source_is_definitely_empty(live_depth):
        return live_index
    return primary


def ensure_orderbook_index_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary_index_path = data_cfg.layout.orderbook_index_path(date_str)
    primary_depth_path = data_cfg.layout.orderbook_depth_path(date_str)
    if primary_index_path.exists() and not _depth_source_is_definitely_empty(primary_depth_path):
        if _should_rebuild_index(index_path=primary_index_path, depth_path=primary_depth_path):
            _rebuild_orderbook_index(data_cfg, date_str=date_str)
        return primary_index_path
    source_cfg = _orderbook_source_cfg(data_cfg, date_str)
    index_path = source_cfg.layout.orderbook_index_path(date_str)
    depth_path = source_cfg.layout.orderbook_depth_path(date_str)
    if not depth_path.exists():
        return index_path
    if _should_rebuild_index(index_path=index_path, depth_path=depth_path):
        _rebuild_orderbook_index(source_cfg, date_str=date_str)
    return index_path


def preflight_orderbook_index_dates(
    data_cfg: DataConfig,
    *,
    date_strings: Sequence[str],
    expected_market_ids_by_date: dict[str, set[str]] | None = None,
) -> dict[str, object]:
    requested_dates = sorted({str(value).strip() for value in date_strings if str(value).strip()})
    catalog_market_ids_by_date = _catalog_market_ids_by_date(data_cfg)
    details = [
        _inspect_orderbook_index_date(
            data_cfg,
            date_str=date_str,
            expected_market_ids=(
                expected_market_ids_by_date.get(date_str, set())
                if expected_market_ids_by_date is not None and date_str in expected_market_ids_by_date
                else catalog_market_ids_by_date.get(date_str, set())
            ),
        )
        for date_str in requested_dates
    ]
    status_counts: dict[str, int] = {}
    for detail in details:
        status = str(detail.get("status") or "")
        status_counts[status] = int(status_counts.get(status, 0)) + 1

    ready_statuses = {"ready", "rebuilt", "refreshed"}
    ready_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) in ready_statuses]
    rebuilt_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) == "rebuilt"]
    refreshed_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) == "refreshed"]
    missing_depth_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) == "missing_depth"]
    empty_depth_source_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) == "empty_depth_source"]
    partial_market_coverage_dates = [str(detail["date"]) for detail in details if str(detail.get("status")) == "partial_market_coverage"]
    index_missing_dates = [
        str(detail["date"])
        for detail in details
        if str(detail.get("status")) in {"index_missing", "index_missing_after_rebuild", "empty_index", "index_rebuild_error"}
    ]
    used_live_surface_dates = [str(detail["date"]) for detail in details if str(detail.get("source_surface")) == "live"]

    return {
        "requested_dates": requested_dates,
        "requested_date_count": int(len(requested_dates)),
        "checked_date_count": int(len(details)),
        "status_counts": status_counts,
        "ready_dates": ready_dates,
        "ready_date_count": int(len(ready_dates)),
        "rebuilt_dates": rebuilt_dates,
        "rebuilt_date_count": int(len(rebuilt_dates)),
        "refreshed_dates": refreshed_dates,
        "refreshed_date_count": int(len(refreshed_dates)),
        "missing_depth_dates": missing_depth_dates,
        "missing_depth_date_count": int(len(missing_depth_dates)),
        "empty_depth_source_dates": empty_depth_source_dates,
        "empty_depth_source_date_count": int(len(empty_depth_source_dates)),
        "partial_market_coverage_dates": partial_market_coverage_dates,
        "partial_market_coverage_date_count": int(len(partial_market_coverage_dates)),
        "index_missing_dates": index_missing_dates,
        "index_missing_date_count": int(len(index_missing_dates)),
        "used_live_surface_dates": used_live_surface_dates,
        "used_live_surface_date_count": int(len(used_live_surface_dates)),
        "details": details,
    }


def live_surface_cfg(data_cfg: DataConfig) -> DataConfig:
    if data_cfg.surface == "live":
        return data_cfg
    root = data_cfg.layout.storage.data_root.parent
    return DataConfig.build(
        market=data_cfg.asset.slug,
        cycle=data_cfg.cycle,
        surface="live",
        root=root,
    )


def _orderbook_source_cfg(data_cfg: DataConfig, date_str: str) -> DataConfig:
    primary_depth = data_cfg.layout.orderbook_depth_path(date_str)
    if data_cfg.surface == "live":
        return data_cfg
    if primary_depth.exists() and not _depth_source_is_definitely_empty(primary_depth):
        return data_cfg
    live_cfg = live_surface_cfg(data_cfg)
    live_depth = live_cfg.layout.orderbook_depth_path(date_str)
    if live_depth.exists() and not _depth_source_is_definitely_empty(live_depth):
        return live_cfg
    return data_cfg


def _should_rebuild_index(*, index_path: Path, depth_path: Path) -> bool:
    if not depth_path.exists():
        return False
    if not index_path.exists():
        return True
    try:
        if float(depth_path.stat().st_mtime) > float(index_path.stat().st_mtime):
            return True
    except Exception:
        return False
    return _index_is_suspiciously_sparse(index_path=index_path, depth_path=depth_path)


def _inspect_orderbook_index_date(
    data_cfg: DataConfig,
    *,
    date_str: str,
    expected_market_ids: set[str] | None = None,
) -> dict[str, object]:
    source_cfg = _orderbook_source_cfg(data_cfg, date_str)
    depth_path = source_cfg.layout.orderbook_depth_path(date_str)
    index_path = source_cfg.layout.orderbook_index_path(date_str)
    depth_exists = depth_path.exists()
    index_exists_before = index_path.exists()
    depth_bytes = _safe_file_size(depth_path)
    detail: dict[str, object] = {
        "date": str(date_str),
        "requested_surface": str(data_cfg.surface),
        "source_surface": str(source_cfg.surface),
        "depth_path": str(depth_path),
        "index_path": str(index_path),
        "depth_exists": bool(depth_exists),
        "index_exists_before": bool(index_exists_before),
        "depth_bytes": depth_bytes,
        "expected_market_id_count": int(len(expected_market_ids or set())),
    }
    if not depth_exists:
        detail["status"] = "missing_depth"
        detail["index_exists_after"] = bool(index_exists_before)
        detail["index_row_count"] = _safe_index_row_count(index_path)
        return detail

    if _depth_source_is_definitely_empty(depth_path):
        detail["status"] = "empty_depth_source"
        detail["index_exists_after"] = bool(index_exists_before)
        detail["index_row_count"] = _safe_index_row_count(index_path)
        return detail

    rebuild_needed = _should_rebuild_index(index_path=index_path, depth_path=depth_path)
    detail["rebuild_needed"] = bool(rebuild_needed)
    rebuild_action = "refresh" if index_exists_before else "build"
    if rebuild_needed:
        try:
            rebuild_result = _rebuild_orderbook_index(source_cfg, date_str=date_str)
            if rebuild_result is not None:
                detail["rebuild_result"] = dict(rebuild_result)
                detail["rows_written"] = int(rebuild_result.get("rows_written", 0))
                detail["rows_parsed"] = int(rebuild_result.get("rows_parsed", 0))
                detail["rows_skipped"] = int(rebuild_result.get("rows_skipped", 0))
        except Exception as exc:
            detail["status"] = "index_rebuild_error"
            detail["error"] = f"{type(exc).__name__}: {exc}"
            detail["index_exists_after"] = bool(index_path.exists())
            detail["index_row_count"] = _safe_index_row_count(index_path)
            return detail

    index_exists_after = index_path.exists()
    index_row_count = _safe_index_row_count(index_path)
    detail["index_exists_after"] = bool(index_exists_after)
    detail["index_row_count"] = int(index_row_count)
    observed_market_ids = _safe_index_market_ids(index_path)
    detail["index_market_id_count"] = int(len(observed_market_ids))
    missing_market_ids = sorted((expected_market_ids or set()) - observed_market_ids)
    detail["missing_market_id_count"] = int(len(missing_market_ids))
    if missing_market_ids:
        detail["missing_market_id_samples"] = missing_market_ids[:10]
    if index_row_count > 0:
        if missing_market_ids:
            detail["status"] = "partial_market_coverage"
            return detail
        if rebuild_needed and rebuild_action == "refresh":
            detail["status"] = "refreshed"
        elif rebuild_needed:
            detail["status"] = "rebuilt"
        else:
            detail["status"] = "ready"
        return detail

    depth_has_raw_records = _depth_source_has_raw_records(depth_path)
    detail["depth_has_raw_records"] = bool(depth_has_raw_records)
    if not depth_has_raw_records:
        detail["status"] = "empty_depth_source"
    elif index_exists_after:
        detail["status"] = "empty_index"
    elif rebuild_needed:
        detail["status"] = "index_missing_after_rebuild"
    else:
        detail["status"] = "index_missing"
    return detail


def _index_is_suspiciously_sparse(*, index_path: Path, depth_path: Path) -> bool:
    try:
        if float(depth_path.stat().st_size) < float(MIN_DEPTH_BYTES_FOR_INDEX_SANITY):
            return False
    except Exception:
        return False
    try:
        row_count = int(len(pd.read_parquet(index_path, columns=["captured_ts_ms"])))
    except Exception:
        return True
    return row_count < int(MIN_INDEX_ROWS_FOR_LARGE_DEPTH)


def _safe_index_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(len(pd.read_parquet(path, columns=["captured_ts_ms"])))
    except Exception:
        return 0


def _safe_index_market_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        frame = pd.read_parquet(path, columns=["market_id"])
    except Exception:
        return set()
    values = frame.get("market_id", pd.Series(dtype="string")).astype("string").fillna("")
    return {str(value) for value in values.tolist() if str(value)}


def _safe_file_size(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return int(path.stat().st_size)
    except Exception:
        return 0


def _depth_source_is_definitely_empty(depth_path: Path) -> bool:
    return _safe_file_size(depth_path) <= int(TINY_DEPTH_BYTES_FOR_EMPTY_SOURCE)


def _catalog_market_ids_by_date(data_cfg: DataConfig) -> dict[str, set[str]]:
    table = load_market_catalog_with_fallback(data_cfg)
    if table.empty or "cycle_start_ts" not in table.columns or "market_id" not in table.columns:
        return {}
    out = table.copy()
    numeric = pd.to_numeric(out["cycle_start_ts"], errors="coerce")
    out["cycle_start_ts"] = pd.to_datetime(numeric, unit="s", utc=True, errors="coerce")
    out["market_id"] = out["market_id"].astype("string").fillna("")
    out = out.loc[out["cycle_start_ts"].notna() & out["market_id"].ne("")].copy()
    if out.empty:
        return {}
    dates = out["cycle_start_ts"].dt.strftime("%Y-%m-%d")
    grouped = out.groupby(dates, dropna=False, sort=False)["market_id"]
    return {
        str(date_str): {str(value) for value in series.astype("string").tolist() if str(value)}
        for date_str, series in grouped
    }


def _depth_source_has_raw_records(depth_path: Path) -> bool:
    if not depth_path.exists():
        return False
    if _depth_source_is_definitely_empty(depth_path):
        return False
    try:
        iterator = iter_ndjson_zst(depth_path)
        next(iterator, None)
        return True
    except Exception:
        return False


def _rebuild_orderbook_index(data_cfg: DataConfig, *, date_str: str) -> dict[str, object] | None:
    from pm15min.data.pipelines.orderbook_recording import build_orderbook_index_from_depth

    lock_path = (
        data_cfg.layout.surface_var_root
        / "locks"
        / "orderbook_index"
        / f"{data_cfg.asset.slug}-{data_cfg.cycle}-{date_str}.lock"
    )
    with _exclusive_lock(lock_path):
        index_path = data_cfg.layout.orderbook_index_path(date_str)
        depth_path = data_cfg.layout.orderbook_depth_path(date_str)
        if not _should_rebuild_index(index_path=index_path, depth_path=depth_path):
            return None
        return build_orderbook_index_from_depth(data_cfg, date_str=date_str)


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
