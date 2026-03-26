from __future__ import annotations

from contextlib import contextmanager
import fcntl
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.queries.loaders import load_market_catalog


MIN_DEPTH_BYTES_FOR_INDEX_SANITY = 1_000_000
MIN_INDEX_ROWS_FOR_LARGE_DEPTH = 1_000


def load_market_catalog_with_fallback(data_cfg: DataConfig) -> pd.DataFrame:
    primary = load_market_catalog(data_cfg)
    if not primary.empty or data_cfg.surface == "live":
        return primary
    return load_market_catalog(live_surface_cfg(data_cfg))


def resolve_orderbook_depth_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary = data_cfg.layout.orderbook_depth_path(date_str)
    if primary.exists() or data_cfg.surface == "live":
        return primary
    return live_surface_cfg(data_cfg).layout.orderbook_depth_path(date_str)


def resolve_orderbook_index_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary = data_cfg.layout.orderbook_index_path(date_str)
    if primary.exists() or data_cfg.surface == "live":
        return primary
    return live_surface_cfg(data_cfg).layout.orderbook_index_path(date_str)


def ensure_orderbook_index_path(data_cfg: DataConfig, date_str: str) -> Path:
    primary_index_path = data_cfg.layout.orderbook_index_path(date_str)
    primary_depth_path = data_cfg.layout.orderbook_depth_path(date_str)
    if primary_index_path.exists():
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
    if primary_depth.exists() or data_cfg.surface == "live":
        return data_cfg
    return live_surface_cfg(data_cfg)


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


def _rebuild_orderbook_index(data_cfg: DataConfig, *, date_str: str) -> None:
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
            return
        build_orderbook_index_from_depth(data_cfg, date_str=date_str)


@contextmanager
def _exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
