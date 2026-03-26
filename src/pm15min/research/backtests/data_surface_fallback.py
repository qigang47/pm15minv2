from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.queries.loaders import load_market_catalog


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
