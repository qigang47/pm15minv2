from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..config import DataConfig


def _concat_parquets(paths: list[Path]) -> pd.DataFrame:
    frames = [pd.read_parquet(path) for path in paths if path.exists()]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)


def load_market_catalog(cfg: DataConfig) -> pd.DataFrame:
    path = cfg.layout.market_catalog_table_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def load_binance_klines_1m(
    cfg: DataConfig,
    symbol: str | None = None,
    *,
    columns: list[str] | tuple[str, ...] | None = None,
    filters: list[tuple[str, str, object]] | None = None,
) -> pd.DataFrame:
    path = cfg.layout.binance_klines_path(symbol=symbol)
    if not path.exists():
        return pd.DataFrame()
    kwargs: dict[str, object] = {}
    if columns is not None:
        kwargs["columns"] = list(columns)
    if filters:
        kwargs["filters"] = filters
    return pd.read_parquet(path, **kwargs)


def load_streams_source(cfg: DataConfig) -> pd.DataFrame:
    paths = sorted(cfg.layout.streams_source_root.glob("year=*/month=*/data.parquet"))
    return _concat_parquets(paths)


def load_datafeeds_source(cfg: DataConfig) -> pd.DataFrame:
    paths = sorted(cfg.layout.datafeeds_source_root.glob("year=*/month=*/data.parquet"))
    return _concat_parquets(paths)


def load_settlement_truth_source(cfg: DataConfig) -> pd.DataFrame:
    path = cfg.layout.settlement_truth_source_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def load_direct_oracle_source(cfg: DataConfig) -> pd.DataFrame:
    path = cfg.layout.direct_oracle_source_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def load_oracle_prices_table(cfg: DataConfig) -> pd.DataFrame:
    path = cfg.layout.oracle_prices_table_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()


def load_truth_table(cfg: DataConfig) -> pd.DataFrame:
    path = cfg.layout.truth_table_path
    return pd.read_parquet(path) if path.exists() else pd.DataFrame()
