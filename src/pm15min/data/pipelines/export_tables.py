from __future__ import annotations

from pathlib import Path

import pandas as pd

from ..config import DataConfig


def _export_table_to_csv(source_path: Path, export_path: Path) -> dict[str, object]:
    if not source_path.exists():
        raise FileNotFoundError(f"Missing canonical table: {source_path}")
    df = pd.read_parquet(source_path)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(export_path, index=False)
    return {
        "rows_written": int(len(df)),
        "source_path": str(source_path),
        "export_path": str(export_path),
    }


def export_oracle_prices_15m(cfg: DataConfig) -> dict[str, object]:
    summary = _export_table_to_csv(cfg.layout.oracle_prices_table_path, cfg.layout.oracle_prices_export_path)
    return {
        "dataset": f"oracle_prices_{cfg.cycle}_export",
        "market": cfg.asset.slug,
        **summary,
    }


def export_truth_15m(cfg: DataConfig) -> dict[str, object]:
    summary = _export_table_to_csv(cfg.layout.truth_table_path, cfg.layout.truth_export_path)
    return {
        "dataset": f"truth_{cfg.cycle}_export",
        "market": cfg.asset.slug,
        **summary,
    }
