from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..config import DataConfig
from .shared import duplicate_count, null_key_count, time_range


def build_data_datasets(cfg: DataConfig) -> dict[str, dict[str, Any]]:
    return {
        "binance_klines_1m_source": summarize_single_parquet_dataset(
            path=cfg.layout.binance_klines_path(),
            key_columns=["open_time"],
            time_columns=["open_time"],
            freshness_columns=["open_time"],
        ),
        "market_catalog_table": summarize_single_parquet_dataset(
            path=cfg.layout.market_catalog_table_path,
            key_columns=["market_id"],
            time_columns=["cycle_start_ts", "cycle_end_ts"],
            freshness_columns=["source_snapshot_ts", "cycle_end_ts"],
        ),
        "direct_oracle_source": summarize_single_parquet_dataset(
            path=cfg.layout.direct_oracle_source_path,
            key_columns=["asset", "cycle_start_ts"],
            time_columns=["cycle_start_ts", "cycle_end_ts"],
            freshness_columns=["fetched_at", "api_timestamp_ms", "cycle_end_ts"],
        ),
        "settlement_truth_source": summarize_single_parquet_dataset(
            path=cfg.layout.settlement_truth_source_path,
            key_columns=["market_id", "cycle_end_ts"],
            time_columns=["cycle_start_ts", "cycle_end_ts"],
            freshness_columns=["ingested_at", "cycle_end_ts"],
        ),
        "oracle_prices_table": summarize_single_parquet_dataset(
            path=cfg.layout.oracle_prices_table_path,
            key_columns=["asset", "cycle_start_ts"],
            time_columns=["cycle_start_ts", "cycle_end_ts"],
        ),
        "truth_table": summarize_single_parquet_dataset(
            path=cfg.layout.truth_table_path,
            key_columns=["asset", "cycle_end_ts"],
            time_columns=["cycle_start_ts", "cycle_end_ts"],
        ),
        "chainlink_streams_source": summarize_partitioned_parquet_dataset(
            root=cfg.layout.streams_source_root,
            key_columns=["tx_hash", "perform_idx", "value_idx"],
            time_columns=["observation_ts", "extra_ts"],
            freshness_columns=["ingested_at", "observation_ts", "extra_ts"],
        ),
        "chainlink_datafeeds_source": summarize_partitioned_parquet_dataset(
            root=cfg.layout.datafeeds_source_root,
            key_columns=["tx_hash", "log_index"],
            time_columns=["updated_at"],
            freshness_columns=["ingested_at", "updated_at"],
        ),
        "orderbook_index_table": summarize_partitioned_parquet_dataset(
            root=cfg.layout.tables_root / "orderbook_index" / f"cycle={cfg.cycle}" / f"asset={cfg.asset.slug}",
            key_columns=["captured_ts_ms", "market_id", "token_id", "side"],
            time_columns=["captured_ts_ms"],
            freshness_columns=["captured_ts_ms"],
        ),
        "orderbook_depth_source": summarize_ndjson_partitions(
            root=cfg.layout.orderbook_source_root,
            pattern="depth.ndjson.zst",
        ),
    }


def summarize_single_parquet_dataset(
    *,
    path: Path,
    key_columns: list[str],
    time_columns: list[str],
    freshness_columns: list[str] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "kind": "single_parquet",
        "path": str(path),
        "exists": path.exists(),
    }
    if not path.exists():
        payload["status"] = "missing"
        return payload
    df = pd.read_parquet(path)
    row_count = int(len(df))
    payload.update(
        {
            "status": "ok" if row_count > 0 else "empty",
            "row_count": row_count,
            "column_count": int(len(df.columns)),
            "columns": [str(column) for column in df.columns],
            "null_key_count": null_key_count(df, key_columns=key_columns),
            "duplicate_count": duplicate_count(df, key_columns=key_columns),
            "time_range": time_range(df, columns=time_columns),
            "freshness_range": time_range(df, columns=freshness_columns or time_columns),
        }
    )
    return payload


def summarize_partitioned_parquet_dataset(
    *,
    root: Path,
    key_columns: list[str],
    time_columns: list[str],
    freshness_columns: list[str] | None = None,
) -> dict[str, Any]:
    files = sorted(root.rglob("data.parquet")) if root.exists() else []
    payload: dict[str, Any] = {
        "kind": "partitioned_parquet",
        "root": str(root),
        "exists": bool(files),
        "file_count": int(len(files)),
    }
    if not files:
        payload["status"] = "missing"
        return payload
    row_count = 0
    duplicate_keys = 0
    null_keys = 0
    min_ts = None
    max_ts = None
    freshness_min_ts = None
    freshness_max_ts = None
    for path in files:
        df = pd.read_parquet(path)
        row_count += int(len(df))
        duplicate_keys += duplicate_count(df, key_columns=key_columns)
        null_keys += null_key_count(df, key_columns=key_columns)
        item_time_range = time_range(df, columns=time_columns)
        item_freshness_range = time_range(df, columns=freshness_columns or time_columns)
        if item_time_range["min"] is not None:
            min_ts = item_time_range["min"] if min_ts is None else min(min_ts, item_time_range["min"])
        if item_time_range["max"] is not None:
            max_ts = item_time_range["max"] if max_ts is None else max(max_ts, item_time_range["max"])
        if item_freshness_range["min"] is not None:
            freshness_min_ts = (
                item_freshness_range["min"]
                if freshness_min_ts is None
                else min(freshness_min_ts, item_freshness_range["min"])
            )
        if item_freshness_range["max"] is not None:
            freshness_max_ts = (
                item_freshness_range["max"]
                if freshness_max_ts is None
                else max(freshness_max_ts, item_freshness_range["max"])
            )
    payload.update(
        {
            "status": "ok" if row_count > 0 else "empty",
            "row_count": int(row_count),
            "partition_count": int(len(files)),
            "null_key_count": int(null_keys),
            "duplicate_count": int(duplicate_keys),
            "time_range": {"min": min_ts, "max": max_ts},
            "freshness_range": {"min": freshness_min_ts, "max": freshness_max_ts},
        }
    )
    return payload


def summarize_ndjson_partitions(*, root: Path, pattern: str) -> dict[str, Any]:
    files = sorted(root.rglob(pattern)) if root.exists() else []
    payload: dict[str, Any] = {
        "kind": "partitioned_ndjson_zst",
        "root": str(root),
        "exists": bool(files),
        "file_count": int(len(files)),
    }
    if not files:
        payload["status"] = "missing"
        return payload
    dates: list[str] = []
    total_bytes = 0
    for path in files:
        total_bytes += int(path.stat().st_size)
        parent = path.parent.name
        if parent.startswith("date="):
            dates.append(parent.split("=", 1)[1])
    payload.update(
        {
            "status": "ok",
            "partition_count": int(len(files)),
            "total_bytes": int(total_bytes),
            "date_range": {
                "min": min(dates) if dates else None,
                "max": max(dates) if dates else None,
            },
        }
    )
    return payload
