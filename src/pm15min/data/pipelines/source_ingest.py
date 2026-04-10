from __future__ import annotations

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pm15min.core.assets import resolve_asset
from pm15min.core.layout import workspace_root
from ..config import DataConfig
from ..io.parquet import upsert_parquet
from .market_catalog import _frame_from_records
from ..sources.polymarket_gamma import build_market_catalog_records_from_markets
from ..layout import cycle_seconds


STREAM_SOURCE_COLUMNS = [
    "asset",
    "tx_hash",
    "block_number",
    "observation_ts",
    "extra_ts",
    "benchmark_price_raw",
    "price",
    "report_feed_id",
    "requester",
    "path",
    "perform_idx",
    "value_idx",
    "source_file",
    "ingested_at",
]

SETTLEMENT_SOURCE_COLUMNS = [
    "market_id",
    "condition_id",
    "asset",
    "cycle",
    "cycle_start_ts",
    "cycle_end_ts",
    "slug",
    "question",
    "resolution_source",
    "winner_side",
    "label_updown",
    "onchain_resolved",
    "stream_match_exact",
    "full_truth",
    "stream_price",
    "stream_extra_ts",
    "source_file",
    "ingested_at",
]

LEGACY_ORDERBOOK_DEPTH_RE = re.compile(r"orderbook_depth_(\d{8})\.ndjson\.zst$")
LEGACY_MARKET_SNAPSHOT_RE = re.compile(r"_(\d{8}_\d{6})\.(?:csv|json)$")
LEGACY_SETTLEMENT_TRUTH_CYCLE_RE = re.compile(r"polymarket_(\d+m)_settlement_truth\.csv$", re.IGNORECASE)


def _utc_now_label() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _coerce_optional_int_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NA] * len(df), index=df.index, dtype="Int64")
    series = pd.to_numeric(df[column], errors="coerce")
    return pd.Series(series, index=df.index, dtype="Int64")


def _coerce_optional_bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([False] * len(df), index=df.index, dtype=bool)
    series = df[column]
    if series.dtype == bool:
        return series.fillna(False).astype(bool)
    lowered = series.astype(str).str.strip().str.lower()
    return lowered.isin({"1", "true", "t", "yes", "y"})


def _latest_path(paths: list[Path]) -> Path | None:
    if not paths:
        return None
    return max(paths, key=lambda path: path.stat().st_mtime_ns)


def _detect_legacy_settlement_truth_cycles(source_path: Path, df: pd.DataFrame) -> set[str]:
    detected: set[str] = set()
    match = LEGACY_SETTLEMENT_TRUTH_CYCLE_RE.search(source_path.name)
    if match:
        detected.add(str(match.group(1)).strip().lower())

    if "cycle" in df.columns:
        values = {
            str(value).strip().lower()
            for value in df["cycle"].dropna().tolist()
            if str(value).strip()
        }
        detected.update(values)

    if "slug" in df.columns:
        values: set[str] = set()
        for value in df["slug"].dropna().tolist():
            text = str(value).strip().lower()
            if not text:
                continue
            for token in re.split(r"[^0-9a-z]+", text):
                if re.fullmatch(r"\d+m", token):
                    values.add(token)
        detected.update(values)

    return detected


def discover_legacy_streams_csv() -> Path | None:
    root = workspace_root() / "data" / "markets" / "_shared" / "oracle"
    paths = sorted(root.glob("streams_reports_registry_all_*.csv"))
    return _latest_path(paths)


def discover_legacy_settlement_truth_csv(cycle: str = "15m") -> Path | None:
    root = workspace_root() / "data" / "markets" / "_shared" / "oracle"
    paths = [
        path
        for path in root.rglob(f"polymarket_{cycle}_settlement_truth.csv")
        if "parts" not in {part.lower() for part in path.parts}
    ]
    return _latest_path(paths)


def discover_legacy_market_catalog_csv(cfg: DataConfig) -> Path | None:
    root = workspace_root() / "data" / "markets" / cfg.asset.slug / "data" / "polymarket" / "markets" / "all"
    paths = sorted(root.glob(f"*_updown_{cfg.cycle}_markets_*.csv"))
    return _latest_path(paths)


def discover_legacy_orderbook_depth_paths(cfg: DataConfig) -> list[Path]:
    root = workspace_root() / "data" / "markets" / cfg.asset.slug / "data" / "polymarket" / "raw" / "orderbooks_full"
    return sorted(path for path in root.glob("orderbook_depth_*.ndjson.zst") if LEGACY_ORDERBOOK_DEPTH_RE.match(path.name))


def _snapshot_ts_from_legacy_name(path: Path) -> str:
    match = LEGACY_MARKET_SNAPSHOT_RE.search(path.name)
    if match:
        raw = match.group(1)
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}T{raw[9:11]}-{raw[11:13]}-{raw[13:15]}Z"
    dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H-%M-%SZ")


def import_legacy_streams(
    cfg: DataConfig,
    *,
    source_path: Path | None = None,
) -> dict[str, object]:
    source_path = source_path or discover_legacy_streams_csv()
    if source_path is None or not source_path.exists():
        raise FileNotFoundError("Could not locate legacy streams CSV.")

    df = pd.read_csv(source_path, low_memory=False)
    required = {"asset", "extra_ts", "benchmark_price_raw", "tx_hash"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"Legacy streams CSV missing required columns: {missing}")

    out = df.copy()
    out["asset"] = out["asset"].astype(str).str.lower()
    out = out[out["asset"].eq(cfg.asset.slug)].copy()
    if out.empty:
        return {
            "dataset": "chainlink_streams",
            "market": cfg.asset.slug,
            "rows_imported": 0,
            "source_file": str(source_path),
            "partitions_written": 0,
        }

    out["extra_ts"] = pd.to_numeric(out["extra_ts"], errors="coerce")
    out["observation_ts"] = pd.to_numeric(out.get("observation_ts"), errors="coerce")
    out["benchmark_price_raw"] = pd.to_numeric(out["benchmark_price_raw"], errors="coerce")
    out = out.dropna(subset=["extra_ts", "benchmark_price_raw"]).copy()
    out["extra_ts"] = out["extra_ts"].astype("int64")
    out["observation_ts"] = out["observation_ts"].fillna(out["extra_ts"]).astype("int64")
    out["benchmark_price_raw"] = out["benchmark_price_raw"].astype(float)
    out["price"] = out["benchmark_price_raw"] / 1e18
    out["block_number"] = _coerce_optional_int_series(out, "block_number")
    out["perform_idx"] = _coerce_optional_int_series(out, "perform_idx").fillna(-1).astype("int64")
    out["value_idx"] = _coerce_optional_int_series(out, "value_idx").fillna(-1).astype("int64")
    out["report_feed_id"] = out.get("report_feed_id", "").fillna("").astype(str)
    out["requester"] = out.get("requester", "").fillna("").astype(str)
    out["path"] = out.get("path", "").fillna("").astype(str)
    out["source_file"] = str(source_path)
    out["ingested_at"] = _utc_now_label()
    out["year"] = pd.to_datetime(out["extra_ts"], unit="s", utc=True).dt.year.astype(int)
    out["month"] = pd.to_datetime(out["extra_ts"], unit="s", utc=True).dt.month.astype(int)
    out = out[STREAM_SOURCE_COLUMNS + ["year", "month"]]

    partitions_written = 0
    for (year, month), group in out.groupby(["year", "month"], dropna=False):
        target = cfg.layout.streams_partition_path(int(year), int(month))
        upsert_parquet(
            path=target,
            incoming=group[STREAM_SOURCE_COLUMNS].copy(),
            key_columns=["tx_hash", "perform_idx", "value_idx"],
            sort_columns=["extra_ts", "tx_hash", "perform_idx", "value_idx"],
        )
        partitions_written += 1

    return {
        "dataset": "chainlink_streams",
        "market": cfg.asset.slug,
        "rows_imported": int(len(out)),
        "source_file": str(source_path),
        "partitions_written": partitions_written,
        "target_root": str(cfg.layout.streams_source_root),
    }


def import_legacy_market_catalog(
    cfg: DataConfig,
    *,
    source_path: Path | None = None,
) -> dict[str, object]:
    source_path = source_path or discover_legacy_market_catalog_csv(cfg)
    if source_path is None or not source_path.exists():
        raise FileNotFoundError("Could not locate legacy market-catalog CSV.")

    df = pd.read_csv(source_path, low_memory=False)
    snapshot_ts = _snapshot_ts_from_legacy_name(source_path)
    records = build_market_catalog_records_from_markets(
        markets=df.to_dict("records"),
        asset=cfg.asset.slug,
        cycle=cfg.cycle,
        snapshot_ts=snapshot_ts,
    )
    snapshot_df = _frame_from_records(records)
    snapshot_path = cfg.layout.market_catalog_snapshot_path(snapshot_ts)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_df.to_parquet(snapshot_path, index=False)

    canonical_df = upsert_parquet(
        path=cfg.layout.market_catalog_table_path,
        incoming=snapshot_df,
        key_columns=["market_id"],
        sort_columns=["cycle_start_ts", "source_snapshot_ts", "market_id"],
    )
    return {
        "dataset": "market_catalog",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "source_mode": "legacy_market_catalog_csv",
        "source_file": str(source_path),
        "snapshot_rows": int(len(snapshot_df)),
        "canonical_rows": int(len(canonical_df)),
        "snapshot_path": str(snapshot_path),
        "canonical_path": str(cfg.layout.market_catalog_table_path),
    }


def import_legacy_orderbook_depth(
    cfg: DataConfig,
    *,
    source_paths: list[Path] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    overwrite: bool = False,
) -> dict[str, object]:
    selected = list(source_paths or discover_legacy_orderbook_depth_paths(cfg))
    copied: list[str] = []
    skipped_existing: list[str] = []
    skipped_out_of_range: list[str] = []

    date_from_val = str(date_from or "").strip() or None
    date_to_val = str(date_to or "").strip() or None
    for source_path in selected:
        match = LEGACY_ORDERBOOK_DEPTH_RE.match(source_path.name)
        if not match:
            continue
        raw_date = match.group(1)
        date_str = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        if date_from_val and date_str < date_from_val:
            skipped_out_of_range.append(date_str)
            continue
        if date_to_val and date_str > date_to_val:
            skipped_out_of_range.append(date_str)
            continue
        target_path = cfg.layout.orderbook_depth_path(date_str)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists() and not overwrite:
            skipped_existing.append(date_str)
            continue
        shutil.copy2(source_path, target_path)
        copied.append(date_str)

    return {
        "dataset": "orderbook_depth",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "source_mode": "legacy_orderbook_depth_zst",
        "source_root": str(workspace_root() / "data" / "markets" / cfg.asset.slug / "data" / "polymarket" / "raw" / "orderbooks_full"),
        "dates_copied": copied,
        "copied_count": int(len(copied)),
        "skipped_existing": skipped_existing,
        "skipped_out_of_range": skipped_out_of_range,
        "target_root": str(cfg.layout.orderbook_source_root),
    }


def import_legacy_settlement_truth(
    cfg: DataConfig,
    *,
    source_path: Path | None = None,
) -> dict[str, object]:
    source_path = source_path or discover_legacy_settlement_truth_csv(cfg.cycle)
    if source_path is None or not source_path.exists():
        raise FileNotFoundError("Could not locate legacy settlement-truth CSV.")

    df = pd.read_csv(source_path, low_memory=False)
    detected_source_cycles = _detect_legacy_settlement_truth_cycles(source_path, df)
    if len(detected_source_cycles) > 1:
        raise ValueError(
            f"Legacy settlement truth source {source_path} has conflicting cycle hints: "
            f"{sorted(detected_source_cycles)}."
        )
    source_cycle = next(iter(detected_source_cycles)) if detected_source_cycles else None
    if source_cycle is not None and source_cycle != cfg.cycle:
        raise ValueError(
            f"Legacy settlement truth source {source_path} is not compatible with cycle={cfg.cycle}; "
            f"detected cycle={source_cycle}."
        )
    if cfg.cycle != "15m" and source_cycle is None:
        raise ValueError(
            f"Legacy settlement truth source {source_path} is not compatible with cycle={cfg.cycle}; "
            "unable to validate source cycle."
        )
    required = {"asset", "end_ts", "market_id", "winner_side", "label_updown"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise KeyError(f"Legacy settlement truth CSV missing required columns: {missing}")

    out = df.copy()
    out["asset"] = out["asset"].astype(str).str.lower()
    out = out[out["asset"].eq(cfg.asset.slug)].copy()
    if out.empty:
        return {
            "dataset": "settlement_truth",
            "market": cfg.asset.slug,
            "rows_imported": 0,
            "source_file": str(source_path),
            "target_path": str(cfg.layout.settlement_truth_source_path),
        }

    out["cycle_end_ts"] = pd.to_numeric(out["end_ts"], errors="coerce")
    out = out.dropna(subset=["cycle_end_ts"]).copy()
    out["cycle_end_ts"] = out["cycle_end_ts"].astype("int64")
    out["cycle_start_ts"] = out["cycle_end_ts"] - cycle_seconds(cfg.cycle)
    out["cycle"] = cfg.cycle
    out["market_id"] = out["market_id"].astype(str)
    out["condition_id"] = out.get("condition_id", "").fillna("").astype(str)
    out["slug"] = out.get("slug", "").fillna("").astype(str)
    out["question"] = out.get("question", "").fillna("").astype(str)
    out["resolution_source"] = out.get("resolution_source", "").fillna("").astype(str)
    out["winner_side"] = out["winner_side"].fillna("").astype(str).str.upper()
    out["label_updown"] = out["label_updown"].fillna("").astype(str).str.upper()
    out["onchain_resolved"] = _coerce_optional_bool_series(out, "onchain_resolved")
    out["stream_match_exact"] = _coerce_optional_bool_series(out, "stream_match_exact")
    out["full_truth"] = _coerce_optional_bool_series(out, "full_truth")
    out["stream_price"] = pd.to_numeric(out.get("stream_price"), errors="coerce")
    out["stream_extra_ts"] = _coerce_optional_int_series(out, "stream_extra_ts")
    out["source_file"] = str(source_path)
    out["ingested_at"] = _utc_now_label()
    out = out[SETTLEMENT_SOURCE_COLUMNS]

    canonical = upsert_parquet(
        path=cfg.layout.settlement_truth_source_path,
        incoming=out,
        key_columns=["market_id", "cycle_end_ts"],
        sort_columns=["cycle_end_ts", "full_truth", "source_file", "market_id"],
    )
    return {
        "dataset": "settlement_truth",
        "market": cfg.asset.slug,
        "rows_imported": int(len(out)),
        "canonical_rows": int(len(canonical)),
        "source_file": str(source_path),
        "target_path": str(cfg.layout.settlement_truth_source_path),
    }
