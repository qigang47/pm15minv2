from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm5min.data.config import DataConfig
from pm5min.data.service import describe_data_runtime, show_data_summary

from .common import json_ready, read_json_object


SUMMARY_SOURCE_COMPUTED = "computed"
SUMMARY_SOURCE_PERSISTED = "persisted"


def load_data_overview(
    *,
    market: str,
    cycle: str | int = "5m",
    surface: str = "backtest",
    root: Path | None = None,
    prefer_persisted: bool = True,
    now: pd.Timestamp | None = None,
) -> dict[str, object]:
    cfg = DataConfig.build(
        market=market,
        cycle=cycle,
        surface=surface,
        root=root,
    )
    return load_data_overview_from_config(
        cfg,
        prefer_persisted=prefer_persisted,
        now=now,
    )


def load_data_overview_from_config(
    cfg: DataConfig,
    *,
    prefer_persisted: bool = True,
    now: pd.Timestamp | None = None,
) -> dict[str, object]:
    summary_payload = _load_persisted_summary(cfg) if prefer_persisted else None
    summary_source = SUMMARY_SOURCE_PERSISTED if summary_payload is not None else SUMMARY_SOURCE_COMPUTED
    if summary_payload is None:
        summary_payload = show_data_summary(cfg, persist=False, now=now)

    manifest_payload = read_json_object(cfg.layout.latest_summary_manifest_path)
    dataset_rows = build_data_overview_dataset_rows(summary_payload)
    payload: dict[str, object] = {
        "domain": "console",
        "dataset": "console_data_overview",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "summary_source": summary_source,
        "latest_summary_path": str(cfg.layout.latest_summary_path),
        "latest_manifest_path": str(cfg.layout.latest_summary_manifest_path),
        "persisted_summary_exists": cfg.layout.latest_summary_path.exists(),
        "persisted_manifest_exists": cfg.layout.latest_summary_manifest_path.exists(),
        "runtime": describe_data_runtime(cfg),
        "generated_at": summary_payload.get("generated_at"),
        "generated_at_iso": summary_payload.get("generated_at_iso"),
        "summary": dict(summary_payload.get("summary") or {}),
        "audit": dict(summary_payload.get("audit") or {}),
        "completeness": dict(summary_payload.get("completeness") or {}),
        "issues": list(summary_payload.get("issues") or []),
        "datasets": dict(summary_payload.get("datasets") or {}),
        "dataset_rows": dataset_rows,
        "latest_manifest": manifest_payload,
    }
    return json_ready(payload)


def build_data_overview_dataset_rows(summary_payload: dict[str, object]) -> list[dict[str, object]]:
    datasets = summary_payload.get("datasets")
    if not isinstance(datasets, dict):
        return []
    rows: list[dict[str, object]] = []
    for dataset_name in sorted(datasets):
        item = datasets.get(dataset_name)
        if not isinstance(item, dict):
            continue
        rows.append(
            {
                "dataset_name": str(dataset_name),
                "kind": str(item.get("kind") or ""),
                "status": str(item.get("status") or ""),
                "exists": bool(item.get("exists")),
                "location": _resolve_dataset_location(item),
                "row_count": _optional_int(item.get("row_count")),
                "partition_count": _optional_int(item.get("partition_count")),
                "file_count": _optional_int(item.get("file_count")),
                "column_count": _optional_int(item.get("column_count")),
                "duplicate_count": _optional_int(item.get("duplicate_count")),
                "null_key_count": _optional_int(item.get("null_key_count")),
                "time_range": _mapping(item.get("time_range")),
                "freshness_range": _mapping(item.get("freshness_range")),
                "date_range": _mapping(item.get("date_range")),
                "total_bytes": _optional_int(item.get("total_bytes")),
            }
        )
    return json_ready(rows)


def _load_persisted_summary(cfg: DataConfig) -> dict[str, object] | None:
    payload = read_json_object(cfg.layout.latest_summary_path)
    if payload is None:
        return None
    if not _is_matching_summary(payload, cfg=cfg):
        return None
    return payload


def _is_matching_summary(payload: dict[str, object], *, cfg: DataConfig) -> bool:
    market = str(payload.get("market") or "").strip().lower()
    cycle = str(payload.get("cycle") or "").strip().lower()
    surface = str(payload.get("surface") or "").strip().lower()
    return market == cfg.asset.slug and cycle == cfg.cycle and surface == cfg.surface


def _resolve_dataset_location(item: dict[str, object]) -> str:
    for key in ("path", "root"):
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except Exception:
        return None
