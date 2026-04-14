from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from ..config import DataConfig
from ..io.json_files import write_json_atomic
from ..layout import utc_snapshot_label
from .audit import build_data_audit
from .datasets import build_data_datasets
from .reporting import (
    build_data_completeness_report,
    build_data_issue_inventory,
    build_data_summary_manifest,
)
from .shared import normalize_utc_timestamp


def show_data_summary(
    cfg: DataConfig,
    *,
    persist: bool = False,
    now: pd.Timestamp | None = None,
) -> dict[str, object]:
    audit_now = normalize_utc_timestamp(now)
    generated_at = utc_snapshot_label(audit_now.to_pydatetime())
    datasets = build_data_datasets(cfg)
    ok_count = sum(1 for item in datasets.values() if bool(item.get("exists")))
    audit = build_data_audit(cfg=cfg, datasets=datasets, now=audit_now)
    issues = build_data_issue_inventory(datasets=datasets, audit=audit)
    completeness = build_data_completeness_report(datasets=datasets, audit=audit, issues=issues)
    payload: dict[str, object] = {
        "domain": "data",
        "dataset": "data_surface_summary",
        "generated_at": generated_at,
        "generated_at_iso": audit_now.isoformat(),
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "surface_data_root": str(cfg.layout.surface_data_root),
        "summary": {
            "dataset_count": len(datasets),
            "existing_dataset_count": ok_count,
            "missing_dataset_count": len(datasets) - ok_count,
        },
        "audit": audit,
        "completeness": completeness,
        "issues": issues,
        "datasets": datasets,
    }
    if persist:
        paths = persist_data_summary(cfg=cfg, payload=payload)
        payload["latest_summary_path"] = str(paths["latest"])
        payload["summary_snapshot_path"] = str(paths["snapshot"])
        payload["latest_manifest_path"] = str(paths["latest_manifest"])
        payload["manifest_snapshot_path"] = str(paths["manifest_snapshot"])
    return payload


def describe_data_runtime(cfg: DataConfig) -> dict[str, object]:
    return {
        "domain": "data",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "surface": cfg.surface,
        "poll_interval_sec": cfg.poll_interval_sec,
        "orderbook_timeout_sec": cfg.orderbook_timeout_sec,
        "market_depth": cfg.market_depth,
        "data_root": str(cfg.layout.surface_data_root),
        "sources_root": str(cfg.layout.sources_root),
        "tables_root": str(cfg.layout.tables_root),
        "exports_root": str(cfg.layout.exports_root),
        "var_root": str(cfg.layout.surface_var_root),
        "market_catalog_snapshot_root": str(cfg.layout.market_catalog_snapshot_root),
        "market_catalog_table_path": str(cfg.layout.market_catalog_table_path),
        "direct_oracle_source_path": str(cfg.layout.direct_oracle_source_path),
        "orderbook_source_root": str(cfg.layout.orderbook_source_root),
        "orderbook_state_path": str(cfg.layout.orderbook_state_path),
        "foundation_state_path": str(cfg.layout.foundation_state_path),
        "foundation_log_path": str(cfg.layout.foundation_log_path),
    }


def persist_data_summary(*, cfg: DataConfig, payload: dict[str, Any]) -> dict[str, Path]:
    latest_path = cfg.layout.latest_summary_path
    latest_manifest_path = cfg.layout.latest_summary_manifest_path
    snapshot_path = cfg.layout.summary_snapshot_path(str(payload["generated_at"]))
    manifest_snapshot_path = cfg.layout.summary_manifest_snapshot_path(str(payload["generated_at"]))
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    latest_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = build_data_summary_manifest(cfg=cfg, payload=payload)
    write_json_atomic(payload, latest_path)
    write_json_atomic(payload, snapshot_path)
    write_json_atomic(manifest, latest_manifest_path)
    write_json_atomic(manifest, manifest_snapshot_path)
    return {
        "latest": latest_path,
        "snapshot": snapshot_path,
        "latest_manifest": latest_manifest_path,
        "manifest_snapshot": manifest_snapshot_path,
    }
