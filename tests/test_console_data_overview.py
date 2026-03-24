from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.console.read_models.data_overview import (
    SUMMARY_SOURCE_COMPUTED,
    SUMMARY_SOURCE_PERSISTED,
    build_data_overview_dataset_rows,
    load_data_overview,
)
from pm15min.data.config import DataConfig


def test_build_data_overview_dataset_rows_flattens_dataset_payload() -> None:
    rows = build_data_overview_dataset_rows(
        {
            "datasets": {
                "orderbook_depth_source": {
                    "kind": "partitioned_ndjson_zst",
                    "status": "ok",
                    "exists": True,
                    "root": "/tmp/orderbooks",
                    "partition_count": 3,
                    "date_range": {"min": "2026-03-20", "max": "2026-03-22"},
                    "total_bytes": 128,
                },
                "truth_table": {
                    "kind": "single_parquet",
                    "status": "ok",
                    "exists": True,
                    "path": "/tmp/truth.parquet",
                    "row_count": 42,
                    "column_count": 8,
                    "duplicate_count": 0,
                    "null_key_count": 0,
                    "time_range": {"min": "2026-03-01T00:00:00+00:00", "max": "2026-03-02T00:00:00+00:00"},
                    "freshness_range": {"min": "2026-03-02T00:00:00+00:00", "max": "2026-03-02T00:00:00+00:00"},
                },
            }
        }
    )

    assert [row["dataset_name"] for row in rows] == ["orderbook_depth_source", "truth_table"]
    assert rows[0]["location"] == "/tmp/orderbooks"
    assert rows[0]["partition_count"] == 3
    assert rows[1]["location"] == "/tmp/truth.parquet"
    assert rows[1]["row_count"] == 42
    assert rows[1]["time_range"]["min"] == "2026-03-01T00:00:00+00:00"


def test_load_data_overview_prefers_matching_persisted_summary(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    persisted = {
        "domain": "data",
        "dataset": "data_surface_summary",
        "market": "sol",
        "cycle": "15m",
        "surface": "backtest",
        "generated_at": "2026-03-23T00-00-00Z",
        "generated_at_iso": "2026-03-23T00:00:00+00:00",
        "summary": {"dataset_count": 2},
        "audit": {"status": "ok"},
        "completeness": {"status": "ok"},
        "issues": [],
        "datasets": {
            "truth_table": {
                "kind": "single_parquet",
                "status": "ok",
                "exists": True,
                "path": "/tmp/truth.parquet",
                "row_count": 10,
                "column_count": 4,
                "duplicate_count": 0,
                "null_key_count": 0,
                "time_range": {"min": "2026-03-01T00:00:00+00:00", "max": "2026-03-01T01:00:00+00:00"},
                "freshness_range": {"min": "2026-03-01T01:00:00+00:00", "max": "2026-03-01T01:00:00+00:00"},
            }
        },
    }
    cfg.layout.latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.layout.latest_summary_path.write_text(json.dumps(persisted, indent=2), encoding="utf-8")
    cfg.layout.latest_summary_manifest_path.write_text(
        json.dumps({"object_type": "data_summary_manifest", "path": str(cfg.layout.latest_summary_path)}, indent=2),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "pm15min.console.read_models.data_overview.show_data_summary",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("should not recompute summary")),
    )

    payload = load_data_overview(market="sol", cycle="15m", surface="backtest", root=root)

    assert payload["summary_source"] == SUMMARY_SOURCE_PERSISTED
    assert payload["generated_at"] == "2026-03-23T00-00-00Z"
    assert payload["summary"]["dataset_count"] == 2
    assert payload["dataset_rows"][0]["dataset_name"] == "truth_table"
    assert payload["latest_manifest"]["object_type"] == "data_summary_manifest"


def test_load_data_overview_falls_back_to_computed_summary_when_persisted_missing_or_mismatched(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    cfg.layout.latest_summary_path.parent.mkdir(parents=True, exist_ok=True)
    cfg.layout.latest_summary_path.write_text(
        json.dumps(
            {
                "market": "btc",
                "cycle": "15m",
                "surface": "backtest",
                "datasets": {},
            }
        ),
        encoding="utf-8",
    )
    computed_payload = {
        "domain": "data",
        "dataset": "data_surface_summary",
        "market": "sol",
        "cycle": "15m",
        "surface": "backtest",
        "generated_at": "2026-03-23T00-05-00Z",
        "generated_at_iso": "2026-03-23T00:05:00+00:00",
        "summary": {"dataset_count": 1},
        "audit": {"status": "warning"},
        "completeness": {"status": "warning"},
        "issues": [{"code": "truth_table_missing"}],
        "datasets": {
            "orderbook_depth_source": {
                "kind": "partitioned_ndjson_zst",
                "status": "ok",
                "exists": True,
                "root": str(root / "data" / "backtest" / "sources" / "polymarket" / "orderbooks"),
                "partition_count": 2,
                "date_range": {"min": "2026-03-20", "max": "2026-03-21"},
                "total_bytes": 256,
            }
        },
    }
    monkeypatch.setattr(
        "pm15min.console.read_models.data_overview.show_data_summary",
        lambda built_cfg, persist=False, now=None: computed_payload,
    )

    payload = load_data_overview(
        market="sol",
        cycle="15m",
        surface="backtest",
        root=root,
        now=pd.Timestamp("2026-03-23T00:05:00Z"),
    )

    assert payload["summary_source"] == SUMMARY_SOURCE_COMPUTED
    assert payload["summary"]["dataset_count"] == 1
    assert payload["issues"][0]["code"] == "truth_table_missing"
    assert payload["dataset_rows"][0]["dataset_name"] == "orderbook_depth_source"
    assert payload["dataset_rows"][0]["date_range"]["max"] == "2026-03-21"
