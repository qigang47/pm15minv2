from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.service import persist_data_summary, show_data_summary


def test_show_data_summary_reports_core_dataset_stats(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    audit_now = pd.Timestamp("2026-03-19T00:20:00Z")
    cycle_start_ts = int(pd.Timestamp("2026-03-19T00:00:00Z").timestamp())
    cycle_end_ts = int(pd.Timestamp("2026-03-19T00:15:00Z").timestamp())
    captured_ts_ms = int(pd.Timestamp("2026-03-19T00:00:00Z").timestamp() * 1000)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-19T00:00:00Z"),
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                },
                {
                    "open_time": pd.Timestamp("2026-03-19T00:01:00Z"),
                    "open": 1.05,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.1,
                },
            ]
        ),
        cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": cycle_start_ts,
                    "cycle_end_ts": cycle_end_ts,
                    "source_snapshot_ts": "2026-03-19T00-10-00Z",
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle_start_ts,
                        "cycle_end_ts": cycle_end_ts,
                        "price_to_beat": 120.0,
                        "final_price": 121.0,
                        "fetched_at": "2026-03-19T00:18:00Z",
                    }
                ]
            ),
            cfg.layout.direct_oracle_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle_start_ts,
                        "cycle_end_ts": cycle_end_ts,
                        "price_to_beat": 120.0,
                        "final_price": 121.0,
                    }
                ]
            ),
        cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "market_id": "market-1",
                        "cycle_start_ts": cycle_start_ts,
                        "cycle_end_ts": cycle_end_ts,
                        "winner_side": "UP",
                        "ingested_at": "2026-03-19T00:16:00Z",
                    }
                ]
            ),
            cfg.layout.settlement_truth_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle_start_ts,
                        "cycle_end_ts": cycle_end_ts,
                        "winner_side": "UP",
                    }
                ]
            ),
        cfg.layout.truth_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                    {
                        "asset": "sol",
                        "tx_hash": "0x1",
                        "perform_idx": 0,
                        "value_idx": 0,
                        "observation_ts": cycle_end_ts,
                        "extra_ts": cycle_end_ts,
                        "ingested_at": "2026-03-19T00:17:00Z",
                    }
                ]
            ),
        cfg.layout.streams_partition_path(2026, 3),
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                    {
                        "asset": "sol",
                        "tx_hash": "0xfeed",
                        "log_index": 1,
                        "updated_at": cycle_end_ts,
                        "answer": 120.5,
                        "ingested_at": "2026-03-19T00:17:30Z",
                    }
                ]
        ),
        cfg.layout.datafeeds_partition_path(2026, 3),
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "captured_ts_ms": captured_ts_ms,
                        "market_id": "market-1",
                        "token_id": "token-up",
                        "side": "up",
                        "best_ask": 0.2,
                }
            ]
        ),
        cfg.layout.orderbook_index_path("2026-03-19"),
    )
    append_ndjson_zst(
        cfg.layout.orderbook_depth_path("2026-03-19"),
        [
            {
                "captured_ts_ms": captured_ts_ms,
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "asks": [[0.2, 10.0]],
                "bids": [[0.19, 8.0]],
            }
        ],
    )

    payload = show_data_summary(cfg, persist=True, now=audit_now)

    assert payload["dataset"] == "data_surface_summary"
    assert payload["summary"]["dataset_count"] == 10
    assert payload["summary"]["existing_dataset_count"] == 10
    assert payload["audit"]["status"] == "ok"
    assert payload["completeness"]["status"] == "ok"
    assert payload["completeness"]["healthy_dataset_count"] == 10
    assert payload["completeness"]["blocking_issue_count"] == 0
    assert payload["audit"]["critical_missing_datasets"] == []
    assert payload["audit"]["stale_issue_datasets"] == []
    assert payload["audit"]["alignment_issue_checks"] == []
    assert payload["issues"] == []
    assert payload["datasets"]["binance_klines_1m_source"]["row_count"] == 2
    assert payload["datasets"]["market_catalog_table"]["duplicate_count"] == 0
    assert payload["datasets"]["direct_oracle_source"]["time_range"]["min"].startswith("2026-")
    assert payload["datasets"]["market_catalog_table"]["time_range"]["min"].startswith("2026-")
    assert payload["datasets"]["market_catalog_table"]["freshness_range"]["max"].startswith("2026-03-19T00:10:00")
    assert payload["datasets"]["direct_oracle_source"]["freshness_range"]["max"].startswith("2026-03-19T00:18:00")
    assert payload["datasets"]["chainlink_streams_source"]["partition_count"] == 1
    assert payload["datasets"]["chainlink_datafeeds_source"]["partition_count"] == 1
    assert payload["datasets"]["orderbook_index_table"]["row_count"] == 1
    assert payload["datasets"]["orderbook_depth_source"]["partition_count"] == 1
    assert Path(payload["latest_summary_path"]).exists()
    assert Path(payload["summary_snapshot_path"]).exists()
    assert Path(payload["latest_manifest_path"]).exists()
    assert Path(payload["manifest_snapshot_path"]).exists()
    manifest = json.loads(Path(payload["latest_manifest_path"]).read_text(encoding="utf-8"))
    assert manifest["object_type"] == "data_summary_manifest"
    assert manifest["completeness"]["status"] == "ok"
    assert manifest["paths"]["latest_manifest_path"] == str(Path(payload["latest_manifest_path"]))
    assert len(manifest["dataset_inventory"]) == 10


def test_show_data_summary_marks_missing_datasets(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="btc", cycle="15m", surface="backtest", root=tmp_path / "v2")

    payload = show_data_summary(cfg)

    assert payload["summary"]["existing_dataset_count"] == 0
    assert payload["summary"]["missing_dataset_count"] == 10
    assert payload["audit"]["status"] == "error"
    assert payload["completeness"]["status"] == "error"
    assert payload["completeness"]["missing_dataset_count"] == 10
    assert payload["audit"]["critical_missing_datasets"] == [
        "binance_klines_1m_source",
        "oracle_prices_table",
        "truth_table",
    ]
    assert "market_catalog_table" in payload["audit"]["warning_missing_datasets"]
    assert payload["datasets"]["market_catalog_table"]["status"] == "missing"
    assert payload["datasets"]["orderbook_depth_source"]["status"] == "missing"
    assert payload["audit"]["dataset_audits"]["market_catalog_table"]["status"] == "missing"
    assert any(issue["code"] == "critical_missing_dataset" for issue in payload["issues"])


def test_show_data_summary_backtest_only_warns_when_builder_inputs_missing_but_tables_exist(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=tmp_path / "v2")
    cycle_start_ts = int(pd.Timestamp("2026-03-19T00:00:00Z").timestamp())
    cycle_end_ts = int(pd.Timestamp("2026-03-19T00:15:00Z").timestamp())

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-19T00:00:00Z"),
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                },
                {
                    "open_time": pd.Timestamp("2026-03-19T00:01:00Z"),
                    "open": 1.05,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.1,
                },
            ]
        ),
        cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": cycle_start_ts,
                    "cycle_end_ts": cycle_end_ts,
                    "price_to_beat": 120.0,
                    "final_price": 121.0,
                }
            ]
        ),
        cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": cycle_start_ts,
                    "cycle_end_ts": cycle_end_ts,
                    "winner_side": "UP",
                }
            ]
        ),
        cfg.layout.truth_table_path,
    )

    payload = show_data_summary(cfg)

    assert payload["audit"]["status"] == "warning"
    assert payload["completeness"]["status"] == "warning"
    assert payload["audit"]["critical_missing_datasets"] == []
    assert "market_catalog_table" in payload["audit"]["warning_missing_datasets"]
    assert "chainlink_streams_source" in payload["audit"]["warning_missing_datasets"]
    assert payload["completeness"]["blocking_issue_count"] == 0
    assert "market_catalog_table" not in payload["completeness"]["blocking_datasets"]


def test_show_data_summary_flags_stale_and_lagging_live_datasets(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    cycle0_start_ts = int(pd.Timestamp("2026-03-19T00:00:00Z").timestamp())
    cycle0_end_ts = int(pd.Timestamp("2026-03-19T00:15:00Z").timestamp())
    cycle1_start_ts = int(pd.Timestamp("2026-03-19T04:00:00Z").timestamp())
    cycle1_end_ts = int(pd.Timestamp("2026-03-19T04:15:00Z").timestamp())
    orderbook_index_ts_ms = int(pd.Timestamp("2026-03-20T00:00:00Z").timestamp() * 1000)
    orderbook_depth_ts_ms = int(pd.Timestamp("2026-03-20T00:05:00Z").timestamp() * 1000)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "open_time": pd.Timestamp("2026-03-19T00:00:00Z"),
                    "open": 1.0,
                    "high": 1.1,
                    "low": 0.9,
                    "close": 1.05,
                },
                {
                    "open_time": pd.Timestamp("2026-03-19T00:01:00Z"),
                    "open": 1.05,
                    "high": 1.2,
                    "low": 1.0,
                    "close": 1.1,
                },
            ]
        ),
        cfg.layout.binance_klines_path(),
    )
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "market-1",
                    "condition_id": "cond-1",
                    "asset": "sol",
                    "cycle": "15m",
                    "cycle_start_ts": cycle0_start_ts,
                    "cycle_end_ts": cycle0_end_ts,
                    "source_snapshot_ts": "2026-03-19T00-05-00Z",
                }
            ]
        ),
        cfg.layout.market_catalog_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle0_start_ts,
                        "cycle_end_ts": cycle0_end_ts,
                        "price_to_beat": 120.0,
                        "final_price": 121.0,
                        "fetched_at": "2026-03-19T00:10:00Z",
                    },
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle1_start_ts,
                        "cycle_end_ts": cycle1_end_ts,
                        "price_to_beat": 122.0,
                        "final_price": 123.0,
                        "fetched_at": "2026-03-19T04:20:00Z",
                    },
                ]
            ),
            cfg.layout.direct_oracle_source_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "asset": "sol",
                        "cycle_start_ts": cycle0_start_ts,
                        "cycle_end_ts": cycle0_end_ts,
                        "price_to_beat": 120.0,
                        "final_price": 121.0,
                    }
                ]
            ),
        cfg.layout.oracle_prices_table_path,
    )
    write_parquet_atomic(
        pd.DataFrame(
                [
                    {
                        "captured_ts_ms": orderbook_index_ts_ms,
                        "market_id": "market-1",
                        "token_id": "token-up",
                        "side": "up",
                        "best_ask": 0.2,
                }
            ]
        ),
        cfg.layout.orderbook_index_path("2026-03-20"),
    )
    append_ndjson_zst(
        cfg.layout.orderbook_depth_path("2026-03-20"),
        [
            {
                "captured_ts_ms": orderbook_depth_ts_ms,
                "market_id": "market-1",
                "token_id": "token-up",
                "side": "up",
                "asks": [[0.2, 10.0]],
                "bids": [[0.19, 8.0]],
            }
        ],
    )

    payload = show_data_summary(cfg, now=pd.Timestamp("2026-03-22T12:00:00Z"))

    assert payload["audit"]["status"] == "error"
    assert payload["completeness"]["status"] == "error"
    assert payload["completeness"]["blocking_issue_count"] > 0
    assert "binance_klines_1m_source" in payload["audit"]["stale_issue_datasets"]
    assert "market_catalog_table" in payload["audit"]["stale_issue_datasets"]
    assert "direct_oracle_source" in payload["audit"]["stale_issue_datasets"]
    assert "orderbook_depth_source" in payload["audit"]["stale_issue_datasets"]
    assert "oracle_prices_table_vs_direct_oracle_source" in payload["audit"]["alignment_issue_checks"]
    assert payload["audit"]["dataset_audits"]["orderbook_index_table"]["status"] == "warning"
    assert any(issue["code"] == "alignment_check_failed" for issue in payload["issues"])


def test_persist_data_summary_preserves_existing_latest_file_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    original_payload = {
        "generated_at": "2026-03-19T00-20-00Z",
        "generated_at_iso": "2026-03-19T00:20:00+00:00",
        "market": "sol",
        "cycle": "15m",
        "surface": "live",
        "surface_data_root": str(cfg.layout.surface_data_root),
        "summary": {"dataset_count": 1, "existing_dataset_count": 1, "missing_dataset_count": 0},
        "audit": {
            "status": "ok",
            "critical_expected_datasets": [],
            "dataset_audits": {},
        },
        "completeness": {"status": "ok"},
        "issues": [],
        "datasets": {},
    }
    persist_data_summary(cfg=cfg, payload=original_payload)
    original_write_text = Path.write_text

    def _failing_write_text(self: Path, data: str, *args, **kwargs):
        if self.parent == cfg.layout.latest_summary_path.parent and self.name.startswith(cfg.layout.latest_summary_path.name):
            original_write_text(self, "{", *args, **kwargs)
            raise RuntimeError("simulated summary write failure")
        return original_write_text(self, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", _failing_write_text)

    with pytest.raises(RuntimeError, match="simulated summary write failure"):
        persist_data_summary(
            cfg=cfg,
            payload={
                **original_payload,
                "generated_at": "2026-03-19T00-25-00Z",
                "generated_at_iso": "2026-03-19T00:25:00+00:00",
            },
        )

    persisted = json.loads(cfg.layout.latest_summary_path.read_text(encoding="utf-8"))
    assert persisted["generated_at"] == original_payload["generated_at"]
