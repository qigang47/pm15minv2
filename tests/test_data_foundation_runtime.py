from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.foundation_runtime import run_live_data_foundation


def test_run_live_data_foundation_writes_state_and_logs(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    calls: list[tuple[str, str]] = []

    def fake_market_catalog(*args, **kwargs):
        target_cfg = kwargs.get("cfg") or args[0]
        calls.append(("market_catalog", target_cfg.asset.slug))
        return {"dataset": "market_catalog"}

    def fake_binance(*args, **kwargs):
        target_cfg = args[0]
        calls.append(("binance", target_cfg.asset.slug))
        return {"dataset": "binance_klines_1m", "market": target_cfg.asset.slug}

    def fake_direct_oracle(*args, **kwargs):
        target_cfg = args[0]
        calls.append(("direct_oracle", target_cfg.asset.slug))
        return {"dataset": "polymarket_direct_oracle_prices"}

    def fake_streams(*args, **kwargs):
        target_cfg = args[0]
        calls.append(("streams", target_cfg.asset.slug))
        return {"dataset": "chainlink_streams_rpc"}

    def fake_build_oracle(*args, **kwargs):
        target_cfg = args[0]
        calls.append(("oracle_table", target_cfg.asset.slug))
        return {"dataset": "oracle_prices_15m"}

    def fake_orderbooks(*args, **kwargs):
        target_cfg = args[0]
        calls.append(("orderbooks", target_cfg.asset.slug))
        return {"status": "ok", "dataset": "orderbook_depth"}

    start = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    ticks = iter(start + timedelta(seconds=idx) for idx in range(8))

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", fake_market_catalog)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", fake_binance)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", fake_streams)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_prices_direct", fake_direct_oracle)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", fake_build_oracle)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", fake_orderbooks)

    summary = run_live_data_foundation(
        cfg,
        iterations=2,
        loop=False,
        include_direct_oracle=True,
        include_orderbooks=True,
        now_provider=lambda: next(ticks),
    )

    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 2
    assert ("binance", "sol") in calls
    assert ("binance", "btc") in calls
    assert calls.count(("market_catalog", "sol")) == 2
    assert calls.count(("streams", "sol")) == 2
    assert calls.count(("direct_oracle", "sol")) == 2
    assert calls.count(("oracle_table", "sol")) == 2
    assert calls.count(("orderbooks", "sol")) == 2

    state_path = Path(summary["state_path"])
    log_path = Path(summary["log_path"])
    assert state_path.exists()
    assert log_path.exists()

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["completed_iterations"] == 2
    assert summary["run_started_at"] == "2026-03-20T00:00:00Z"
    assert summary["last_completed_at"] == "2026-03-20T00:00:06Z"
    assert summary["finished_at"] == "2026-03-20T00:00:06Z"
    assert state["run_started_at"] == summary["run_started_at"]
    assert state["last_completed_at"] == summary["last_completed_at"]
    assert state["finished_at"] == summary["finished_at"]
    assert state["last_task_results"]["orderbooks"]["status"] == "ok"

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any('"event": "foundation_iteration"' in line for line in lines)
    assert any('"event": "foundation_finished"' in line for line in lines)


def test_run_live_data_foundation_allows_oracle_fail_open_with_existing_table(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": 1773964800,
                    "cycle_end_ts": 1773965700,
                    "price_to_beat": 100.0,
                    "final_price": 101.0,
                    "source_price_to_beat": "cached",
                    "source_final_price": "cached",
                    "has_price_to_beat": True,
                    "has_final_price": True,
                    "has_both": True,
                }
            ]
        ),
        cfg.layout.oracle_prices_table_path,
    )

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", lambda *args, **kwargs: {"dataset": "market_catalog"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", lambda *args, **kwargs: {"dataset": "binance_klines_1m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "chainlink_streams_rpc"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_prices_direct", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("429 too many requests")))
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_15m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", lambda *args, **kwargs: {"status": "ok", "dataset": "orderbook_depth"})

    summary = run_live_data_foundation(cfg, iterations=1, loop=False)
    state = json.loads(cfg.layout.foundation_state_path.read_text(encoding="utf-8"))

    assert summary["status"] == "ok_with_errors"
    assert summary["errors"] == 1
    assert summary["run_started_at"]
    assert summary["last_completed_at"]
    assert summary["finished_at"]
    assert summary["last_results"]["market_catalog"]["dataset"] == "market_catalog"
    assert summary["last_task_results"]["oracle"]["status"] == "degraded"
    assert summary["issue_codes"] == ["oracle_direct_rate_limited"]
    assert summary["reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert state["status"] == "ok_with_errors"
    assert state["issue_codes"] == ["oracle_direct_rate_limited"]
    assert state["reason"] == "oracle:oracle_direct_rate_limited:429 too many requests"
    assert state["finished_at"] == summary["finished_at"]
    assert summary["degraded_tasks"] == [
        {
            "task": "oracle",
            "issue_code": "oracle_direct_rate_limited",
            "error_type": "RuntimeError",
            "error": "429 too many requests",
            "fail_open": True,
            "fallback_path": str(cfg.layout.oracle_prices_table_path),
        }
    ]


def test_run_live_data_foundation_persists_error_metadata_before_raise(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", lambda *args, **kwargs: {"dataset": "market_catalog"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", lambda *args, **kwargs: {"dataset": "binance_klines_1m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "chainlink_streams_rpc"})
    monkeypatch.setattr(
        "pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_prices_direct",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("oracle down")),
    )
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_15m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", lambda *args, **kwargs: {"status": "ok", "dataset": "orderbook_depth"})

    with pytest.raises(RuntimeError, match="oracle down"):
        run_live_data_foundation(
            cfg,
            iterations=1,
            loop=False,
            include_direct_oracle=True,
            include_orderbooks=True,
        )

    state = json.loads(cfg.layout.foundation_state_path.read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["issue_codes"] == ["oracle_refresh_failed"]
    assert state["reason"] == "oracle:oracle_refresh_failed:oracle down"
    assert state["last_task_results"]["oracle"]["status"] == "error"
    assert state["last_task_results"]["oracle"]["error"] == "oracle down"
    assert state["finished_at"]
