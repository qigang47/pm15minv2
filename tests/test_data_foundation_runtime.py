from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import read_parquet_if_exists
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.foundation_runtime import run_live_data_foundation, run_live_data_foundation_shared
import pm15min.data.pipelines.foundation_runtime as foundation_runtime_module


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
        existing = read_parquet_if_exists(target_cfg.layout.direct_oracle_source_path)
        incoming = pd.DataFrame(
            [
                {
                    "asset": target_cfg.asset.slug,
                    "cycle": target_cfg.cycle,
                    "cycle_start_ts": int(pd.Timestamp("2026-03-20T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-20T00:15:00Z").timestamp()),
                    "price_to_beat": 100.0,
                    "final_price": pd.NA,
                    "has_price_to_beat": True,
                    "has_final_price": False,
                    "has_both": False,
                    "completed": False,
                    "incomplete": True,
                    "cached": False,
                    "api_timestamp_ms": None,
                    "http_status": 200,
                    "source": "polymarket_api_crypto_price",
                    "source_priority": 3,
                    "fetched_at": "2026-03-20T00:00:00Z",
                }
            ]
        )
        write_parquet_atomic(incoming if existing is None else pd.concat([existing, incoming], ignore_index=True), target_cfg.layout.direct_oracle_source_path)
        return {"dataset": "polymarket_direct_oracle_price_window", "rows_imported": 1}

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
    ticks = iter(start + timedelta(seconds=idx) for idx in range(10))

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", fake_market_catalog)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", fake_binance)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", fake_streams)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window", fake_direct_oracle)
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
    assert ("binance", "btc") not in calls
    assert calls.count(("market_catalog", "sol")) == 2
    assert calls.count(("binance", "sol")) == 2
    assert calls.count(("streams", "sol")) == 2
    assert calls.count(("direct_oracle", "sol")) == 1
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
    assert summary["last_completed_at"] == "2026-03-20T00:00:08Z"
    assert summary["finished_at"] == "2026-03-20T00:00:08Z"
    assert state["run_started_at"] == summary["run_started_at"]
    assert state["last_completed_at"] == summary["last_completed_at"]
    assert state["finished_at"] == summary["finished_at"]
    assert state["last_task_results"]["orderbooks"]["status"] == "ok"

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert any('"event": "foundation_iteration"' in line for line in lines)
    assert any('"event": "foundation_finished"' in line for line in lines)


def test_build_foundation_task_specs_refreshes_only_binance_now(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    iteration_now = datetime(2026, 3, 20, 0, 6, 59, 900000, tzinfo=timezone.utc)
    binance_now = datetime(2026, 3, 20, 0, 7, 0, 400000, tzinfo=timezone.utc)
    seen: dict[str, object] = {}

    monkeypatch.setattr(
        foundation_runtime_module,
        "_run_market_catalog_step",
        lambda *args, **kwargs: seen.__setitem__("market_catalog", kwargs["now"]) or {"dataset": "market_catalog"},
    )
    monkeypatch.setattr(
        foundation_runtime_module,
        "_run_binance_step",
        lambda *args, **kwargs: seen.__setitem__("binance", kwargs["now"]) or {"dataset": "binance_klines_1m"},
    )
    monkeypatch.setattr(
        foundation_runtime_module,
        "_run_streams_step",
        lambda *args, **kwargs: seen.__setitem__("streams", kwargs["now"]) or {"dataset": "chainlink_streams_rpc"},
    )
    monkeypatch.setattr(
        foundation_runtime_module,
        "_run_oracle_step",
        lambda *args, **kwargs: seen.__setitem__("oracle", kwargs["now"]) or {"dataset": "oracle_prices_15m"},
    )
    monkeypatch.setattr(
        foundation_runtime_module,
        "_run_orderbook_step",
        lambda *args, **kwargs: seen.__setitem__("orderbooks", True) or {"status": "ok", "dataset": "orderbook_depth"},
    )

    task_specs = foundation_runtime_module._build_foundation_task_specs(
        cfg,
        now=iteration_now,
        now_provider=lambda: binance_now,
        gamma_client=None,
        binance_client=None,
        oracle_client=None,
        orderbook_client=None,
        market_catalog_refresh_sec=300.0,
        binance_refresh_sec=60.0,
        oracle_refresh_sec=60.0,
        streams_refresh_sec=300.0,
        orderbook_refresh_sec=0.35,
        market_catalog_lookback_hours=24,
        market_catalog_lookahead_hours=24,
        binance_lookback_minutes=2880,
        binance_batch_limit=1000,
        oracle_lookback_days=2,
        oracle_lookahead_hours=24,
        include_direct_oracle=True,
        include_streams=True,
        include_orderbooks=True,
    )

    for _task_name, _interval_sec, task_fn in task_specs:
        task_fn()

    assert seen["market_catalog"] == iteration_now
    assert seen["binance"] == binance_now
    assert seen["streams"] == iteration_now
    assert seen["oracle"] == iteration_now
    assert seen["orderbooks"] is True


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
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("429 too many requests")))
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
        "pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window",
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


def test_run_live_data_foundation_retries_missing_current_cycle_open_price_only_after_retry_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    foundation_runtime_module._CURRENT_CYCLE_DIRECT_ORACLE_LAST_ATTEMPT.clear()
    calls = {"direct_oracle": 0}
    clock = {"now": 100.0}

    monkeypatch.setattr(foundation_runtime_module.time, "monotonic", lambda: float(clock["now"]))
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", lambda *args, **kwargs: {"dataset": "market_catalog"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", lambda *args, **kwargs: {"dataset": "binance_klines_1m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "chainlink_streams_rpc"})

    def fake_direct_oracle(*args, **kwargs):
        calls["direct_oracle"] += 1
        return {
            "dataset": "polymarket_direct_oracle_price_window",
            "rows_imported": 0,
            "canonical_rows": 0,
            "target_path": str(cfg.layout.direct_oracle_source_path),
        }

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window", fake_direct_oracle)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_15m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", lambda *args, **kwargs: {"status": "ok", "dataset": "orderbook_depth"})

    now = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    first = run_live_data_foundation(cfg, iterations=1, loop=False, now_provider=lambda: now)
    second = run_live_data_foundation(cfg, iterations=1, loop=False, now_provider=lambda: now + timedelta(seconds=10))
    clock["now"] += 60.0
    third = run_live_data_foundation(cfg, iterations=1, loop=False, now_provider=lambda: now + timedelta(seconds=70))

    assert calls["direct_oracle"] == 2
    assert first["last_results"]["oracle"]["direct_summary"]["status"] == "missing"
    assert second["last_results"]["oracle"]["direct_summary"]["status"] == "deferred"
    assert second["last_results"]["oracle"]["direct_summary"]["reason"] == "current_cycle_open_price_retry_deferred"
    assert third["last_results"]["oracle"]["direct_summary"]["status"] == "missing"


def test_run_live_data_foundation_loop_zero_iterations_runs_until_stopped(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class StopLoop(RuntimeError):
        pass

    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    calls = {"market_catalog": 0, "binance": 0}
    clock = {"tick": 0}
    sleep_calls = {"count": 0}

    def fake_now() -> datetime:
        value = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc) + timedelta(seconds=clock["tick"])
        clock["tick"] += 1
        return value

    def fake_sleep(_seconds: float) -> None:
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            raise StopLoop("stop after confirming repeated loop iterations")

    monkeypatch.setattr(foundation_runtime_module.time, "sleep", fake_sleep)
    monkeypatch.setattr(
        "pm15min.data.pipelines.foundation_runtime.sync_market_catalog",
        lambda *args, **kwargs: calls.__setitem__("market_catalog", calls["market_catalog"] + 1) or {"dataset": "market_catalog"},
    )
    monkeypatch.setattr(
        "pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m",
        lambda *args, **kwargs: calls.__setitem__("binance", calls["binance"] + 1) or {"dataset": "binance_klines_1m"},
    )
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "chainlink_streams_rpc"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window", lambda *args, **kwargs: {"dataset": "polymarket_direct_oracle_price_window", "rows_imported": 0, "canonical_rows": 0, "target_path": str(cfg.layout.direct_oracle_source_path)})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_15m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", lambda *args, **kwargs: {"status": "ok", "dataset": "orderbook_depth"})

    with pytest.raises(StopLoop):
        run_live_data_foundation(
            cfg,
            iterations=0,
            loop=True,
            sleep_sec=1.0,
            market_catalog_refresh_sec=0.0,
            binance_refresh_sec=0.0,
            include_streams=False,
            include_direct_oracle=False,
            include_orderbooks=False,
            now_provider=fake_now,
        )

    assert calls["market_catalog"] >= 2
    assert calls["binance"] >= 2


def test_run_live_data_foundation_shared_prioritizes_due_factor_tasks_across_markets(
    tmp_path: Path,
    monkeypatch,
) -> None:
    sol_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    xrp_cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=tmp_path / "v2")
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
        calls.append(("oracle", target_cfg.asset.slug))
        return {
            "dataset": "polymarket_direct_oracle_price_window",
            "rows_imported": 1,
            "canonical_rows": 1,
            "target_path": str(target_cfg.layout.direct_oracle_source_path),
        }

    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_market_catalog", fake_market_catalog)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_binance_klines_1m", fake_binance)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_polymarket_oracle_price_window", fake_direct_oracle)
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.sync_streams_from_rpc", lambda *args, **kwargs: {"dataset": "chainlink_streams_rpc"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.build_oracle_prices_15m", lambda *args, **kwargs: {"dataset": "oracle_prices_15m"})
    monkeypatch.setattr("pm15min.data.pipelines.foundation_runtime.run_orderbook_recorder", lambda *args, **kwargs: {"status": "ok", "dataset": "orderbook_depth"})

    summary = run_live_data_foundation_shared(
        [sol_cfg, xrp_cfg],
        iterations=1,
        loop=False,
        include_streams=False,
        include_orderbooks=False,
        now_provider=lambda: datetime(2026, 3, 20, 0, 7, tzinfo=timezone.utc),
    )

    assert summary["status"] == "ok"
    assert summary["markets"] == ["sol", "xrp"]
    assert calls[:6] == [
        ("binance", "sol"),
        ("binance", "xrp"),
        ("oracle", "sol"),
        ("oracle", "xrp"),
        ("market_catalog", "sol"),
        ("market_catalog", "xrp"),
    ]
    sol_state = json.loads(sol_cfg.layout.foundation_state_path.read_text(encoding="utf-8"))
    xrp_state = json.loads(xrp_cfg.layout.foundation_state_path.read_text(encoding="utf-8"))
    assert sol_state["mode"] == "shared"
    assert xrp_state["mode"] == "shared"
    assert sol_state["shared_markets"] == ["sol", "xrp"]


def test_next_foundation_task_due_at_prefers_boundary_then_fallback(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS", "7,8,9")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC", "300")

    near_boundary = datetime(2026, 3, 20, 0, 6, 1, tzinfo=timezone.utc)
    due_near = foundation_runtime_module._next_foundation_task_due_at(
        cfg=cfg,
        task_name="binance",
        now=near_boundary,
        interval_sec=60.0,
    )
    assert pd.Timestamp(due_near, unit="s", tz="UTC") == pd.Timestamp("2026-03-20T00:07:00Z")

    after_window = datetime(2026, 3, 20, 0, 10, 0, tzinfo=timezone.utc)
    due_fallback = foundation_runtime_module._next_foundation_task_due_at(
        cfg=cfg,
        task_name="binance",
        now=after_window,
        interval_sec=60.0,
    )
    assert pd.Timestamp(due_fallback, unit="s", tz="UTC") == pd.Timestamp("2026-03-20T00:15:00Z")


def test_next_foundation_task_due_at_retries_when_closed_bar_not_ready(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS", "7,8,9")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_INTERVAL_SEC", "0.2")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_RETRY_WINDOW_SEC", "1.5")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC", "300")

    just_after_close = datetime(2026, 3, 20, 0, 7, 0, 100000, tzinfo=timezone.utc)
    due_retry = foundation_runtime_module._next_foundation_task_due_at(
        cfg=cfg,
        task_name="binance",
        now=just_after_close,
        interval_sec=60.0,
        last_summary={"latest_open_time": "2026-03-20T00:05:00Z"},
    )
    assert due_retry == pytest.approx(pd.Timestamp("2026-03-20T00:07:00.300000Z").timestamp(), abs=1e-6)

    due_next = foundation_runtime_module._next_foundation_task_due_at(
        cfg=cfg,
        task_name="binance",
        now=just_after_close,
        interval_sec=60.0,
        last_summary={"latest_open_time": "2026-03-20T00:06:00Z"},
    )
    assert pd.Timestamp(due_next, unit="s", tz="UTC") == pd.Timestamp("2026-03-20T00:08:00Z")


def test_next_foundation_task_due_at_honors_zero_fallback_refresh(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=tmp_path / "v2")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_OFFSETS", "7,8,9")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_BOUNDARY_DELAY_SEC", "0")
    monkeypatch.setenv("PM15MIN_LIVE_FOUNDATION_BINANCE_FALLBACK_REFRESH_SEC", "0")

    now = datetime(2026, 3, 20, 0, 10, 0, tzinfo=timezone.utc)
    due = foundation_runtime_module._next_foundation_task_due_at(
        cfg=cfg,
        task_name="binance",
        now=now,
        interval_sec=0.0,
    )
    assert due == pytest.approx(now.timestamp(), abs=1e-6)
