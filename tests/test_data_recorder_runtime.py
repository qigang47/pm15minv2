from __future__ import annotations

import json
from pathlib import Path
import time

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.data.pipelines.orderbook_recording import CapturedOrderbookBatch
from pm15min.data.pipelines.orderbook_runtime import run_orderbook_recorder


class _FakeClobClient:
    def fetch_book(self, token_id: str, *, levels: int = 0, timeout_sec: float = 1.2):
        return {
            "timestamp": "2026-03-19T09:00:00Z",
            "asks": [{"price": "0.12", "size": "10"}],
            "bids": [{"price": "0.11", "size": "8"}],
        }


def test_run_orderbook_recorder_writes_state_and_logs(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", root=tmp_path / "v2", market_depth=1)
    market_table = pd.DataFrame(
        [
            {
                "market_id": "market-1",
                "condition_id": "cond-1",
                "asset": "xrp",
                "cycle": "15m",
                "cycle_start_ts": 1_710_000_000,
                "cycle_end_ts": 1_910_000_000,
                "token_up": "token-up",
                "token_down": "token-down",
                "slug": "xrp-up-or-down-15m-1710000000",
                "question": "XRP up or down",
                "resolution_source": "https://data.chain.link/streams/xrp-usd",
                "event_id": "event-1",
                "event_slug": "slug",
                "event_title": "title",
                "series_slug": "xrp-up-or-down-15m",
                "closed_ts": None,
                "source_snapshot_ts": "2026-03-19T09-00-00Z",
            }
        ]
    )
    write_parquet_atomic(market_table, cfg.layout.market_catalog_table_path)

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=2,
        loop=True,
        sleep_sec=0.0,
    )

    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 2
    assert summary["last_completed_at"]
    state_path = Path(summary["state_path"])
    log_path = Path(summary["log_path"])
    assert state_path.exists()
    assert log_path.exists()

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["completed_iterations"] == 2
    assert state["provider"] == "DirectOrderbookProvider"
    assert state["last_completed_at"] == summary["last_completed_at"]

    lines = [line for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) >= 2
    assert any('"event": "iteration_ok"' in line for line in lines)
    assert any('"event": "run_finished"' in line for line in lines)


def test_run_orderbook_recorder_uses_sleep_sec_as_target_period(tmp_path: Path, monkeypatch) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", root=tmp_path / "v2", market_depth=1)
    monotonic_now = {"value": 100.0}
    sleep_calls: list[float] = []

    def _fake_monotonic() -> float:
        return monotonic_now["value"]

    def _fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)
        monotonic_now["value"] += float(seconds)

    def _fake_record_orderbooks_once(cfg, *, provider=None):
        del cfg, provider
        monotonic_now["value"] += 0.1
        return {
            "captured_ts_ms": 1,
            "selected_markets": 1,
            "market_start_offset": 0,
            "snapshot_rows": 2,
            "recent_rows": 2,
            "recent_window_minutes": 15,
        }

    monkeypatch.setattr("pm15min.data.pipelines.orderbook_runtime.time.monotonic", _fake_monotonic)
    monkeypatch.setattr("pm15min.data.pipelines.orderbook_runtime.time.sleep", _fake_sleep)
    monkeypatch.setattr(
        "pm15min.data.pipelines.orderbook_runtime.record_orderbooks_once",
        _fake_record_orderbooks_once,
    )

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=2,
        loop=True,
        sleep_sec=0.5,
    )

    assert summary["status"] == "ok"
    assert len(sleep_calls) == 1
    assert abs(sleep_calls[0] - 0.4) < 1e-6


def test_run_orderbook_recorder_async_persist_uses_bounded_queue(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured: list[int] = []
    persisted: list[int] = []

    def _fake_capture(cfg, *, provider=None):
        del cfg, provider
        ts_ms = 1_710_000_000_000 + len(captured)
        captured.append(ts_ms)
        return CapturedOrderbookBatch(
            captured_ts_ms=ts_ms,
            date_str="2026-03-19",
            selected_markets=1,
            market_start_offset=0,
            selected_market_ids=["market-1"],
            recent_window_minutes=15,
            snapshot_rows=[
                {
                    "captured_ts_ms": ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "asks": [{"price": 0.2, "size": 1.0}],
                    "bids": [{"price": 0.19, "size": 1.0}],
                }
            ],
            index_rows=[
                {
                    "captured_ts_ms": ts_ms,
                    "market_id": "market-1",
                    "token_id": "token-up",
                    "side": "up",
                    "best_ask": 0.2,
                    "best_bid": 0.19,
                    "ask_size_1": 1.0,
                    "bid_size_1": 1.0,
                    "spread": 0.01,
                }
            ],
        )

    def _fake_persist(cfg, *, batch):
        del cfg
        time.sleep(0.05)
        persisted.append(int(batch.captured_ts_ms))
        return {
            "captured_ts_ms": int(batch.captured_ts_ms),
            "selected_markets": 1,
            "market_start_offset": 0,
            "snapshot_rows": len(batch.snapshot_rows),
            "recent_rows": len(batch.index_rows),
            "recent_window_minutes": int(batch.recent_window_minutes),
        }

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=4,
        loop=True,
        sleep_sec=0.0,
        async_persist=True,
        max_pending_batches=1,
        drop_oldest_when_full=True,
        capture_orderbooks_once_fn=_fake_capture,
        persist_captured_orderbooks_once_fn=_fake_persist,
    )

    assert summary["status"] == "ok"
    assert summary["completed_iterations"] == 4
    assert summary["persisted_iterations"] < summary["completed_iterations"]
    assert summary["dropped_batches"] >= 1

    state = json.loads(Path(summary["state_path"]).read_text(encoding="utf-8"))
    assert state["persistence_mode"] == "async_bounded"
    assert state["dropped_batches"] >= 1
    assert state["persisted_iterations"] == summary["persisted_iterations"]
    assert captured[-1] in persisted


def test_run_orderbook_recorder_async_persist_surfaces_write_errors(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured: list[int] = []
    persisted: list[int] = []

    def _fake_capture(cfg, *, provider=None):
        del cfg, provider
        ts_ms = 1_710_100_000_000 + len(captured)
        captured.append(ts_ms)
        return CapturedOrderbookBatch(
            captured_ts_ms=ts_ms,
            date_str="2026-03-20",
            selected_markets=1,
            market_start_offset=0,
            selected_market_ids=["market-1"],
            recent_window_minutes=15,
            snapshot_rows=[{"captured_ts_ms": ts_ms}],
            index_rows=[{"captured_ts_ms": ts_ms, "market_id": "market-1", "token_id": "token-up", "side": "up"}],
        )

    def _fake_persist(cfg, *, batch):
        del cfg
        if int(batch.captured_ts_ms) == captured[0]:
            raise RuntimeError("disk full")
        persisted.append(int(batch.captured_ts_ms))
        return {
            "captured_ts_ms": int(batch.captured_ts_ms),
            "selected_markets": 1,
            "market_start_offset": 0,
            "snapshot_rows": len(batch.snapshot_rows),
            "recent_rows": len(batch.index_rows),
            "recent_window_minutes": int(batch.recent_window_minutes),
        }

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=3,
        loop=True,
        sleep_sec=0.0,
        async_persist=True,
        max_pending_batches=3,
        drop_oldest_when_full=False,
        capture_orderbooks_once_fn=_fake_capture,
        persist_captured_orderbooks_once_fn=_fake_persist,
    )

    assert summary["status"] == "error"
    assert summary["errors"] == 1
    assert summary["persisted_iterations"] == 2
    assert persisted == captured[1:]

    state = json.loads(Path(summary["state_path"]).read_text(encoding="utf-8"))
    assert state["status"] == "error"
    assert state["errors"] == 1
    assert state["write_errors"] == 1
    assert "disk full" in str(state["last_error"])


def test_run_orderbook_recorder_async_persist_flushes_before_return(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured: list[int] = []
    persisted: list[int] = []

    def _fake_capture(cfg, *, provider=None):
        del cfg, provider
        ts_ms = 1_710_200_000_000 + len(captured)
        captured.append(ts_ms)
        return CapturedOrderbookBatch(
            captured_ts_ms=ts_ms,
            date_str="2026-03-21",
            selected_markets=1,
            market_start_offset=0,
            selected_market_ids=["market-1"],
            recent_window_minutes=15,
            snapshot_rows=[{"captured_ts_ms": ts_ms}],
            index_rows=[{"captured_ts_ms": ts_ms, "market_id": "market-1", "token_id": "token-up", "side": "up"}],
        )

    def _fake_persist(cfg, *, batch):
        del cfg
        time.sleep(0.02)
        persisted.append(int(batch.captured_ts_ms))
        return {
            "captured_ts_ms": int(batch.captured_ts_ms),
            "selected_markets": 1,
            "market_start_offset": 0,
            "snapshot_rows": len(batch.snapshot_rows),
            "recent_rows": len(batch.index_rows),
            "recent_window_minutes": int(batch.recent_window_minutes),
        }

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=3,
        loop=True,
        sleep_sec=0.0,
        async_persist=True,
        max_pending_batches=1,
        drop_oldest_when_full=False,
        capture_orderbooks_once_fn=_fake_capture,
        persist_captured_orderbooks_once_fn=_fake_persist,
    )

    assert summary["status"] == "ok"
    assert summary["errors"] == 0
    assert summary["completed_iterations"] == 3
    assert summary["persisted_iterations"] == 3
    assert persisted == captured

    state = json.loads(Path(summary["state_path"]).read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["pending_batches"] == 0
    assert state["persisted_iterations"] == 3


def test_run_orderbook_recorder_async_persist_supports_unbounded_queue(tmp_path: Path) -> None:
    cfg = DataConfig.build(market="xrp", cycle="15m", surface="live", root=tmp_path / "v2", market_depth=1)
    captured: list[int] = []
    persisted: list[int] = []

    def _fake_capture(cfg, *, provider=None):
        del cfg, provider
        ts_ms = 1_710_300_000_000 + len(captured)
        captured.append(ts_ms)
        return CapturedOrderbookBatch(
            captured_ts_ms=ts_ms,
            date_str="2026-03-22",
            selected_markets=1,
            market_start_offset=0,
            selected_market_ids=["market-1"],
            recent_window_minutes=15,
            snapshot_rows=[{"captured_ts_ms": ts_ms}],
            index_rows=[{"captured_ts_ms": ts_ms, "market_id": "market-1", "token_id": "token-up", "side": "up"}],
        )

    def _fake_persist(cfg, *, batch):
        del cfg
        time.sleep(0.02)
        persisted.append(int(batch.captured_ts_ms))
        return {
            "captured_ts_ms": int(batch.captured_ts_ms),
            "selected_markets": 1,
            "market_start_offset": 0,
            "snapshot_rows": len(batch.snapshot_rows),
            "recent_rows": len(batch.index_rows),
            "recent_window_minutes": int(batch.recent_window_minutes),
        }

    summary = run_orderbook_recorder(
        cfg,
        client=_FakeClobClient(),
        iterations=4,
        loop=True,
        sleep_sec=0.0,
        async_persist=True,
        max_pending_batches=0,
        drop_oldest_when_full=True,
        capture_orderbooks_once_fn=_fake_capture,
        persist_captured_orderbooks_once_fn=_fake_persist,
    )

    assert summary["status"] == "ok"
    assert summary["errors"] == 0
    assert summary["completed_iterations"] == 4
    assert summary["persisted_iterations"] == 4
    assert summary["dropped_batches"] == 0
    assert persisted == captured

    state = json.loads(Path(summary["state_path"]).read_text(encoding="utf-8"))
    assert state["status"] == "ok"
    assert state["persistence_mode"] == "async_unbounded"
    assert state["dropped_batches"] == 0
    assert state["persisted_iterations"] == 4
