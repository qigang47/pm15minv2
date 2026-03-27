from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.research.backtests.data_surface_fallback import preflight_orderbook_index_dates


def test_preflight_orderbook_index_dates_rebuilds_missing_index_and_reports_gaps(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-01T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 8.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-01T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 7.0]],
            },
        ],
    )

    empty_depth_path = data_cfg.layout.orderbook_depth_path("2026-03-02")
    empty_depth_path.parent.mkdir(parents=True, exist_ok=True)
    empty_depth_path.write_bytes(b"\x28\xb5\x2f\xfd\x00\x58\x69\x00\x00\x65\x6d\x70\x74")

    summary = preflight_orderbook_index_dates(
        data_cfg,
        date_strings=["2026-03-01", "2026-03-02", "2026-03-03"],
    )

    assert summary["requested_dates"] == ["2026-03-01", "2026-03-02", "2026-03-03"]
    assert summary["rebuilt_dates"] == ["2026-03-01"]
    assert summary["ready_dates"] == ["2026-03-01"]
    assert summary["empty_depth_source_dates"] == ["2026-03-02"]
    assert summary["missing_depth_dates"] == ["2026-03-03"]
    assert summary["status_counts"] == {
        "rebuilt": 1,
        "empty_depth_source": 1,
        "missing_depth": 1,
    }

    index_path = data_cfg.layout.orderbook_index_path("2026-03-01")
    assert index_path.exists()
    index_frame = pd.read_parquet(index_path)
    assert len(index_frame) == 2
    assert sorted(index_frame["side"].tolist()) == ["down", "up"]


def test_preflight_orderbook_index_dates_rebuilds_under_lock(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-04"),
        [
            {
                "logged_at": "2026-03-04T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-04T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 8.0]],
            }
        ],
    )

    lock_calls: list[str] = []

    @contextmanager
    def _fake_lock(path: Path):
        lock_calls.append(str(path))
        yield

    monkeypatch.setattr(
        "pm15min.research.backtests.data_surface_fallback._exclusive_lock",
        _fake_lock,
    )

    summary = preflight_orderbook_index_dates(
        data_cfg,
        date_strings=["2026-03-04"],
    )

    assert summary["rebuilt_dates"] == ["2026-03-04"]
    assert len(lock_calls) == 1
    assert lock_calls[0].endswith("sol-15m-2026-03-04.lock")
