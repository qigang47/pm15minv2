from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pandas as pd
import pytest

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.data_surface_fallback import preflight_orderbook_index_dates
from pm15min.research.backtests.engine import _assert_orderbook_preflight_is_usable


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


def test_preflight_orderbook_index_dates_prefers_live_surface_when_backtest_source_is_empty(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    backtest_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    live_cfg = DataConfig.build(market="sol", cycle="15m", surface="live", root=root)

    empty_depth_path = backtest_cfg.layout.orderbook_depth_path("2026-03-05")
    empty_depth_path.parent.mkdir(parents=True, exist_ok=True)
    empty_depth_path.write_bytes(b"\x28\xb5\x2f\xfd\x00\x58\x69\x00\x00\x65\x6d\x70\x74")

    append_ndjson_zst(
        live_cfg.layout.orderbook_depth_path("2026-03-05"),
        [
            {
                "logged_at": "2026-03-05T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-05T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 8.0]],
            }
        ],
    )

    summary = preflight_orderbook_index_dates(backtest_cfg, date_strings=["2026-03-05"])

    assert summary["used_live_surface_dates"] == ["2026-03-05"]
    assert summary["status_counts"] == {"rebuilt": 1}


def test_assert_orderbook_preflight_is_usable_raises_on_empty_or_missing_dates() -> None:
    with pytest.raises(RuntimeError, match="backtest_orderbook_coverage_incomplete"):
        _assert_orderbook_preflight_is_usable(
            {
                "empty_depth_source_dates": ["2026-03-19"],
                "missing_depth_dates": ["2026-03-20"],
            }
        )


def test_assert_orderbook_preflight_is_usable_raises_on_partial_market_coverage() -> None:
    with pytest.raises(RuntimeError, match="partial_market_coverage_dates"):
        _assert_orderbook_preflight_is_usable(
            {
                "partial_market_coverage_dates": ["2026-03-23"],
            }
        )


def test_preflight_orderbook_index_dates_flags_empty_depth_even_when_stale_index_exists(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    empty_depth_path = data_cfg.layout.orderbook_depth_path("2026-03-06")
    empty_depth_path.parent.mkdir(parents=True, exist_ok=True)
    empty_depth_path.write_bytes(b"\x28\xb5\x2f\xfd\x00\x58\x69\x00\x00\x65\x6d\x70\x74")

    index_path = data_cfg.layout.orderbook_index_path("2026-03-06")
    index_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "captured_ts_ms": 1,
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
            }
        ]
    ).to_parquet(index_path)

    summary = preflight_orderbook_index_dates(data_cfg, date_strings=["2026-03-06"])

    assert summary["empty_depth_source_dates"] == ["2026-03-06"]
    assert summary["status_counts"] == {"empty_depth_source": 1}


def test_preflight_orderbook_index_dates_flags_partial_market_coverage(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up-1",
                "token_down": "tok-down-1",
                "question": "SOL 1?",
                "cycle_start_ts": 1_772_841_600,
                "cycle_end_ts": 1_772_842_500,
            },
            {
                "market_id": "m-2",
                "condition_id": "c-2",
                "token_up": "tok-up-2",
                "token_down": "tok-down-2",
                "question": "SOL 2?",
                "cycle_start_ts": 1_772_842_500,
                "cycle_end_ts": 1_772_843_400,
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )

    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-07"),
        [
            {
                "logged_at": "2026-03-07T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-07T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up-1",
                "side": "up",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 8.0]],
            }
        ],
    )

    summary = preflight_orderbook_index_dates(
        data_cfg,
        date_strings=["2026-03-07"],
    )

    assert summary["partial_market_coverage_dates"] == ["2026-03-07"]
    assert summary["status_counts"] == {"partial_market_coverage": 1}
    detail = summary["details"][0]
    assert detail["expected_market_id_count"] == 2
    assert detail["index_market_id_count"] == 1
    assert detail["missing_market_id_count"] == 1


def test_preflight_orderbook_index_dates_limits_coverage_check_to_replay_markets(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)

    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "token_up": "tok-up-1",
                    "token_down": "tok-down-1",
                    "question": "SOL 1?",
                    "cycle_start_ts": 1_772_841_600,
                    "cycle_end_ts": 1_772_842_500,
                },
                {
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "token_up": "tok-up-2",
                    "token_down": "tok-down-2",
                    "question": "SOL 2?",
                    "cycle_start_ts": 1_772_842_500,
                    "cycle_end_ts": 1_772_843_400,
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )

    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-07"),
        [
            {
                "logged_at": "2026-03-07T00:05:00.100000+00:00",
                "orderbook_ts": "2026-03-07T00:05:00.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up-1",
                "side": "up",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 8.0]],
            }
        ],
    )

    summary = preflight_orderbook_index_dates(
        data_cfg,
        date_strings=["2026-03-07"],
        expected_market_ids_by_date={"2026-03-07": {"m-1"}},
    )

    assert summary["ready_dates"] == ["2026-03-07"]
    assert summary["partial_market_coverage_dates"] == []
    assert summary["status_counts"] == {"rebuilt": 1}
    detail = summary["details"][0]
    assert detail["expected_market_id_count"] == 1
    assert detail["missing_market_id_count"] == 0
