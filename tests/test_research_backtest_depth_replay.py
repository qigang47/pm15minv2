from __future__ import annotations

from pathlib import Path

import pandas as pd

from pm15min.data.config import DataConfig
from pm15min.data.io.ndjson_zst import append_ndjson_zst
from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.backtests.depth_replay import build_raw_depth_replay_frame


def test_build_raw_depth_replay_frame_pairs_legacy_multi_snapshot_buckets(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    heartbeats: list[str] = []
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:00.300000+00:00",
                "orderbook_ts": "2026-03-01T00:05:00.310000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 6.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:00.300000+00:00",
                "orderbook_ts": "2026-03-01T00:05:00.320000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:01.200000+00:00",
                "orderbook_ts": "2026-03-01T00:05:01.210000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.42, 11.0]],
                "bids": [[0.40, 7.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:01.200000+00:00",
                "orderbook_ts": "2026-03-01T00:05:01.220000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.60, 8.0]],
                "bids": [[0.56, 4.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:02.000000+00:00",
                "orderbook_ts": "2026-03-01T00:05:02.010000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.43, 12.0]],
                "bids": [[0.41, 8.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:03.000000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 8,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.99, 1.0]],
                "bids": [[0.01, 1.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "token_up": "tok-up",
                "token_down": "tok-down",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        heartbeat=heartbeats.append,
    )

    assert len(out) == 3
    assert out["depth_snapshot_rank"].tolist() == [1, 2, 3]
    assert out["depth_candidate_total_count"].tolist() == [3, 3, 3]
    assert out["depth_snapshot_status"].tolist() == ["ok", "ok", "partial"]
    assert out["depth_snapshot_reason"].tolist() == ["", "", "down_snapshot_missing"]
    assert out["depth_match_strategy"].tolist() == ["decision_key", "decision_key", "decision_key"]
    assert out["depth_snapshot_ts_ms"].tolist() == [
        int(pd.Timestamp("2026-03-01T00:05:00.300000Z").timestamp() * 1000),
        int(pd.Timestamp("2026-03-01T00:05:01.200000Z").timestamp() * 1000),
        int(pd.Timestamp("2026-03-01T00:05:02.000000Z").timestamp() * 1000),
    ]
    assert out.iloc[0]["depth_up_record"]["asks"][0] == [0.41, 10.0]
    assert out.iloc[1]["depth_down_record"]["asks"][0] == [0.60, 8.0]
    assert out.iloc[2]["depth_down_record"] is None
    assert summary.raw_records_scanned == 6
    assert summary.raw_record_matches == 5
    assert summary.snapshot_rows == 3
    assert summary.complete_snapshot_rows == 2
    assert summary.partial_snapshot_rows == 1
    assert summary.decision_key_snapshot_rows == 3
    assert summary.token_window_snapshot_rows == 0
    assert summary.replay_rows_with_snapshots == 1
    assert summary.replay_rows_without_snapshots == 0
    assert heartbeats == ["Scanning depth replay file 1/1: 2026-03-01"]


def test_build_raw_depth_replay_frame_matches_legacy_one_minute_shifted_decision_key(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:07:00.100000+00:00",
                "market_id": "m-shift",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:07:00Z",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 6.0]],
            },
            {
                "logged_at": "2026-03-01T00:07:00.100000+00:00",
                "market_id": "m-shift",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:07:00Z",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 5.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-shift",
                "token_up": "tok-up",
                "token_down": "tok-down",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
    )

    assert len(out) == 1
    assert out.iloc[0]["depth_match_strategy"] == "decision_key"
    assert out.iloc[0]["depth_snapshot_status"] == "ok"
    assert summary.raw_record_matches == 2
    assert summary.replay_rows_with_snapshots == 1


def test_build_raw_depth_replay_frame_scans_previous_day_for_shifted_midnight_key(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T23:59:00.100000+00:00",
                "market_id": "m-midnight",
                "token_id": "tok-up",
                "side": "up",
                "offset": 0,
                "decision_ts": "2026-03-01T23:59:00Z",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 6.0]],
            },
            {
                "logged_at": "2026-03-01T23:59:00.100000+00:00",
                "market_id": "m-midnight",
                "token_id": "tok-down",
                "side": "down",
                "offset": 0,
                "decision_ts": "2026-03-01T23:59:00Z",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 5.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-02T00:00:00Z",
                "cycle_start_ts": "2026-03-01T23:45:00Z",
                "cycle_end_ts": "2026-03-02T00:00:00Z",
                "offset": 0,
                "market_id": "m-midnight",
                "token_up": "tok-up",
                "token_down": "tok-down",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
    )

    assert len(out) == 1
    assert out.iloc[0]["depth_match_strategy"] == "decision_key"
    assert summary.source_files_scanned == 1
    assert summary.replay_rows_with_snapshots == 1


def test_build_raw_depth_replay_frame_caps_retained_snapshots_per_replay_row(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:00.300000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 6.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:00.300000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:01.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.42, 11.0]],
                "bids": [[0.40, 7.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:01.200000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.60, 8.0]],
                "bids": [[0.56, 4.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:02.000000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.43, 12.0]],
                "bids": [[0.41, 8.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "token_up": "tok-up",
                "token_down": "tok-down",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        max_snapshots_per_replay_row=2,
    )

    assert len(out) == 2
    assert out["depth_snapshot_rank"].tolist() == [1, 2]
    assert out["depth_candidate_total_count"].tolist() == [3, 3]
    assert summary.raw_record_matches == 5
    assert summary.snapshot_rows == 2


def test_build_raw_depth_replay_frame_matches_legacy_raw_decision_ts_backshift(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:07:00.300000+00:00",
                "orderbook_ts": "2026-03-01T00:07:00.310000+00:00",
                "market_id": "m-legacy",
                "token_id": "tok-up-legacy",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:07:00Z",
                "asks": [[0.41, 10.0]],
                "bids": [[0.39, 6.0]],
            },
            {
                "logged_at": "2026-03-01T00:07:00.300000+00:00",
                "orderbook_ts": "2026-03-01T00:07:00.320000+00:00",
                "market_id": "m-legacy",
                "token_id": "tok-down-legacy",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:07:00Z",
                "asks": [[0.59, 9.0]],
                "bids": [[0.57, 5.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:08:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-legacy",
                "token_up": "tok-up-legacy",
                "token_down": "tok-down-legacy",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["depth_snapshot_status"] == "ok"
    assert row["depth_match_strategy"] == "decision_key"
    assert row["depth_candidate_total_count"] == 1
    assert row["depth_up_record"] == {"asks": [[0.41, 10.0]]}
    assert row["depth_down_record"] == {"asks": [[0.59, 9.0]]}
    assert row["depth_up_snapshot_ts_ms"] == int(pd.Timestamp("2026-03-01T00:07:00.310000+00:00").timestamp() * 1000)
    assert row["depth_down_snapshot_ts_ms"] == int(pd.Timestamp("2026-03-01T00:07:00.320000+00:00").timestamp() * 1000)
    assert summary.raw_record_matches == 2
    assert summary.snapshot_rows == 1
    assert summary.replay_rows_with_snapshots == 1
    assert summary.replay_rows_without_snapshots == 0


def test_build_raw_depth_replay_frame_uses_token_window_fallback_with_catalog_metadata(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "token_up": "tok-up-2",
                    "token_down": "tok-down-2",
                    "question": "SOL up?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:04:58.000000+00:00",
                "market_id": "m-2",
                "token_id": "tok-up-2",
                "side": "up",
                "asks": [[0.30, 10.0]],
                "bids": [[0.29, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-2",
                "token_id": "tok-up-2",
                "side": "up",
                "asks": [[0.31, 9.0]],
                "bids": [[0.30, 4.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-2",
                "token_id": "tok-down-2",
                "side": "down",
                "asks": [[0.69, 8.0]],
                "bids": [[0.68, 3.0]],
            },
            {
                "logged_at": "2026-03-01T00:07:30.000000+00:00",
                "market_id": "m-2",
                "token_id": "tok-down-2",
                "side": "down",
                "asks": [[0.80, 2.0]],
                "bids": [[0.79, 1.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        snapshot_tolerance_ms=60_000,
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["market_id"] == "m-2"
    assert row["token_up"] == "tok-up-2"
    assert row["token_down"] == "tok-down-2"
    assert row["question"] == "SOL up?"
    assert row["depth_snapshot_status"] == "ok"
    assert row["depth_match_strategy"] == "token_window"
    assert row["depth_snapshot_ts_ms"] == int(pd.Timestamp("2026-03-01T00:05:20Z").timestamp() * 1000)
    assert row["depth_up_record"]["asks"][0] == [0.31, 9.0]
    assert row["depth_down_record"]["asks"][0] == [0.69, 8.0]
    assert summary.market_rows_loaded == 1
    assert summary.raw_records_scanned == 4
    assert summary.raw_record_matches == 2
    assert summary.snapshot_rows == 1
    assert summary.complete_snapshot_rows == 1
    assert summary.partial_snapshot_rows == 0
    assert summary.decision_key_snapshot_rows == 0
    assert summary.token_window_snapshot_rows == 1
    assert summary.replay_rows_with_snapshots == 1


def test_build_raw_depth_replay_frame_backfills_blank_market_id_from_cycle_catalog(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-3",
                    "condition_id": "c-3",
                    "token_up": "tok-up-3",
                    "token_down": "tok-down-3",
                    "question": "SOL higher?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                }
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-3",
                "token_id": "tok-up-3",
                "side": "up",
                "asks": [[0.30, 10.0]],
                "bids": [[0.29, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-3",
                "token_id": "tok-down-3",
                "side": "down",
                "asks": [[0.70, 8.0]],
                "bids": [[0.69, 3.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "",
                "condition_id": "",
                "token_up": "",
                "token_down": "",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        snapshot_tolerance_ms=60_000,
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["market_id"] == "m-3"
    assert row["condition_id"] == "c-3"
    assert row["token_up"] == "tok-up-3"
    assert row["token_down"] == "tok-down-3"
    assert row["question"] == "SOL higher?"
    assert row["depth_snapshot_status"] == "ok"
    assert row["depth_match_strategy"] == "token_window"
    assert summary.replay_rows_with_snapshots == 1


def test_build_raw_depth_replay_frame_does_not_copy_unrelated_replay_columns_into_each_snapshot(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.30, 10.0]],
                "bids": [[0.29, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.70, 8.0]],
                "bids": [[0.69, 3.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "token_up": "tok-up",
                "token_down": "tok-down",
                "ret_1m": 0.123,
                "ret_5m": 0.456,
                "p_up": 0.78,
                "p_down": 0.22,
                "quote_up_ask": 0.30,
                "quote_down_ask": 0.70,
                "some_large_feature_blob": "x" * 500,
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        snapshot_tolerance_ms=60_000,
    )

    assert len(out) == 1
    assert summary.replay_rows_with_snapshots == 1
    assert "ret_1m" not in out.columns
    assert "ret_5m" not in out.columns
    assert "p_up" not in out.columns
    assert "p_down" not in out.columns
    assert "quote_up_ask" not in out.columns
    assert "quote_down_ask" not in out.columns
    assert "some_large_feature_blob" not in out.columns
    assert out["offset"].tolist() == [7]


def test_build_raw_depth_replay_frame_compacts_snapshot_records_to_asks_only(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "orderbook_ts": "2026-03-01T00:05:20.050000+00:00",
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.30, 10.0]],
                "bids": [[0.29, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "orderbook_ts": "2026-03-01T00:05:20.060000+00:00",
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "offset": 7,
                "decision_ts": "2026-03-01T00:05:00Z",
                "asks": [[0.70, 8.0]],
                "bids": [[0.69, 3.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "token_up": "tok-up",
                "token_down": "tok-down",
            }
        ]
    )

    out, _summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        snapshot_tolerance_ms=60_000,
    )

    assert out.iloc[0]["depth_up_record"] == {"asks": [[0.30, 10.0]]}
    assert out.iloc[0]["depth_down_record"] == {"asks": [[0.70, 8.0]]}


def test_build_raw_depth_replay_frame_prefers_market_id_catalog_match_before_cycle_fallback(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    write_parquet_atomic(
        pd.DataFrame(
            [
                {
                    "market_id": "m-good",
                    "condition_id": "c-good",
                    "token_up": "tok-up-good",
                    "token_down": "tok-down-good",
                    "question": "SOL good?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                },
                {
                    "market_id": "m-bad",
                    "condition_id": "c-bad",
                    "token_up": "tok-up-bad",
                    "token_down": "tok-down-bad",
                    "question": "SOL bad?",
                    "cycle_start_ts": 1_772_323_200,
                    "cycle_end_ts": 1_772_324_100,
                },
            ]
        ),
        data_cfg.layout.market_catalog_table_path,
    )
    append_ndjson_zst(
        data_cfg.layout.orderbook_depth_path("2026-03-01"),
        [
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-good",
                "token_id": "tok-up-good",
                "side": "up",
                "asks": [[0.30, 10.0]],
                "bids": [[0.29, 5.0]],
            },
            {
                "logged_at": "2026-03-01T00:05:20.000000+00:00",
                "market_id": "m-good",
                "token_id": "tok-down-good",
                "side": "down",
                "asks": [[0.70, 8.0]],
                "bids": [[0.69, 3.0]],
            },
        ],
    )
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:05:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "market_id": "m-good",
                "condition_id": "",
                "token_up": "",
                "token_down": "",
            }
        ]
    )

    out, summary = build_raw_depth_replay_frame(
        replay=replay,
        data_cfg=data_cfg,
        snapshot_tolerance_ms=60_000,
    )

    assert len(out) == 1
    row = out.iloc[0]
    assert row["market_id"] == "m-good"
    assert row["token_up"] == "tok-up-good"
    assert row["token_down"] == "tok-down-good"
    assert row["question"] == "SOL good?"
    assert row["depth_snapshot_status"] == "ok"
    assert summary.replay_rows_with_snapshots == 1
