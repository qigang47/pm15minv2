from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

import pm15min.research.backtests.engine as backtest_engine_module
import pm15min.research.backtests.fills as fills_module
import pm15min.research.backtests.orderbook_surface as orderbook_surface_module
from pm15min.research.backtests.depth_replay import DepthReplaySummary
from pm15min.research.backtests.engine import (
    _load_scoped_backtest_feature_frame,
    _load_scoped_backtest_label_frame,
    _scope_backtest_klines,
)
from pm15min.research.config import ResearchConfig


def _research_cfg(root: Path, *, target: str = "reversal") -> ResearchConfig:
    return ResearchConfig.build(
        market="eth",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="focus_eth_test",
        label_set="truth",
        target=target,
        model_family="deep_otm",
        root=root,
    )


def _bundle_dir(tmp_path: Path, name: str, *, offset_to_columns: dict[int, list[str]]) -> Path:
    bundle_dir = tmp_path / name
    for offset, columns in offset_to_columns.items():
        offset_dir = bundle_dir / "offsets" / f"offset={offset}"
        offset_dir.mkdir(parents=True, exist_ok=True)
        joblib.dump(columns, offset_dir / "feature_cols.joblib")
    return bundle_dir


def test_load_scoped_backtest_feature_frame_limits_columns_offsets_and_window(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = _research_cfg(tmp_path)
    primary_bundle = _bundle_dir(tmp_path, "bundle-primary", offset_to_columns={7: ["feat_a"], 8: ["feat_b"]})
    seen: dict[str, object] = {}

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None):
        seen["feature_set"] = feature_set
        seen["columns"] = tuple(columns or ())
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-27T23:59:00Z",
                    "cycle_start_ts": "2026-03-27T23:45:00Z",
                    "cycle_end_ts": "2026-03-28T00:00:00Z",
                    "offset": 7,
                    "feat_a": 1.0,
                    "ret_from_strike": 0.1,
                    "ret_from_cycle_open": 0.2,
                },
                {
                    "decision_ts": "2026-03-28T00:01:00Z",
                    "cycle_start_ts": "2026-03-28T00:00:00Z",
                    "cycle_end_ts": "2026-03-28T00:15:00Z",
                    "offset": 7,
                    "feat_a": 2.0,
                    "ret_from_strike": 0.3,
                    "ret_from_cycle_open": 0.4,
                },
                {
                    "decision_ts": "2026-03-28T00:02:00Z",
                    "cycle_start_ts": "2026-03-28T00:00:00Z",
                    "cycle_end_ts": "2026-03-28T00:15:00Z",
                    "offset": 9,
                    "feat_a": 3.0,
                    "ret_from_strike": 0.5,
                    "ret_from_cycle_open": 0.6,
                },
                {
                    "decision_ts": "2026-03-29T00:01:00Z",
                    "cycle_start_ts": "2026-03-29T00:00:00Z",
                    "cycle_end_ts": "2026-03-29T00:15:00Z",
                    "offset": 8,
                    "feat_b": 4.0,
                    "ret_from_strike": 0.7,
                    "ret_from_cycle_open": 0.8,
                },
            ]
        )

    monkeypatch.setattr("pm15min.research.backtests.engine.load_feature_frame", _fake_load_feature_frame)

    scoped = _load_scoped_backtest_feature_frame(
        cfg=cfg,
        feature_set="focus_eth_test",
        bundle_dirs=(primary_bundle,),
        targets=("reversal",),
        available_offsets=[7, 8],
        decision_start="2026-03-28",
        decision_end="2026-03-28",
    )

    assert seen["feature_set"] == "focus_eth_test"
    assert set(seen["columns"]) == {
        "decision_ts",
        "cycle_start_ts",
        "cycle_end_ts",
        "offset",
        "feat_a",
        "feat_b",
        "ret_from_strike",
        "ret_from_cycle_open",
    }
    assert scoped["offset"].tolist() == [7]
    assert scoped["decision_ts"].tolist() == ["2026-03-28T00:01:00Z"]


def test_load_scoped_backtest_label_frame_limits_to_scoped_feature_cycles(
    tmp_path: Path,
    monkeypatch,
) -> None:
    cfg = _research_cfg(tmp_path)
    seen: dict[str, object] = {}
    scoped_features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
            }
        ]
    )

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None):
        seen["label_set"] = label_set
        seen["columns"] = tuple(columns or ())
        return pd.DataFrame(
            [
                {
                    "asset": "eth",
                    "cycle_start_ts": int(pd.Timestamp("2026-03-28T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-28T00:15:00Z").timestamp()),
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "label_set": "truth",
                    "settlement_source": "settlement_truth",
                    "label_source": "settlement_truth",
                    "resolved": True,
                    "price_to_beat": 100.0,
                    "final_price": 101.0,
                    "winner_side": "UP",
                    "direction_up": 1.0,
                    "full_truth": True,
                },
                {
                    "asset": "eth",
                    "cycle_start_ts": int(pd.Timestamp("2026-03-29T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-29T00:15:00Z").timestamp()),
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "label_set": "truth",
                    "settlement_source": "settlement_truth",
                    "label_source": "settlement_truth",
                    "resolved": True,
                    "price_to_beat": 100.0,
                    "final_price": 99.0,
                    "winner_side": "DOWN",
                    "direction_up": 0.0,
                    "full_truth": True,
                },
            ]
        )

    monkeypatch.setattr("pm15min.research.backtests.engine.load_label_frame", _fake_load_label_frame)

    scoped = _load_scoped_backtest_label_frame(
        cfg=cfg,
        label_set="truth",
        scoped_features=scoped_features,
    )

    assert seen["label_set"] == "truth"
    assert set(seen["columns"]) == {
        "asset",
        "cycle_start_ts",
        "cycle_end_ts",
        "market_id",
        "condition_id",
        "label_set",
        "settlement_source",
        "label_source",
        "resolved",
        "price_to_beat",
        "final_price",
        "winner_side",
        "direction_up",
        "full_truth",
    }
    assert scoped["market_id"].tolist() == ["m-1"]
    assert scoped["winner_side"].tolist() == ["UP"]


def test_scope_backtest_klines_keeps_required_history_for_liquidity_and_returns() -> None:
    raw_klines = pd.DataFrame(
        {
            "open_time": pd.date_range("2026-03-27T20:00:00Z", periods=600, freq="min", tz="UTC"),
            "close": [100.0 + idx for idx in range(600)],
            "quote_asset_volume": [1_000.0] * 600,
            "number_of_trades": [100] * 600,
        }
    )

    scoped = _scope_backtest_klines(
        raw_klines,
        decision_start="2026-03-28T03:00:00Z",
        decision_end="2026-03-28T03:10:00Z",
        required_lookback_minutes=210,
    )

    assert pd.Timestamp(scoped["open_time"].min()) == pd.Timestamp("2026-03-27T23:30:00Z")
    assert pd.Timestamp(scoped["open_time"].max()) == pd.Timestamp("2026-03-28T03:10:00Z")


def test_build_depth_candidate_lookup_materializes_candidates_lazily(monkeypatch) -> None:
    depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
                "depth_candidate_total_count": 2,
                "depth_up_record": {"asks": [[0.2, 10.0]]},
                "depth_down_record": {"asks": [[0.8, 10.0]]},
            },
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 2,
                "depth_candidate_total_count": 2,
                "depth_up_record": {"asks": [[0.21, 9.0]]},
                "depth_down_record": {"asks": [[0.79, 9.0]]},
            },
        ]
    )
    call_count = {"value": 0}
    original_to_dict = pd.DataFrame.to_dict

    def _counting_to_dict(self, *args, **kwargs):
        call_count["value"] += 1
        return original_to_dict(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_dict", _counting_to_dict)

    lookup = fills_module.build_depth_candidate_lookup(depth_replay)

    assert call_count["value"] == 0
    candidates = lookup.get(
        ("2026-03-28T00:01:00+00:00", "2026-03-28T00:00:00+00:00", "2026-03-28T00:15:00+00:00", 7)
    )
    assert call_count["value"] == 1
    assert [item["depth_snapshot_rank"] for item in candidates] == [1, 2]


def test_prepare_orderbook_lookup_uses_row_positions_without_group_dataframe_copies() -> None:
    frame = pd.DataFrame(
        [
            {
                "captured_ts_ms": 1000,
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "best_ask": 0.41,
                "best_bid": 0.39,
            },
            {
                "captured_ts_ms": 1005,
                "market_id": "m-1",
                "token_id": "tok-down",
                "side": "down",
                "best_ask": 0.59,
                "best_bid": 0.57,
            },
            {
                "captured_ts_ms": 1010,
                "market_id": "m-1",
                "token_id": "tok-up",
                "side": "up",
                "best_ask": 0.42,
                "best_bid": 0.40,
            },
        ]
    )

    prepared_frame, token_lookup, market_side_lookup = orderbook_surface_module._prepare_orderbook_lookup(frame)

    assert not isinstance(token_lookup[("m-1", "tok-up", "up")], pd.DataFrame)
    row = orderbook_surface_module._resolve_side_row(
        prepared_frame,
        market_id="m-1",
        token_id="tok-up",
        side="up",
        decision_ts_ms=1008,
        token_lookup=token_lookup,
        market_side_lookup=market_side_lookup,
    )

    assert row is not None
    assert float(row["best_ask"]) == 0.41


def test_build_decision_depth_runtime_preserves_full_snapshot_window_when_refresh_enabled(monkeypatch) -> None:
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
            },
            {
                "decision_ts": "2026-03-28T00:02:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
            },
        ]
    )
    seen: dict[str, object] = {}

    def _fake_build_raw_depth_replay_frame(*, replay, data_cfg, max_snapshots_per_replay_row=None, heartbeat=None):
        seen["rows"] = len(replay)
        seen["cap"] = max_snapshots_per_replay_row
        return pd.DataFrame(), DepthReplaySummary(
            market_rows_loaded=0,
            replay_rows=len(replay),
            source_files_scanned=0,
            raw_records_scanned=0,
            raw_record_matches=0,
            snapshot_rows=0,
            complete_snapshot_rows=0,
            partial_snapshot_rows=0,
            decision_key_snapshot_rows=0,
            token_window_snapshot_rows=0,
            mixed_strategy_snapshot_rows=0,
            replay_rows_with_snapshots=0,
            replay_rows_without_snapshots=len(replay),
        )

    monkeypatch.setattr(backtest_engine_module, "build_raw_depth_replay_frame", _fake_build_raw_depth_replay_frame)

    _depth_replay, summary, lookup = backtest_engine_module._build_decision_depth_runtime(
        replay=replay,
        data_cfg=None,
        fill_config=fills_module.BacktestFillConfig(raw_depth_fak_refresh_enabled=True),
    )

    assert seen == {"rows": 2, "cap": None}
    assert summary.replay_rows == 2
    assert len(lookup) == 0


def test_build_fill_depth_runtime_skips_scan_when_no_accepted_rows(monkeypatch) -> None:
    accepted = pd.DataFrame(
        columns=["decision_ts", "cycle_start_ts", "cycle_end_ts", "offset", "market_id", "token_up", "token_down"]
    )
    called = {"value": False}

    def _fake_build_raw_depth_replay_frame(*, replay, data_cfg, max_snapshots_per_replay_row=None, heartbeat=None):
        called["value"] = True
        return pd.DataFrame(), DepthReplaySummary(
            market_rows_loaded=0,
            replay_rows=0,
            source_files_scanned=0,
            raw_records_scanned=0,
            raw_record_matches=0,
            snapshot_rows=0,
            complete_snapshot_rows=0,
            partial_snapshot_rows=0,
            decision_key_snapshot_rows=0,
            token_window_snapshot_rows=0,
            mixed_strategy_snapshot_rows=0,
            replay_rows_with_snapshots=0,
            replay_rows_without_snapshots=0,
        )

    monkeypatch.setattr(backtest_engine_module, "build_raw_depth_replay_frame", _fake_build_raw_depth_replay_frame)

    depth_replay, summary, lookup = backtest_engine_module._build_fill_depth_runtime(
        accepted=accepted,
        data_cfg=None,
    )

    assert called["value"] is False
    assert depth_replay.empty
    assert summary.replay_rows == 0
    assert len(lookup) == 0
