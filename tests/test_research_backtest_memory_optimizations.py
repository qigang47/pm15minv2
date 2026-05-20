from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

import pm15min.research.backtests.engine as backtest_engine_module
import pm15min.research.backtests.fills as fills_module
import pm15min.research.backtests.orderbook_surface as orderbook_surface_module
from pm15min.research.backtests.depth_replay import DepthReplaySummary
from pm15min.research.backtests.engine import (
    _compact_runtime_surface_frame,
    _compact_replay_for_runtime,
    _factor_source_frame,
    _load_scoped_backtest_feature_frame,
    _narrow_depth_runtime_to_accepted_decisions,
    _narrow_factor_source_to_accepted_decisions,
    _merge_factor_columns_for_traded_decisions,
    _load_scoped_backtest_label_frame,
    _merge_cached_runtime_surface,
    _serialize_decision_frame,
    _release_decision_depth_runtime_after_fill_resolution,
    _scope_backtest_klines,
    _surface_input_signature,
)
from pm15min.research.backtests.decision_engine_parity import (
    DecisionEngineParityConfig,
    apply_decision_engine_parity,
)
from pm15min.research.backtests.retry_contract import attach_pre_submit_orderbook_retry_contract
from pm15min.live.profiles import resolve_live_profile_spec
from pm15min.research.config import ResearchConfig
from pm15min.research.datasets.loaders import load_feature_frame
from pm15min.research.labels.alignment import merge_feature_and_label_frames


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

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None, filters=None):
        seen["feature_set"] = feature_set
        seen["columns"] = tuple(columns or ())
        seen["filters"] = filters
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
    assert seen["filters"] == [
        ("decision_ts", ">=", pd.Timestamp("2026-03-28T00:00:00Z")),
        ("decision_ts", "<", pd.Timestamp("2026-03-29T00:00:00Z")),
    ]
    assert scoped["offset"].tolist() == [7]
    assert scoped["decision_ts"].tolist() == ["2026-03-28T00:01:00Z"]


def test_compact_runtime_surface_frame_drops_score_columns_but_keeps_reusable_surface() -> None:
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:07:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
                "p_up": 0.71,
                "p_down": 0.29,
                "feature_a": 123.0,
                "quote_status": "ok",
                "liquidity_status": "ok",
            }
        ]
    )

    compact = _compact_runtime_surface_frame(replay)
    merged = _merge_cached_runtime_surface(
        replay.assign(p_up=0.62, p_down=0.38, quote_status="stale", liquidity_status="stale"),
        compact,
    )

    assert set(compact.columns) == {
        "decision_ts",
        "cycle_start_ts",
        "cycle_end_ts",
        "offset",
        "market_id",
        "condition_id",
        "quote_status",
        "liquidity_status",
    }
    assert "p_up" not in compact.columns
    assert "feature_a" not in compact.columns
    assert merged.loc[0, "p_up"] == 0.62
    assert merged.loc[0, "quote_status"] == "ok"
    assert merged.loc[0, "liquidity_status"] == "ok"


def test_compact_replay_for_runtime_drops_bulk_factors_but_preserves_factor_source() -> None:
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:07:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
                "resolved": True,
                "winner_side": "UP",
                "p_up": 0.71,
                "p_down": 0.29,
                "score_valid": True,
                "score_present": True,
                "bundle_offset_available": True,
                "ret_1m": 0.01,
                "feature_blob": "x" * 100,
            }
        ]
    )

    factor_source = _factor_source_frame(replay, factor_columns=("ret_1m", "feature_blob"))
    compact = _compact_replay_for_runtime(replay)
    decisions = compact.assign(policy_action=["trade"], decision_source=["primary"])
    factor_decisions = _merge_factor_columns_for_traded_decisions(
        decisions,
        factor_source_frame=factor_source,
    )

    assert "p_up" in compact.columns
    assert "winner_side" in compact.columns
    assert "ret_1m" not in compact.columns
    assert "feature_blob" not in compact.columns
    assert factor_source["ret_1m"].tolist() == [0.01]
    assert factor_decisions["ret_1m"].tolist() == [0.01]
    assert factor_decisions["feature_blob"].tolist() == ["x" * 100]


def test_serialize_decision_frame_uses_shallow_copy(monkeypatch) -> None:
    decisions = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:07:00Z",
                "guard_reasons": ["quote_missing"],
                "quote_metrics": {"ask": 0.42},
                "bulk_payload": "x" * 10_000,
            }
        ]
    )
    original_copy = pd.DataFrame.copy
    calls: list[bool | None] = []

    def _track_copy(self, *args, **kwargs):
        calls.append(kwargs.get("deep"))
        if kwargs.get("deep", True) is not False and "bulk_payload" in self.columns:
            raise AssertionError("decision serialization should not deep-copy bulk columns")
        return original_copy(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "copy", _track_copy)

    serialized = _serialize_decision_frame(decisions)

    assert calls == [False]
    assert serialized.loc[0, "guard_reasons"] == '["quote_missing"]'
    assert serialized.loc[0, "quote_metrics"] == '{"ask": 0.42}'
    assert decisions.loc[0, "guard_reasons"] == ["quote_missing"]


def test_decision_surface_helpers_use_shallow_copy(monkeypatch) -> None:
    rows = pd.DataFrame(
        [
            {
                "offset": 7,
                "p_up": 0.72,
                "p_down": 0.28,
                "quote_up_ask": 0.42,
                "quote_down_ask": 0.58,
                "decision_engine_reason": "orderbook_limit_reject",
                "bulk_payload": "x" * 10_000,
            }
        ]
    )
    original_copy = pd.DataFrame.copy
    calls: list[bool | None] = []

    def _track_copy(self, *args, **kwargs):
        calls.append(kwargs.get("deep"))
        if kwargs.get("deep", True) is not False and "bulk_payload" in self.columns:
            raise AssertionError("decision helpers should not deep-copy bulk columns")
        return original_copy(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "copy", _track_copy)

    parity = apply_decision_engine_parity(rows, config=DecisionEngineParityConfig(min_dir_prob_default=0.55))
    retry = attach_pre_submit_orderbook_retry_contract(
        rows,
        spec=resolve_live_profile_spec("deep_otm_baseline"),
    )

    assert calls == [False, False]
    assert parity.loc[0, "decision_engine_action"] == "trade"
    assert bool(retry.loc[0, "pre_submit_orderbook_retry_armed"]) is True


def test_build_bundle_replay_uses_single_label_merge(tmp_path: Path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle"
    (bundle_dir / "offsets" / "offset=7").mkdir(parents=True)
    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:07:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
            }
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "eth",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
            }
        ]
    )
    merge_calls = {"count": 0}

    def _count_merge(feature_frame, label_frame):
        merge_calls["count"] += 1
        return merge_feature_and_label_frames(feature_frame, label_frame)

    def _fake_score_bundle_offset(_bundle_dir: Path, feature_frame: pd.DataFrame, *, offset: int) -> pd.DataFrame:
        assert "market_id" in feature_frame.columns
        assert offset == 7
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:07:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "p_up": 0.72,
                    "p_down": 0.28,
                    "score_valid": True,
                }
            ]
        )

    monkeypatch.setattr(backtest_engine_module, "merge_feature_and_label_frames", _count_merge)
    monkeypatch.setattr("pm15min.research.backtests.replay_loader.merge_feature_and_label_frames", _count_merge)
    monkeypatch.setattr(backtest_engine_module, "score_bundle_offset", _fake_score_bundle_offset)

    replay, summary, available_offsets = backtest_engine_module._build_bundle_replay(
        bundle_dir=bundle_dir,
        features=features,
        labels=labels,
    )

    assert merge_calls["count"] == 1
    assert available_offsets == [7]
    assert replay["market_id"].tolist() == ["m-1"]
    assert summary.feature_rows == 1


def test_surface_input_signature_avoids_materializing_csv(monkeypatch) -> None:
    replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:07:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            },
            {
                "decision_ts": "2026-03-28T00:08:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
                "market_id": "m-1",
                "condition_id": "c-1",
            },
        ]
    )

    def _fail_to_csv(self, *args, **kwargs):
        raise AssertionError("surface signature should not materialize a CSV copy")

    monkeypatch.setattr(pd.DataFrame, "to_csv", _fail_to_csv)

    first = _surface_input_signature(replay)
    second = _surface_input_signature(replay.iloc[::-1].reset_index(drop=True))

    assert first == second
    assert '"rows": 2' in first


def test_load_feature_frame_applies_timestamp_filters_to_timestamp_columns(tmp_path: Path) -> None:
    cfg = _research_cfg(tmp_path)
    feature_path = cfg.layout.feature_frame_path("focus_eth_test", source_surface=cfg.source_surface)
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(
                [
                    "2026-03-27T23:59:00Z",
                    "2026-03-28T00:01:00Z",
                    "2026-03-29T00:01:00Z",
                ],
                utc=True,
            ),
            "offset": [7, 7, 8],
            "feature_a": [1.0, 2.0, 3.0],
        }
    ).to_parquet(feature_path, index=False)

    out = load_feature_frame(
        cfg,
        feature_set="focus_eth_test",
        columns=["decision_ts", "offset", "feature_a"],
        filters=[
            ("decision_ts", ">=", pd.Timestamp("2026-03-28T00:00:00Z")),
            ("decision_ts", "<", pd.Timestamp("2026-03-29T00:00:00Z")),
        ],
    )

    assert out["decision_ts"].tolist() == [pd.Timestamp("2026-03-28T00:01:00Z")]
    assert out["offset"].tolist() == [7]


def test_load_feature_frame_applies_timestamp_filters_to_string_timestamp_columns(tmp_path: Path) -> None:
    cfg = _research_cfg(tmp_path)
    feature_path = cfg.layout.feature_frame_path("focus_eth_test", source_surface=cfg.source_surface)
    feature_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        {
            "decision_ts": [
                "2026-03-27T23:59:00Z",
                "2026-03-28T00:01:00Z",
                "2026-03-29T00:01:00Z",
            ],
            "offset": [7, 7, 8],
            "feature_a": [1.0, 2.0, 3.0],
        }
    ).to_parquet(feature_path, index=False)

    out = load_feature_frame(
        cfg,
        feature_set="focus_eth_test",
        columns=["decision_ts", "offset", "feature_a"],
        filters=[
            ("decision_ts", ">=", pd.Timestamp("2026-03-28T00:00:00Z")),
            ("decision_ts", "<", pd.Timestamp("2026-03-29T00:00:00Z")),
        ],
    )

    assert out["decision_ts"].tolist() == ["2026-03-28T00:01:00Z"]
    assert out["offset"].tolist() == [7]


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

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None, filters=None):
        seen["label_set"] = label_set
        seen["columns"] = tuple(columns or ())
        seen["filters"] = filters
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
    assert seen["filters"] == [
        ("cycle_start_ts", ">=", int(pd.Timestamp("2026-03-28T00:00:00Z").timestamp())),
        ("cycle_end_ts", "<=", int(pd.Timestamp("2026-03-28T00:15:00Z").timestamp())),
    ]
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


def test_load_scoped_backtest_klines_passes_time_filters(monkeypatch, tmp_path: Path) -> None:
    cfg = _research_cfg(tmp_path)
    seen: dict[str, object] = {}

    def _fake_load_binance_klines_1m(_data_cfg, symbol=None, *, columns=None, filters=None):
        seen["symbol"] = symbol
        seen["columns"] = tuple(columns or ())
        seen["filters"] = filters
        return pd.DataFrame(
            {
                "open_time": pd.date_range("2026-03-27T20:00:00Z", periods=600, freq="min", tz="UTC"),
                "close": [100.0 + idx for idx in range(600)],
                "quote_asset_volume": [1_000.0] * 600,
                "number_of_trades": [100] * 600,
            }
        )

    monkeypatch.setattr("pm15min.research.backtests.engine.load_binance_klines_1m", _fake_load_binance_klines_1m)

    scoped = backtest_engine_module._load_scoped_backtest_klines(
        data_cfg=None,
        decision_start="2026-03-28T03:00:00Z",
        decision_end="2026-03-28T03:10:00Z",
        required_lookback_minutes=210,
    )

    assert seen["symbol"] is None
    assert seen["columns"] == ()
    assert seen["filters"] == [
        ("open_time", ">=", pd.Timestamp("2026-03-27T23:30:00Z")),
        ("open_time", "<=", pd.Timestamp("2026-03-28T03:10:00Z")),
    ]
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


def test_build_proxy_fills_avoids_bulk_plan_record_materialization(monkeypatch) -> None:
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "quote_up_ask": 0.20,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.80,
                "p_down": 0.20,
                "decision_source": "primary",
            },
            {
                "decision_ts": "2026-03-28T00:02:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
                "quote_down_ask": 0.20,
                "quote_down_ask_size_1": 10.0,
                "p_up": None,
                "p_down": None,
                "decision_source": "secondary",
            },
        ]
    )
    original_to_dict = pd.DataFrame.to_dict

    def _guard_bulk_records(self, *args, **kwargs):
        orient = args[0] if args else kwargs.get("orient")
        if orient == "records":
            raise AssertionError("build_proxy_fills should stream planned rows instead of bulk to_dict(records)")
        return original_to_dict(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_dict", _guard_bulk_records)

    fills, rejects = fills_module.build_proxy_fills(
        accepted,
        config=fills_module.BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            prefer_depth=False,
        ),
    )

    assert len(fills) == 1
    assert rejects["reason"].tolist() == ["predicted_prob_missing"]
    assert rejects["decision_source"].tolist() == ["secondary"]


def test_build_proxy_fills_preserves_nullable_reject_decision_source() -> None:
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:02:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
                "quote_down_ask": 0.20,
                "quote_down_ask_size_1": 10.0,
                "p_up": None,
                "p_down": None,
                "decision_source": pd.NA,
            },
        ]
    )

    fills, rejects = fills_module.build_proxy_fills(
        accepted,
        config=fills_module.BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
            prefer_depth=False,
        ),
    )

    assert fills.empty
    assert rejects["decision_source"].tolist() == ["<NA>"]


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


def test_resolve_fill_depth_runtime_reuses_decision_depth_runtime(monkeypatch) -> None:
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
            }
        ]
    )
    decision_depth_replay = pd.DataFrame([{"decision_ts": "2026-03-28T00:01:00Z", "offset": 7}])
    decision_lookup = object()
    called = {"value": False}

    def _fake_build_fill_depth_runtime(*, accepted, data_cfg, heartbeat=None):
        called["value"] = True
        return pd.DataFrame(), DepthReplaySummary(
            market_rows_loaded=0,
            replay_rows=len(accepted),
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
            replay_rows_without_snapshots=len(accepted),
        ), object()

    monkeypatch.setattr(backtest_engine_module, "_build_fill_depth_runtime", _fake_build_fill_depth_runtime)

    fill_depth_replay, fill_lookup = backtest_engine_module._resolve_fill_depth_runtime(
        accepted=accepted,
        decision_depth_replay=decision_depth_replay,
        decision_depth_candidate_lookup=decision_lookup,
        data_cfg=None,
    )

    assert called["value"] is False
    assert fill_depth_replay is decision_depth_replay
    assert fill_lookup is decision_lookup


def test_narrow_depth_runtime_to_accepted_decisions_drops_unaccepted_snapshots() -> None:
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
            }
        ]
    )
    decision_depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "depth_snapshot_rank": 1,
            },
            {
                "decision_ts": "2026-03-28T00:02:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
                "depth_snapshot_rank": 1,
            },
        ]
    )

    narrowed_depth, narrowed_lookup = _narrow_depth_runtime_to_accepted_decisions(
        accepted=accepted,
        decision_depth_replay=decision_depth_replay,
    )

    assert len(narrowed_depth) == 1
    assert narrowed_depth["offset"].tolist() == [7]
    assert len(narrowed_lookup) == 1
    assert narrowed_lookup.get(
        ("2026-03-28T00:01:00+00:00", "2026-03-28T00:00:00+00:00", "2026-03-28T00:15:00+00:00", 7)
    ) == [
        {
            "decision_ts": "2026-03-28T00:01:00Z",
            "cycle_start_ts": "2026-03-28T00:00:00Z",
            "cycle_end_ts": "2026-03-28T00:15:00Z",
            "offset": 7,
            "depth_snapshot_rank": 1,
        }
    ]


def test_release_decision_depth_runtime_after_fill_resolution_drops_full_depth_when_narrowed() -> None:
    full_depth_replay = pd.DataFrame(
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
    narrowed_depth_replay = full_depth_replay.iloc[[0]].reset_index(drop=True)
    decision_lookup = fills_module.build_depth_candidate_lookup(full_depth_replay)

    released_depth, released_lookup = _release_decision_depth_runtime_after_fill_resolution(
        decision_depth_replay=full_depth_replay,
        decision_depth_candidate_lookup=decision_lookup,
        fill_depth_replay=narrowed_depth_replay,
    )

    assert released_depth.empty
    assert released_lookup is None


def test_release_decision_depth_runtime_after_fill_resolution_keeps_shared_depth_when_reused() -> None:
    full_depth_replay = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
            },
        ]
    )
    decision_lookup = fills_module.build_depth_candidate_lookup(full_depth_replay)

    kept_depth, kept_lookup = _release_decision_depth_runtime_after_fill_resolution(
        decision_depth_replay=full_depth_replay,
        decision_depth_candidate_lookup=decision_lookup,
        fill_depth_replay=full_depth_replay,
    )

    assert kept_depth is full_depth_replay
    assert kept_lookup is decision_lookup


def test_narrow_factor_source_to_accepted_decisions_drops_untraded_rows() -> None:
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
            }
        ]
    )
    factor_source = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "m-1",
                "condition_id": "c-1",
                "feature_a": 1.0,
            },
            {
                "decision_ts": "2026-03-28T00:02:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 8,
                "market_id": "m-2",
                "condition_id": "c-2",
                "feature_a": 2.0,
            },
        ]
    )

    narrowed = _narrow_factor_source_to_accepted_decisions(
        factor_source_frame=factor_source,
        accepted=accepted,
    )

    assert len(narrowed) == 1
    assert narrowed["feature_a"].tolist() == [1.0]


def test_build_canonical_fills_preserves_materialized_columns_when_all_rows_reject(tmp_path: Path) -> None:
    from pm15min.data.config import DataConfig

    root = tmp_path / "v2"
    data_cfg = DataConfig.build(market="sol", cycle="15m", surface="backtest", root=root)
    accepted = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-28T00:01:00Z",
                "cycle_start_ts": "2026-03-28T00:00:00Z",
                "cycle_end_ts": "2026-03-28T00:15:00Z",
                "offset": 7,
                "market_id": "market-1",
                "condition_id": "cond-1",
                "token_up": "token-up",
                "token_down": "token-down",
                "quote_up_ask": 0.20,
                "quote_up_ask_size_1": 10.0,
                "p_up": 0.80,
                "p_down": 0.20,
            },
        ]
    )

    fills, rejects = fills_module.build_canonical_fills(
        accepted,
        data_cfg=data_cfg,
        config=fills_module.BacktestFillConfig(
            base_stake=1.0,
            max_stake=1.0,
            fee_bps=0.0,
            high_conf_threshold=0.99,
            high_conf_multiplier=1.0,
        ),
        depth_replay=pd.DataFrame(),
    )

    assert fills.empty
    assert "depth_status" in fills.columns
    assert "depth_reason" in fills.columns
    assert rejects["reason"].tolist() == ["depth_snapshot_missing"]


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
