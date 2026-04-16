from __future__ import annotations

import pandas as pd
from types import SimpleNamespace

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import DateWindow, TrainingSetSpec
from pm15min.research.datasets.loaders import load_label_frame
from pm15min.research.datasets.training_sets import build_training_set_dataset
from pm15min.research.labels.alignment import merge_feature_and_label_frames
from pm15min.research.labels.frames import build_label_frame, label_frame_metadata
from pm15min.research.manifests import read_manifest


def test_label_source_and_alignment_metadata(tmp_path) -> None:
    truth = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "chainlink_streams",
                "full_truth": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_324_100,
                "cycle_end_ts": 1_772_325_000,
                "market_id": "m-2",
                "condition_id": "c-2",
                "winner_side": "DOWN",
                "label_updown": "DOWN",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            },
        ]
    )
    oracle = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "has_both": True,
                "source_price_to_beat": "oracle_prices",
                "source_final_price": "chainlink_datafeeds",
            }
        ]
    )
    frame = build_label_frame(label_set="truth", truth_table=truth, oracle_prices_table=oracle)
    metadata = label_frame_metadata(frame)
    assert frame["label_source"].tolist() == ["streams", "settlement_truth"]
    assert metadata["label_source_counts"] == {"settlement_truth": 1, "streams": 1}

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "ret_1m": 0.2,
            },
            {
                "decision_ts": "2026-03-01T00:31:00Z",
                "cycle_start_ts": "2026-03-01T00:30:00Z",
                "cycle_end_ts": "2026-03-01T00:45:00Z",
                "offset": 7,
                "ret_1m": 0.3,
            },
        ]
    )
    merged, summary = merge_feature_and_label_frames(features, frame)
    assert summary["aligned_rows"] == 2
    assert summary["missing_label_rows"] == 1
    assert merged["label_alignment_status"].tolist() == ["aligned", "aligned", "missing_label"]


def test_build_label_frame_supports_truth_chainlink_source_filters() -> None:
    truth = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "streams",
                "full_truth": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_324_100,
                "cycle_end_ts": 1_772_325_000,
                "market_id": "m-2",
                "condition_id": "c-2",
                "winner_side": "DOWN",
                "label_updown": "DOWN",
                "resolved": True,
                "truth_source": "chainlink_mixed",
                "full_truth": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_325_000,
                "cycle_end_ts": 1_772_325_900,
                "market_id": "m-3",
                "condition_id": "c-3",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "oracle_prices",
                "full_truth": True,
            },
        ]
    )

    streams_frame = build_label_frame(label_set="streams", truth_table=truth, oracle_prices_table=pd.DataFrame())
    mixed_frame = build_label_frame(label_set="chainlink_mixed", truth_table=truth, oracle_prices_table=pd.DataFrame())

    assert streams_frame["market_id"].tolist() == ["m-1"]
    assert streams_frame["label_source"].tolist() == ["streams"]
    assert mixed_frame["market_id"].tolist() == ["m-2"]
    assert mixed_frame["label_source"].tolist() == ["chainlink_mixed"]


def test_build_label_frame_aliases_settlement_truth_to_truth() -> None:
    truth = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            }
        ]
    )

    frame = build_label_frame(label_set="settlement_truth", truth_table=truth, oracle_prices_table=pd.DataFrame())

    assert frame["label_set"].tolist() == ["truth"]
    assert frame["label_source"].tolist() == ["settlement_truth"]


def test_build_label_frame_truth_backfills_missing_cycles_from_oracle_prices() -> None:
    truth = pd.DataFrame(
        [
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            }
        ]
    )
    oracle = pd.DataFrame(
        [
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "price_to_beat": 100.0,
                "final_price": 101.0,
                "has_both": True,
                "source_price_to_beat": "oracle_prices",
                "source_final_price": "chainlink_datafeeds",
            },
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_324_100,
                "cycle_end_ts": 1_772_325_000,
                "price_to_beat": 101.0,
                "final_price": 99.0,
                "has_both": True,
                "source_price_to_beat": "oracle_prices",
                "source_final_price": "chainlink_datafeeds",
            },
        ]
    )

    frame = build_label_frame(label_set="truth", truth_table=truth, oracle_prices_table=oracle)

    assert len(frame) == 2
    assert frame["cycle_start_ts"].tolist() == [1_772_323_200, 1_772_324_100]
    assert frame.iloc[0]["settlement_source"] == "settlement_truth"
    assert frame.iloc[1]["settlement_source"] == "chainlink_datafeeds"
    assert frame.iloc[1]["label_source"] == "datafeeds"
    assert bool(frame.iloc[1]["resolved"]) is True
    assert frame.iloc[1]["winner_side"] == "DOWN"


def test_build_label_frame_truth_preserves_multiple_contracts_per_cycle() -> None:
    truth = pd.DataFrame(
        [
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "winner_side": "UP",
                "label_updown": "UP",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            },
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-2",
                "condition_id": "c-2",
                "winner_side": "DOWN",
                "label_updown": "DOWN",
                "resolved": True,
                "truth_source": "settlement_truth",
                "full_truth": True,
            },
        ]
    )

    frame = build_label_frame(label_set="truth", truth_table=truth, oracle_prices_table=pd.DataFrame())

    assert len(frame) == 2
    assert set(frame["market_id"].tolist()) == {"m-1", "m-2"}


def test_merge_feature_and_label_frames_recomputes_strike_features_per_contract() -> None:
    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:07:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "close": 110.0,
                "ret_from_cycle_open": 0.1,
                "rv_30": 0.05,
                "basis_bp": 0.0,
                "has_oracle_strike": 0,
                "has_cl_strike": 0,
                "ret_from_strike": 999.0,
                "move_z_strike": 999.0,
                "strike_abs_z": 999.0,
                "strike_flip_count_cycle": 999.0,
                "q_bs_up_strike": 0.5,
                "q_bs_up_strike_centered": 0.0,
            }
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
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
                "asset": "btc",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-2",
                "condition_id": "c-2",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 99.0,
                "winner_side": "DOWN",
                "direction_up": 0.0,
                "full_truth": True,
            },
        ]
    )

    merged, summary = merge_feature_and_label_frames(features, labels)

    assert len(merged) == 2
    assert summary["aligned_rows"] == 2
    assert set(merged["market_id"].tolist()) == {"m-1", "m-2"}
    assert set(round(float(value), 6) for value in merged["ret_from_strike"].tolist()) == {0.1, -0.083333}


def test_load_label_frame_uses_truth_for_settlement_truth_alias(tmp_path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )
    label_frame = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "winner_side": "UP",
                "direction_up": 1.0,
                "full_truth": True,
            }
        ]
    )
    write_parquet_atomic(label_frame, cfg.layout.label_frame_path("truth"))

    loaded = load_label_frame(cfg, label_set="settlement_truth")

    assert len(loaded) == 1
    assert loaded.iloc[0]["label_set"] == "truth"


def test_training_set_manifest_captures_label_source_and_alignment(tmp_path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )

    feature_frame = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
                "ret_from_strike": 0.05,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "ret_1m": -0.1,
                "ret_from_strike": -0.05,
            },
        ]
    )
    label_frame = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "settlement_source": "chainlink_streams",
                "label_source": "streams",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "winner_side": "UP",
                "direction_up": 1.0,
                "full_truth": True,
            }
        ]
    )
    write_parquet_atomic(feature_frame, cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))
    write_parquet_atomic(label_frame, cfg.layout.label_frame_path(cfg.label_set))

    summary = build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            offset=7,
        ),
    )
    out = pd.read_parquet(summary["target_path"])
    manifest = read_manifest(summary["manifest_path"])

    assert summary["rows_written"] == 1
    assert "label_source" in out.columns
    assert out.iloc[0]["label_source"] == "streams"
    assert manifest.metadata["label_source_counts"] == {"streams": 1}
    assert manifest.metadata["aligned_rows_in_window"] == 1


def test_training_set_window_supports_precise_utc_start(tmp_path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )

    feature_frame = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
                "ret_from_strike": 0.05,
            },
            {
                "decision_ts": "2026-03-01T00:16:00Z",
                "cycle_start_ts": "2026-03-01T00:15:00Z",
                "cycle_end_ts": "2026-03-01T00:30:00Z",
                "offset": 7,
                "ret_1m": -0.1,
                "ret_from_strike": -0.05,
            },
        ]
    )
    label_frame = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "winner_side": "UP",
                "direction_up": 1.0,
                "full_truth": True,
            },
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_324_100,
                "cycle_end_ts": 1_772_325_000,
                "market_id": "m-2",
                "condition_id": "c-2",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 119.0,
                "winner_side": "DOWN",
                "direction_up": 0.0,
                "full_truth": True,
            },
        ]
    )
    write_parquet_atomic(feature_frame, cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))
    write_parquet_atomic(label_frame, cfg.layout.label_frame_path(cfg.label_set))

    summary = build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="settlement_truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01T00:16:00Z", "2026-03-01"),
            offset=7,
        ),
    )
    out = pd.read_parquet(summary["target_path"])

    assert summary["rows_written"] == 1
    assert out.iloc[0]["decision_ts"] == pd.Timestamp("2026-03-01T00:16:00Z")


def test_build_training_set_dataset_passes_window_and_offset_filters_to_loaders(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )
    seen: dict[str, object] = {}

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None, filters=None):
        seen["feature_set"] = feature_set
        seen["feature_columns"] = columns
        seen["feature_filters"] = filters
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "ret_1m": 0.1,
                    "ret_from_strike": 0.05,
                },
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 8,
                    "ret_1m": 0.2,
                    "ret_from_strike": 0.06,
                },
            ]
        )

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None, filters=None):
        seen["label_set"] = label_set
        seen["label_columns"] = tuple(columns or ())
        seen["label_filters"] = filters
        return pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": int(pd.Timestamp("2026-03-01T00:00:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-01T00:15:00Z").timestamp()),
                    "market_id": "m-1",
                    "condition_id": "c-1",
                    "label_set": "truth",
                    "settlement_source": "settlement_truth",
                    "label_source": "settlement_truth",
                    "resolved": True,
                    "price_to_beat": 120.0,
                    "final_price": 121.0,
                    "winner_side": "UP",
                    "direction_up": 1.0,
                    "full_truth": True,
                }
            ]
        )

    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.load_feature_frame",
        _fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.load_label_frame",
        _fake_load_label_frame,
    )

    summary = build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            offset=7,
        ),
        skip_freshness=True,
    )

    assert summary["rows_written"] == 1
    assert seen["feature_set"] == "deep_otm_v1"
    assert seen["feature_filters"] == [
        ("decision_ts", ">=", pd.Timestamp("2026-03-01T00:00:00Z")),
        ("decision_ts", "<", pd.Timestamp("2026-03-02T00:00:00Z")),
        ("offset", "==", 7),
    ]
    assert seen["label_set"] == "truth"
    assert seen["label_filters"] == [
        ("cycle_end_ts", ">", int(pd.Timestamp("2026-03-01T00:00:00Z").timestamp())),
        ("cycle_start_ts", "<", int(pd.Timestamp("2026-03-02T00:00:00Z").timestamp())),
    ]


def test_build_training_set_dataset_precise_start_filters_label_cycles(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )
    seen: dict[str, object] = {}

    def _fake_load_feature_frame(_cfg, *, feature_set=None, columns=None, filters=None):
        seen["feature_filters"] = filters
        return pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:16:00Z",
                    "cycle_start_ts": "2026-03-01T00:15:00Z",
                    "cycle_end_ts": "2026-03-01T00:30:00Z",
                    "offset": 7,
                    "ret_1m": -0.1,
                    "ret_from_strike": -0.05,
                }
            ]
        )

    def _fake_load_label_frame(_cfg, *, label_set=None, columns=None, filters=None):
        seen["label_filters"] = filters
        return pd.DataFrame(
            [
                {
                    "asset": "sol",
                    "cycle_start_ts": int(pd.Timestamp("2026-03-01T00:15:00Z").timestamp()),
                    "cycle_end_ts": int(pd.Timestamp("2026-03-01T00:30:00Z").timestamp()),
                    "market_id": "m-2",
                    "condition_id": "c-2",
                    "label_set": "truth",
                    "settlement_source": "settlement_truth",
                    "label_source": "settlement_truth",
                    "resolved": True,
                    "price_to_beat": 120.0,
                    "final_price": 119.0,
                    "winner_side": "DOWN",
                    "direction_up": 0.0,
                    "full_truth": True,
                }
            ]
        )

    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.load_feature_frame",
        _fake_load_feature_frame,
    )
    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.load_label_frame",
        _fake_load_label_frame,
    )

    summary = build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01T00:16:00Z", "2026-03-01"),
            offset=7,
        ),
        skip_freshness=True,
    )

    assert summary["rows_written"] == 1
    assert seen["feature_filters"] == [
        ("decision_ts", ">=", pd.Timestamp("2026-03-01T00:16:00Z")),
        ("decision_ts", "<", pd.Timestamp("2026-03-02T00:00:00Z")),
        ("offset", "==", 7),
    ]
    assert seen["label_filters"] == [
        ("cycle_end_ts", ">", int(pd.Timestamp("2026-03-01T00:16:00Z").timestamp())),
        ("cycle_start_ts", "<", int(pd.Timestamp("2026-03-02T00:00:00Z").timestamp())),
    ]


def test_build_training_set_dataset_attaches_tradeable_winner_metadata(
    tmp_path,
    monkeypatch,
) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm_baseline",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        root=root,
    )

    feature_frame = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:01:00Z",
                "cycle_start_ts": "2026-03-01T00:00:00Z",
                "cycle_end_ts": "2026-03-01T00:15:00Z",
                "offset": 7,
                "ret_1m": 0.1,
                "ret_from_strike": 0.05,
            }
        ]
    )
    label_frame = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "market_id": "m-1",
                "condition_id": "c-1",
                "label_set": "truth",
                "settlement_source": "settlement_truth",
                "label_source": "settlement_truth",
                "resolved": True,
                "price_to_beat": 120.0,
                "final_price": 121.0,
                "winner_side": "UP",
                "direction_up": 1.0,
                "full_truth": True,
            }
        ]
    )
    write_parquet_atomic(feature_frame, cfg.layout.feature_frame_path(cfg.feature_set, source_surface=cfg.source_surface))
    write_parquet_atomic(label_frame, cfg.layout.label_frame_path(cfg.label_set))

    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.attach_canonical_quote_surface",
        lambda *, replay, data_cfg: (
            replay.assign(
                quote_status="ok",
                quote_reason="",
                quote_up_ask=0.12,
                quote_down_ask=0.88,
            ),
            SimpleNamespace(
                market_rows_loaded=1,
                replay_rows=len(replay),
                quote_ready_rows=len(replay),
                quote_missing_rows=0,
                to_dict=lambda: {
                    "market_rows_loaded": 1,
                    "replay_rows": len(replay),
                    "quote_ready_rows": len(replay),
                    "quote_missing_rows": 0,
                },
            ),
        ),
    )
    monkeypatch.setattr(
        "pm15min.research.datasets.training_sets.resolve_backtest_profile_spec",
        lambda **kwargs: SimpleNamespace(entry_price_min=0.01, entry_price_max=0.30),
    )

    summary = build_training_set_dataset(
        cfg,
        TrainingSetSpec(
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window=DateWindow.from_bounds("2026-03-01", "2026-03-01"),
            offset=7,
        ),
    )
    out = pd.read_parquet(summary["target_path"])
    manifest = read_manifest(summary["manifest_path"])

    assert bool(out.iloc[0]["winner_in_band"]) is True
    assert float(out.iloc[0]["winner_entry_price"]) == 0.12
    assert out.iloc[0]["quote_status"] == "ok"
    assert manifest.metadata["quote_ready_rows_in_window"] == 1
    assert manifest.metadata["winner_in_band_rows"] == 1
