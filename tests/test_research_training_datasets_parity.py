from __future__ import annotations

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import DateWindow, TrainingSetSpec
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
