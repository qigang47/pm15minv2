from __future__ import annotations

import pandas as pd

from pm15min.research.backtests.replay_loader import build_replay_frame, build_score_frame


def test_build_score_frame_normalizes_basic_columns() -> None:
    score = build_score_frame(
        [
            pd.DataFrame(
                [
                    {
                        "decision_ts": "2026-03-01T00:01:00Z",
                        "cycle_start_ts": "2026-03-01T00:00:00Z",
                        "cycle_end_ts": "2026-03-01T00:15:00Z",
                        "offset": 7,
                        "p_up": 0.7,
                        "p_down": 0.3,
                    }
                ]
            )
        ]
    )
    assert bool(score.iloc[0]["score_valid"]) is False
    assert score.iloc[0]["score_reason"] == ""


def test_build_replay_frame_summarizes_score_coverage() -> None:
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
                "offset": 8,
                "ret_1m": -0.1,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
            }
        ]
    )
    score_frames = [
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "p_up": 0.74,
                    "p_down": 0.26,
                    "score_valid": True,
                }
            ]
        )
    ]
    replay, summary = build_replay_frame(
        features=features,
        labels=labels,
        score_frames=score_frames,
        available_offsets=[7],
    )
    assert len(replay) == 2
    assert summary.score_covered_rows == 1
    assert summary.bundle_offset_missing_rows == 1
    assert summary.ready_rows == 1


def test_build_replay_frame_can_scope_to_available_offsets() -> None:
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
                "offset": 8,
                "ret_1m": -0.1,
            },
        ]
    )
    labels = pd.DataFrame(
        [
            {
                "asset": "sol",
                "cycle_start_ts": 1_772_323_200,
                "cycle_end_ts": 1_772_324_100,
                "label_set": "truth",
                "resolved": True,
                "winner_side": "UP",
            }
        ]
    )
    score_frames = [
        pd.DataFrame(
            [
                {
                    "decision_ts": "2026-03-01T00:01:00Z",
                    "cycle_start_ts": "2026-03-01T00:00:00Z",
                    "cycle_end_ts": "2026-03-01T00:15:00Z",
                    "offset": 7,
                    "p_up": 0.74,
                    "p_down": 0.26,
                    "score_valid": True,
                }
            ]
        )
    ]
    replay, summary = build_replay_frame(
        features=features,
        labels=labels,
        score_frames=score_frames,
        available_offsets=[7],
        scoped_offsets=[7],
    )
    assert replay["offset"].tolist() == [7]
    assert len(replay) == 1
    assert summary.feature_rows == 1
    assert summary.score_covered_rows == 1
    assert summary.bundle_offset_missing_rows == 0
    assert summary.ready_rows == 1
