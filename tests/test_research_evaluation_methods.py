from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_eval.brier_score import (
    brier_by_group as legacy_brier_by_group,
    brier_score as legacy_brier_score,
    calibration_bins as legacy_calibration_bins,
)
from poly_eval.time_slices import (
    add_time_bucket as legacy_add_time_bucket,
    time_slice_metrics as legacy_time_slice_metrics,
)
from pm15min.research.evaluation.methods.binary_metrics import (
    brier_by_group,
    brier_score,
    calibration_bins,
    summarize_binary_predictions,
)
from pm15min.research.evaluation.methods.time_slices import add_time_bucket, time_slice_metrics
from pm15min.research.evaluation.methods.trade_metrics import (
    summarize_trade_drift_slices,
    summarize_trade_groups,
)


def test_binary_metrics_match_legacy_poly_eval() -> None:
    df = pd.DataFrame(
        {
            "prob": [0.1, 0.2, 0.8, np.nan, 1.2],
            "outcome": [0, 0, 1, 1, 1],
            "offset": [7, 7, 8, 8, 8],
        }
    )

    assert brier_score(df["prob"], df["outcome"]) == pytest.approx(legacy_brier_score(df["prob"], df["outcome"]))

    expected = legacy_brier_by_group(df, prob_col="prob", outcome_col="outcome", group_cols=["offset"])
    actual = brier_by_group(df, prob_col="prob", outcome_col="outcome", group_cols=["offset"])
    pdt.assert_frame_equal(actual, expected)

    summary = summarize_binary_predictions(df["prob"], df["outcome"])
    assert summary["count"] == 4
    assert summary["brier"] == pytest.approx(float(np.mean((np.array([0.1, 0.2, 0.8, 1.2]) - np.array([0, 0, 1, 1])) ** 2)))
    assert summary["bias"] == pytest.approx(float(np.mean([0.1, 0.2, 0.8, 1.2]) - np.mean([0, 0, 1, 1])))


def test_calibration_bins_match_legacy_poly_eval() -> None:
    df = pd.DataFrame(
        {
            "prob": [-0.2, 0.1, 0.49, 0.51, 0.9, 1.4],
            "outcome": [0, 0, 0, 1, 1, 1],
        }
    )

    expected = legacy_calibration_bins(df, prob_col="prob", outcome_col="outcome", n_bins=5)
    actual = calibration_bins(df, prob_col="prob", outcome_col="outcome", n_bins=5)
    pdt.assert_frame_equal(actual, expected)


def test_time_slice_metrics_match_legacy_poly_eval() -> None:
    frame = pd.DataFrame(
        {
            "ts": pd.to_datetime(
                [
                    "2026-03-01T00:00:00Z",
                    "2026-03-01T01:00:00Z",
                    "2026-03-02T00:00:00Z",
                    "2026-03-02T02:00:00Z",
                ],
                utc=True,
            ),
            "prob": [0.1, 0.9, 0.2, 0.8],
            "outcome": [0, 1, 0, 1],
        }
    )

    expected_frame, expected_note = legacy_add_time_bucket(frame, ts_col="ts", slice="day", bucket_col="bucket")
    actual_frame, actual_note = add_time_bucket(frame, ts_col="ts", slice="day", bucket_col="bucket")
    pdt.assert_frame_equal(actual_frame, expected_frame)
    assert actual_note == expected_note

    expected = legacy_time_slice_metrics(
        expected_frame,
        bucket_col="bucket",
        prob_col="prob",
        outcome_col="outcome",
        scope_name="all",
    )
    actual = time_slice_metrics(
        actual_frame,
        bucket_col="bucket",
        prob_col="prob",
        outcome_col="outcome",
        scope_name="all",
    )
    pdt.assert_frame_equal(actual, expected)


def test_summarize_trade_groups_returns_runner_shape() -> None:
    trades = pd.DataFrame(
        {
            "offset": [7, 7, 8],
            "predicted_prob": [0.2, 0.6, 0.9],
            "win": [0, 1, 1],
            "pnl": [-1.0, 2.0, 3.5],
        }
    )

    out = summarize_trade_groups(trades, group_col="offset")

    assert list(out.columns) == ["offset", "trades", "win_rate", "avg_pred", "pnl_sum"]
    assert out.to_dict(orient="records") == [
        {"offset": 7, "trades": 2, "win_rate": 0.5, "avg_pred": 0.4, "pnl_sum": 1.0},
        {"offset": 8, "trades": 1, "win_rate": 1.0, "avg_pred": 0.9, "pnl_sum": 3.5},
    ]


def test_summarize_trade_drift_slices_returns_daily_runner_shape() -> None:
    trades = pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(
                [
                    "2026-03-01T00:05:00Z",
                    "2026-03-01T00:10:00Z",
                    "2026-03-02T00:05:00Z",
                ],
                utc=True,
            ),
            "predicted_prob": [0.2, 0.8, 0.7],
            "win": [0, 1, 1],
            "pnl": [-1.0, 2.0, 3.0],
        }
    )

    out = summarize_trade_drift_slices(trades)

    assert list(out.columns) == ["date", "trades", "win_rate", "avg_pred", "pnl_sum", "cumulative_pnl"]
    assert out.to_dict(orient="records") == [
        {
            "date": "2026-03-01",
            "trades": 2,
            "win_rate": 0.5,
            "avg_pred": 0.5,
            "pnl_sum": 1.0,
            "cumulative_pnl": 1.0,
        },
        {
            "date": "2026-03-02",
            "trades": 1,
            "win_rate": 1.0,
            "avg_pred": 0.7,
            "pnl_sum": 3.0,
            "cumulative_pnl": 4.0,
        },
    ]
