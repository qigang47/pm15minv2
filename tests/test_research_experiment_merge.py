from __future__ import annotations

import pandas as pd

from pm15min.research.experiments.merge import merge_compare_frames


def test_merge_compare_frames_sums_metrics_and_recomputes_roi() -> None:
    primary = pd.DataFrame(
        [
            {
                "case_key": "btc:core:run-a:default",
                "market": "btc",
                "group_name": "core",
                "run_name": "run-a",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": "/tmp/train-a",
                "bundle_dir": "/tmp/bundle-a",
                "backtest_run_dir": "/tmp/backtest-a",
                "summary_path": "/tmp/backtest-a/summary.json",
                "training_reused": True,
                "bundle_reused": True,
                "resumed_from_existing": False,
                "trades": 3,
                "rejects": 2,
                "wins": 2,
                "losses": 1,
                "pnl_sum": 1.5,
                "stake_sum": 3.0,
                "roi_pct": 50.0,
            }
        ]
    )
    supplemental = pd.DataFrame(
        [
            {
                "case_key": "btc:core:run-a:default",
                "market": "btc",
                "group_name": "core",
                "run_name": "run-a",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": "/tmp/train-a",
                "bundle_dir": "/tmp/bundle-a",
                "backtest_run_dir": "/tmp/backtest-b",
                "summary_path": "/tmp/backtest-b/summary.json",
                "training_reused": True,
                "bundle_reused": True,
                "resumed_from_existing": False,
                "trades": 2,
                "rejects": 1,
                "wins": 1,
                "losses": 1,
                "pnl_sum": 0.5,
                "stake_sum": 2.0,
                "roi_pct": 25.0,
            }
        ]
    )

    merged = merge_compare_frames(primary, supplemental)

    assert len(merged) == 1
    row = merged.iloc[0]
    assert row["case_key"] == "btc:core:run-a:default"
    assert row["status"] == "completed"
    assert row["trades"] == 5
    assert row["rejects"] == 3
    assert row["wins"] == 3
    assert row["losses"] == 2
    assert row["pnl_sum"] == 2.0
    assert row["stake_sum"] == 5.0
    assert row["roi_pct"] == 40.0
    assert row["source_summary_paths"] == [
        "/tmp/backtest-a/summary.json",
        "/tmp/backtest-b/summary.json",
    ]


def test_merge_compare_frames_keeps_unmatched_cases() -> None:
    primary = pd.DataFrame(
        [
            {
                "case_key": "eth:core:run-a:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "run-a",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "trades": 1,
                "rejects": 0,
                "wins": 1,
                "losses": 0,
                "pnl_sum": 0.2,
                "stake_sum": 1.0,
                "roi_pct": 20.0,
            }
        ]
    )
    supplemental = pd.DataFrame(
        [
            {
                "case_key": "eth:core:run-b:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "run-b",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "trades": 2,
                "rejects": 1,
                "wins": 1,
                "losses": 1,
                "pnl_sum": 0.1,
                "stake_sum": 2.0,
                "roi_pct": 5.0,
            }
        ]
    )

    merged = merge_compare_frames(primary, supplemental)

    assert sorted(merged["case_key"].tolist()) == [
        "eth:core:run-a:default",
        "eth:core:run-b:default",
    ]
    row_a = merged.loc[merged["case_key"].eq("eth:core:run-a:default")].iloc[0]
    row_b = merged.loc[merged["case_key"].eq("eth:core:run-b:default")].iloc[0]
    assert row_a["trades"] == 1
    assert row_b["trades"] == 2
