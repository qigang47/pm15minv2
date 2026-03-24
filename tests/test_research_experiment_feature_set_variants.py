from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research.experiments.leaderboard import build_leaderboard
from pm15min.research.experiments.reports import (
    build_experiment_compare_frame,
    build_experiment_summary,
)
from pm15min.research.experiments.specs import load_suite_definition


def test_load_suite_definition_expands_feature_set_variants(tmp_path: Path) -> None:
    path = tmp_path / "suite.json"
    path.write_text(
        json.dumps(
            {
                "suite_name": "feature_set_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-02"},
                "feature_set_variants": [
                    {"label": "core", "feature_set": "deep_otm_v1", "notes": "core features"},
                    {"label": "wide", "feature_set": "v6_user_core", "notes": "wider features"},
                ],
                "markets": {
                    "sol": {
                        "groups": {
                            "main": {
                                "runs": [{"run_name": "baseline"}]
                            }
                        }
                    }
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    suite = load_suite_definition(path)

    specs = sorted(suite.markets, key=lambda item: item.run_name)
    assert [spec.run_name for spec in specs] == [
        "baseline__fs_core",
        "baseline__fs_wide",
    ]
    assert [spec.feature_set for spec in specs] == [
        "deep_otm_v1",
        "v6_user_core",
    ]
    assert specs[0].notes == "core features"
    assert specs[1].notes == "wider features"
    assert "feature_set:deep_otm_v1" in specs[0].tags
    assert "feature_set:v6_user_core" in specs[1].tags


def test_experiment_compare_and_leaderboard_carry_feature_set() -> None:
    training_runs = pd.DataFrame(
        [
            {
                "case_key": "case_a",
                "market": "sol",
                "group_name": "main",
                "run_name": "baseline__fs_core",
                "feature_set": "deep_otm_v1",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "training_run_dir": "/tmp/train/core",
                "bundle_dir": "/tmp/bundle/core",
                "status": "completed",
            },
            {
                "case_key": "case_b",
                "market": "sol",
                "group_name": "main",
                "run_name": "baseline__fs_wide",
                "feature_set": "v6_user_core",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "training_run_dir": "/tmp/train/wide",
                "bundle_dir": "/tmp/bundle/wide",
                "status": "completed",
            },
        ]
    )
    backtest_runs = pd.DataFrame(
        [
            {
                "case_key": "case_a",
                "market": "sol",
                "group_name": "main",
                "run_name": "baseline__fs_core",
                "feature_set": "deep_otm_v1",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "bundle_dir": "/tmp/bundle/core",
                "backtest_run_dir": "/tmp/backtest/core",
                "summary_path": "/tmp/backtest/core/summary.json",
                "status": "completed",
                "trades": 10,
                "pnl_sum": 2.0,
                "roi_pct": 20.0,
            },
            {
                "case_key": "case_b",
                "market": "sol",
                "group_name": "main",
                "run_name": "baseline__fs_wide",
                "feature_set": "v6_user_core",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "bundle_dir": "/tmp/bundle/wide",
                "backtest_run_dir": "/tmp/backtest/wide",
                "summary_path": "/tmp/backtest/wide/summary.json",
                "status": "completed",
                "trades": 12,
                "pnl_sum": 3.0,
                "roi_pct": 25.0,
            },
        ]
    )

    compare_frame = build_experiment_compare_frame(
        training_runs=training_runs,
        backtest_runs=backtest_runs,
    )
    assert compare_frame["feature_set"].tolist() == ["deep_otm_v1", "v6_user_core"]

    leaderboard = build_leaderboard(backtest_runs)
    assert "feature_set" in leaderboard.columns
    assert leaderboard.iloc[0]["feature_set"] == "v6_user_core"

    summary = build_experiment_summary(
        suite_name="feature_set_suite",
        run_label="exp_feature_sets",
        training_runs=training_runs,
        backtest_runs=backtest_runs,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
    )
    assert summary["feature_sets"] == ["deep_otm_v1", "v6_user_core"]
