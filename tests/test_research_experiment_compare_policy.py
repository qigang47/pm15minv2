from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.compare_policy import build_variant_compare_frame
from pm15min.research.experiments.runner import run_experiment_suite


def test_build_variant_compare_frame_assigns_reference_ranks_and_deltas() -> None:
    compare_frame = pd.DataFrame(
        [
            {
                "case_key": "sol:core:base:default",
                "market": "sol",
                "group_name": "core",
                "run_name": "base",
                "target": "direction",
                "variant_label": "default",
                "variant_notes": "baseline",
                "status": "completed",
                "trades": 10,
                "pnl_sum": 1.0,
                "roi_pct": 100.0,
            },
            {
                "case_key": "sol:core:base:tight",
                "market": "sol",
                "group_name": "core",
                "run_name": "base",
                "target": "direction",
                "variant_label": "tight",
                "variant_notes": "challenger",
                "status": "completed",
                "trades": 9,
                "pnl_sum": 1.4,
                "roi_pct": 140.0,
            },
            {
                "case_key": "sol:core:base:loose",
                "market": "sol",
                "group_name": "core",
                "run_name": "base",
                "target": "direction",
                "variant_label": "loose",
                "variant_notes": "failed",
                "status": "failed",
                "trades": 0,
                "pnl_sum": pd.NA,
                "roi_pct": pd.NA,
            },
        ]
    )

    variant_compare = build_variant_compare_frame(compare_frame)
    rows = variant_compare.set_index("variant_label")

    assert rows.loc["default", "reference_variant_label"] == "default"
    assert rows.loc["default", "reference_policy"] == "preferred_label"
    assert rows.loc["default", "comparison_vs_reference"] == "reference"
    assert rows.loc["default", "rank_in_run_by_roi"] == 2

    assert rows.loc["tight", "best_completed_variant_label"] == "tight"
    assert rows.loc["tight", "rank_in_run_by_roi"] == 1
    assert rows.loc["tight", "rank_in_run_by_pnl"] == 1
    assert rows.loc["tight", "roi_pct_delta_vs_reference"] == 40.0
    assert rows.loc["tight", "pnl_sum_delta_vs_reference"] == pytest.approx(0.4)
    assert rows.loc["tight", "comparison_vs_reference"] == "better_than_reference"

    assert rows.loc["loose", "reference_variant_label"] == "default"
    assert pd.isna(rows.loc["loose", "rank_in_run_by_roi"])
    assert rows.loc["loose", "comparison_vs_reference"] == "candidate_not_completed"


def test_run_experiment_suite_persists_variant_compare_with_custom_reference_policy(
    monkeypatch,
    tmp_path: Path,
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
        model_family="deep_otm",
        root=root,
    )
    suite_path = cfg.layout.storage.suite_spec_path("compare_policy_suite")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "compare_policy_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "compare_policy": {
                    "reference_variant_labels": ["loose", "default"],
                },
                "backtest_variants": [
                    {"label": "tight", "notes": "higher roi"},
                    {"label": "loose", "notes": "reference"},
                ],
                "markets": {
                    "sol": {
                        "groups": {
                            "core": {
                                "runs": [
                                    {"run_name": "base"},
                                ]
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

    monkeypatch.setattr("pm15min.research.experiments.runner.build_feature_frame_dataset", lambda cfg: {"ok": True})
    monkeypatch.setattr("pm15min.research.experiments.runner.build_label_frame_dataset", lambda cfg: {"ok": True})

    def _fake_train(cfg, spec):
        run_dir = root / "research" / "training_runs" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        return {"run_dir": str(run_dir), "run_label": spec.run_label}

    def _fake_bundle(cfg, spec):
        bundle_dir = root / "research" / "model_bundles" / spec.bundle_label
        bundle_dir.mkdir(parents=True, exist_ok=True)
        return {"bundle_dir": str(bundle_dir), "bundle_label": spec.bundle_label}

    def _fake_backtest(cfg, spec):
        run_dir = root / "research" / "backtests" / spec.run_label
        run_dir.mkdir(parents=True, exist_ok=True)
        summary_path = run_dir / "summary.json"
        roi_pct = 120.0 if spec.variant_label == "tight" else 80.0
        pnl_sum = 1.2 if spec.variant_label == "tight" else 0.8
        summary_path.write_text(
            json.dumps(
                {
                    "trades": 2,
                    "rejects": 0,
                    "wins": 1,
                    "losses": 1,
                    "pnl_sum": pnl_sum,
                    "stake_sum": 1.0,
                    "roi_pct": roi_pct,
                }
            ),
            encoding="utf-8",
        )
        return {"run_dir": str(run_dir), "summary_path": str(summary_path)}

    monkeypatch.setattr("pm15min.research.experiments.runner.train_research_run", _fake_train)
    monkeypatch.setattr("pm15min.research.experiments.runner.build_model_bundle", _fake_bundle)
    monkeypatch.setattr("pm15min.research.experiments.runner.run_research_backtest", _fake_backtest)

    summary = run_experiment_suite(cfg=cfg, suite_name="compare_policy_suite", run_label="compare-policy-exp")
    run_dir = Path(summary["run_dir"])
    variant_compare = pd.read_parquet(run_dir / "variant_compare.parquet").sort_values("variant_label").reset_index(drop=True)
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))

    assert (run_dir / "variant_compare.csv").exists()
    assert summary["variant_compare_csv_path"] == str(run_dir / "variant_compare.csv")
    assert variant_compare["reference_variant_label"].tolist() == ["loose", "loose"]
    assert variant_compare["reference_policy"].tolist() == ["preferred_label", "preferred_label"]
    assert variant_compare["comparison_vs_reference"].tolist() == ["reference", "better_than_reference"]
    assert variant_compare.loc[1, "roi_pct_delta_vs_reference"] == 40.0
    assert variant_compare.loc[1, "best_completed_variant_label"] == "tight"
    assert manifest["spec"]["compare_policy"] == {"reference_variant_labels": ["loose", "default"]}
