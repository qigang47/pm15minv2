from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.leaderboard import build_leaderboard
from pm15min.research.experiments.reports import (
    build_best_by_group_frame,
    build_best_by_matrix_frame,
    build_best_by_market_frame,
    build_best_by_run_frame,
    build_experiment_compare_frame,
    build_experiment_summary,
    build_factor_signal_summary,
    build_group_summary_frame,
    build_matrix_summary_frame,
    build_run_summary_frame,
    render_experiment_report,
)
from pm15min.research.experiments.runner import run_experiment_suite


def _write_training_offset_summary(
    run_dir: Path,
    *,
    offset: int,
    positive: list[tuple[str, float]],
    negative: list[tuple[str, float]],
) -> None:
    offset_dir = run_dir / "offsets" / f"offset={offset}"
    offset_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "offset": int(offset),
        "explainability": {
            "top_positive_factors": [
                {"feature": feature, "direction_score": score}
                for feature, score in positive
            ],
            "top_negative_factors": [
                {"feature": feature, "direction_score": score}
                for feature, score in negative
            ],
        },
    }
    (offset_dir / "summary.json").write_text(
        json.dumps(payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _sample_compare_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "case_key": "sol:core:base:tight",
                "market": "sol",
                "group_name": "core",
                "matrix_parent_run_name": "base",
                "matrix_stake_label": "stake_1usd",
                "run_name": "base",
                "target": "direction",
                "variant_label": "tight",
                "stake_usd": 1.0,
                "max_notional_usd": 2.0,
                "status": "completed",
                "training_reused": False,
                "bundle_reused": False,
                "secondary_training_reused": False,
                "secondary_bundle_reused": False,
                "resumed_from_existing": False,
                "trades": 12,
                "pnl_sum": 1.8,
                "roi_pct": 180.0,
            },
            {
                "case_key": "sol:core:base:loose",
                "market": "sol",
                "group_name": "core",
                "matrix_parent_run_name": "base",
                "matrix_stake_label": "stake_2usd",
                "run_name": "base",
                "target": "direction",
                "variant_label": "loose",
                "stake_usd": 2.0,
                "max_notional_usd": 2.0,
                "status": "completed",
                "training_reused": True,
                "bundle_reused": True,
                "secondary_training_reused": False,
                "secondary_bundle_reused": False,
                "resumed_from_existing": False,
                "trades": 8,
                "pnl_sum": 0.7,
                "roi_pct": 70.0,
            },
            {
                "case_key": "sol:core:hedge:alt",
                "market": "sol",
                "group_name": "core",
                "matrix_parent_run_name": "",
                "matrix_stake_label": "",
                "run_name": "hedge",
                "target": "direction",
                "variant_label": "alt",
                "stake_usd": pd.NA,
                "max_notional_usd": pd.NA,
                "status": "failed",
                "training_reused": False,
                "bundle_reused": False,
                "secondary_training_reused": False,
                "secondary_bundle_reused": False,
                "resumed_from_existing": False,
                "trades": 0,
                "pnl_sum": 0.0,
                "roi_pct": pd.NA,
            },
            {
                "case_key": "btc:swing:probe:default",
                "market": "btc",
                "group_name": "swing",
                "matrix_parent_run_name": "",
                "matrix_stake_label": "",
                "run_name": "probe",
                "target": "direction",
                "variant_label": "default",
                "stake_usd": pd.NA,
                "max_notional_usd": pd.NA,
                "status": "completed",
                "training_reused": False,
                "bundle_reused": False,
                "secondary_training_reused": False,
                "secondary_bundle_reused": False,
                "resumed_from_existing": True,
                "trades": 5,
                "pnl_sum": 0.5,
                "roi_pct": 50.0,
            },
        ]
    )


def test_run_experiment_suite_writes_compare_and_report_outputs(monkeypatch, tmp_path: Path) -> None:
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
    suite_path = cfg.layout.storage.suite_spec_path("report_suite")
    suite_path.parent.mkdir(parents=True, exist_ok=True)
    suite_path.write_text(
        json.dumps(
            {
                "suite_name": "report_suite",
                "cycle": "15m",
                "profile": "deep_otm",
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "offsets": [7, 8],
                "window": {"start": "2026-03-01", "end": "2026-03-01"},
                "tags": ["suite"],
                "backtest_variants": [
                    {"label": "tight", "notes": "tight-variant"},
                    {"label": "loose", "notes": "loose-variant"},
                ],
                "markets": {
                    "sol": {
                        "groups": {
                            "core": {
                                "runs": [
                                    {
                                        "run_name": "base",
                                        "tags": ["run"],
                                        "stakes_usd": [1.0, 2.0],
                                        "max_notional_usd": 2.0,
                                    },
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
        summary_path.write_text(
            json.dumps(
                {
                    "trades": 2,
                    "rejects": 1,
                    "wins": 1,
                    "losses": 1,
                    "pnl_sum": 1.2 if spec.variant_label == "tight" else 0.8,
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

    summary = run_experiment_suite(cfg=cfg, suite_name="report_suite", run_label="report-exp")
    run_dir = Path(summary["run_dir"])
    compare_df = pd.read_parquet(run_dir / "compare.parquet")
    matrix_summary_df = pd.read_parquet(run_dir / "matrix_summary.parquet")
    summary_payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    report_text = (run_dir / "report.md").read_text(encoding="utf-8")

    assert (run_dir / "compare.csv").exists()
    assert (run_dir / "matrix_summary.csv").exists()
    assert (run_dir / "report.md").exists()
    assert set(
        [
            "case_key",
            "group_name",
            "matrix_parent_run_name",
            "matrix_stake_label",
            "run_name",
            "variant_label",
            "stake_usd",
            "max_notional_usd",
            "training_reused",
            "bundle_reused",
            "status",
            "failure_stage",
            "error_type",
            "error_message",
            "roi_pct",
        ]
    ).issubset(compare_df.columns)
    assert len(compare_df) == 4
    assert len(matrix_summary_df) == 1
    assert summary_payload["cases"] == 4
    assert summary_payload["groups"] == 1
    assert summary_payload["matrices"] == 1
    assert summary_payload["runs"] == 2
    assert summary_payload["failed_cases"] == 0
    assert summary_payload["training_reused_cases"] == 3
    assert summary_payload["bundle_reused_cases"] == 3
    assert compare_df["matrix_parent_run_name"].dropna().unique().tolist() == ["base"]
    assert sorted(compare_df["matrix_stake_label"].dropna().unique().tolist()) == [
        "stake_1usd__max_2usd",
        "stake_2usd__max_2usd",
    ]
    assert matrix_summary_df.loc[0, "matrix_parent_run_name"] == "base"
    assert "Experiment Summary" in report_text
    assert "Top Cases" in report_text
    assert "Group Summary" in report_text
    assert "Matrix Summary" in report_text
    assert "Run Summary" in report_text
    assert "Focus Cuts" in report_text
    assert "Best Per Market" in report_text
    assert "Best Per Group" in report_text
    assert "Best Variant Per Matrix" in report_text
    assert "Best Variant Per Run" in report_text
    assert "Runtime Notes" in report_text
    assert "Failures" in report_text
    assert "tight" in report_text
    assert "loose" in report_text


def test_render_experiment_report_falls_back_without_tabulate(monkeypatch) -> None:
    leaderboard = build_leaderboard(_sample_compare_frame())
    compare_frame = _sample_compare_frame()
    summary_payload = build_experiment_summary(
        suite_name="demo_suite",
        run_label="demo_run",
        training_runs=pd.DataFrame(),
        backtest_runs=compare_frame,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=pd.DataFrame(),
    )

    def _raise_import_error(self, *args, **kwargs):
        raise ImportError("Missing optional dependency 'tabulate'")

    monkeypatch.setattr(pd.DataFrame, "to_markdown", _raise_import_error)

    report = render_experiment_report(
        summary_payload,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=pd.DataFrame(),
    )

    assert "# Experiment Summary" in report
    assert "| market |" in report


def test_report_builders_produce_group_run_and_focus_views() -> None:
    compare_frame = _sample_compare_frame()
    leaderboard = build_leaderboard(compare_frame.loc[compare_frame["status"].eq("completed")].copy())

    group_summary = build_group_summary_frame(compare_frame)
    matrix_summary = build_matrix_summary_frame(compare_frame)
    run_summary = build_run_summary_frame(compare_frame)
    best_by_market = build_best_by_market_frame(leaderboard)
    best_by_group = build_best_by_group_frame(leaderboard)
    best_by_matrix = build_best_by_matrix_frame(leaderboard)
    best_by_run = build_best_by_run_frame(compare_frame)

    assert group_summary[["market", "group_name"]].values.tolist() == [["sol", "core"], ["btc", "swing"]]
    assert matrix_summary[["market", "group_name", "matrix_parent_run_name"]].values.tolist() == [["sol", "core", "base"]]
    assert matrix_summary.loc[0, "stake_usd_values"] == [1.0, 2.0]
    assert matrix_summary.loc[0, "best_matrix_stake_label"] == "stake_1usd"
    assert group_summary.loc[0, "cases"] == 3
    assert group_summary.loc[0, "completed_cases"] == 2
    assert group_summary.loc[0, "failed_cases"] == 1
    assert group_summary.loc[0, "reused_cases"] == 1
    assert group_summary.loc[0, "best_run_name"] == "base"
    assert group_summary.loc[0, "best_variant_label"] == "tight"
    assert group_summary.loc[0, "best_roi_pct"] == 180.0
    assert group_summary.loc[0, "total_pnl_sum"] == 2.5
    assert group_summary.loc[0, "total_trades"] == 20

    assert run_summary[["market", "group_name", "run_name"]].values.tolist() == [
        ["sol", "core", "base"],
        ["btc", "swing", "probe"],
        ["sol", "core", "hedge"],
    ]
    assert run_summary.loc[0, "cases"] == 2
    assert run_summary.loc[0, "completed_cases"] == 2
    assert run_summary.loc[0, "avg_roi_pct"] == 125.0
    assert run_summary.loc[0, "best_variant_label"] == "tight"
    assert run_summary.loc[0, "best_roi_pct"] == 180.0

    assert best_by_market[["market", "variant_label"]].values.tolist() == [["sol", "tight"], ["btc", "default"]]
    assert best_by_group[["market", "group_name", "variant_label"]].values.tolist() == [
        ["sol", "core", "tight"],
        ["btc", "swing", "default"],
    ]
    assert best_by_matrix[["market", "group_name", "matrix_parent_run_name", "variant_label"]].values.tolist() == [
        ["sol", "core", "base", "tight"],
    ]
    assert best_by_run[["market", "group_name", "run_name", "variant_label"]].values.tolist() == [
        ["sol", "core", "base", "tight"],
        ["btc", "swing", "probe", "default"],
    ]


def test_render_experiment_report_renders_new_summary_sections() -> None:
    compare_frame = _sample_compare_frame()
    leaderboard = build_leaderboard(compare_frame.loc[compare_frame["status"].eq("completed")].copy())
    failed_cases = compare_frame.loc[compare_frame["status"].eq("failed"), ["market", "group_name", "run_name", "variant_label"]].copy()
    failed_cases["failure_stage"] = "backtest"
    failed_cases["error_type"] = "RuntimeError"
    failed_cases["error_message"] = "boom"
    summary = build_experiment_summary(
        suite_name="report_suite",
        run_label="report-exp",
        training_runs=pd.DataFrame(),
        backtest_runs=leaderboard,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=failed_cases,
    )

    report_text = render_experiment_report(
        summary,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=failed_cases,
    )

    assert summary["groups"] == 2
    assert summary["matrices"] == 1
    assert summary["runs"] == 3
    assert "## Group Summary" in report_text
    assert "## Matrix Summary" in report_text
    assert "## Run Summary" in report_text
    assert "## Focus Cuts" in report_text
    assert "### Best Per Market" in report_text
    assert "### Best Per Group" in report_text
    assert "### Best Variant Per Matrix" in report_text
    assert "### Best Variant Per Run" in report_text
    assert "No notable runtime flags." not in report_text
    assert "tight" in report_text
    assert "boom" in report_text


def test_build_experiment_summary_aggregates_factor_signals_from_profitable_traded_cases(tmp_path: Path) -> None:
    winner_train_dir = tmp_path / "winner-train"
    loser_train_dir = tmp_path / "loser-train"
    idle_train_dir = tmp_path / "idle-train"
    _write_training_offset_summary(
        winner_train_dir,
        offset=7,
        positive=[("q_bs_up_strike", 0.60), ("ret_from_cycle_open", 0.48)],
        negative=[("volume_z_3", -0.02)],
    )
    _write_training_offset_summary(
        winner_train_dir,
        offset=8,
        positive=[("q_bs_up_strike", 0.58), ("bb_pos_20", 0.44)],
        negative=[("atr_14", -0.01)],
    )
    _write_training_offset_summary(
        loser_train_dir,
        offset=7,
        positive=[("noise_feature", 0.90)],
        negative=[("bad_feature", -0.90)],
    )
    _write_training_offset_summary(
        idle_train_dir,
        offset=7,
        positive=[("idle_feature", 0.50)],
        negative=[("idle_negative", -0.50)],
    )

    compare_frame = pd.DataFrame(
        [
            {
                "case_key": "eth:core:good:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "good",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": str(winner_train_dir),
                "trades": 6,
                "pnl_sum": 1.2,
                "roi_pct": 120.0,
            },
            {
                "case_key": "eth:core:bad:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "bad",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": str(loser_train_dir),
                "trades": 4,
                "pnl_sum": -0.4,
                "roi_pct": -40.0,
            },
            {
                "case_key": "eth:core:idle:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "idle",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": str(idle_train_dir),
                "trades": 0,
                "pnl_sum": 0.0,
                "roi_pct": 0.0,
            },
        ]
    )
    leaderboard = build_leaderboard(compare_frame)

    summary = build_experiment_summary(
        suite_name="factor_suite",
        run_label="factor_run",
        training_runs=pd.DataFrame(),
        backtest_runs=compare_frame,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=pd.DataFrame(),
    )

    factor_signal_summary = summary["factor_signal_summary"]
    assert factor_signal_summary["selection_mode"] == "profitable_traded"
    assert factor_signal_summary["selected_case_count"] == 1
    assert factor_signal_summary["selected_cases"][0]["case_key"] == "eth:core:good:default"
    assert factor_signal_summary["positive_factors"][0]["feature"] == "q_bs_up_strike"
    assert factor_signal_summary["positive_factors"][0]["hits"] == 2
    assert factor_signal_summary["positive_factors"][0]["offsets"] == [7, 8]
    assert factor_signal_summary["negative_factors"][0]["feature"] == "volume_z_3"
    positive_names = [row["feature"] for row in factor_signal_summary["positive_factors"]]
    negative_names = [row["feature"] for row in factor_signal_summary["negative_factors"]]
    assert "noise_feature" not in positive_names
    assert "bad_feature" not in negative_names
    assert "idle_feature" not in positive_names


def test_render_experiment_report_includes_factor_signal_section(tmp_path: Path) -> None:
    winner_train_dir = tmp_path / "winner-train"
    _write_training_offset_summary(
        winner_train_dir,
        offset=7,
        positive=[("q_bs_up_strike", 0.60), ("ret_from_cycle_open", 0.48)],
        negative=[("volume_z_3", -0.02)],
    )

    compare_frame = pd.DataFrame(
        [
            {
                "case_key": "eth:core:good:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "good",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": str(winner_train_dir),
                "trades": 6,
                "pnl_sum": 1.2,
                "roi_pct": 120.0,
            }
        ]
    )
    leaderboard = build_leaderboard(compare_frame)
    summary = build_experiment_summary(
        suite_name="factor_suite",
        run_label="factor_run",
        training_runs=pd.DataFrame(),
        backtest_runs=compare_frame,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=pd.DataFrame(),
    )

    report_text = render_experiment_report(
        summary,
        leaderboard=leaderboard,
        compare_frame=compare_frame,
        failed_cases=pd.DataFrame(),
    )

    assert "## Factor Signals From Good Cases" in report_text
    assert "q_bs_up_strike" in report_text
    assert "volume_z_3" in report_text


def test_build_factor_signal_summary_skips_cases_with_missing_training_run_dir(tmp_path: Path) -> None:
    winner_train_dir = tmp_path / "winner-train"
    _write_training_offset_summary(
        winner_train_dir,
        offset=7,
        positive=[("q_bs_up_strike", 0.60)],
        negative=[("volume_z_3", -0.02)],
    )

    compare_frame = pd.DataFrame(
        [
            {
                "case_key": "eth:core:good:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "good",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": str(winner_train_dir),
                "trades": 6,
                "pnl_sum": 1.2,
                "roi_pct": 120.0,
            },
            {
                "case_key": "eth:core:legacy:default",
                "market": "eth",
                "group_name": "core",
                "run_name": "legacy",
                "feature_set": "bs_q_replace_direction",
                "variant_label": "default",
                "target": "direction",
                "status": "completed",
                "training_run_dir": pd.NA,
                "trades": 5,
                "pnl_sum": 0.8,
                "roi_pct": 80.0,
            },
        ]
    )

    summary = build_factor_signal_summary(compare_frame)

    assert summary["selection_mode"] == "profitable_traded"
    assert summary["selected_case_count"] == 2
    assert summary["positive_factors"][0]["feature"] == "q_bs_up_strike"
    assert summary["negative_factors"][0]["feature"] == "volume_z_3"


def test_summary_builders_normalize_missing_group_and_run_keys() -> None:
    compare_frame = pd.DataFrame(
        [
            {
                "case_key": "sol::default",
                "market": "sol",
                "group_name": pd.NA,
                "run_name": pd.NA,
                "target": "direction",
                "variant_label": "default",
                "status": "completed",
                "roi_pct": 12.0,
                "pnl_sum": 0.6,
                "trades": 3,
            }
        ]
    )

    group_summary = build_group_summary_frame(compare_frame)
    run_summary = build_run_summary_frame(compare_frame)

    assert group_summary.loc[0, "market"] == "sol"
    assert group_summary.loc[0, "group_name"] == ""
    assert group_summary.loc[0, "completed_cases"] == 1
    assert run_summary.loc[0, "group_name"] == ""
    assert run_summary.loc[0, "run_name"] == ""
    assert run_summary.loc[0, "best_variant_label"] == "default"


def test_failed_only_cases_appear_cleanly_in_compare_and_report() -> None:
    failed_cases = pd.DataFrame(
        [
            {
                "case_key": "sol:core:run-a:alt",
                "market": "sol",
                "group_name": "core",
                "run_name": "run-a",
                "variant_label": "alt",
                "variant_notes": "failed-only",
                "profile": "deep_otm",
                "target": "direction",
                "tags_json": "[\"suite\"]",
                "status": "failed",
                "failure_stage": "prepare_datasets",
                "error_type": "RuntimeError",
                "error_message": "dataset boom",
            }
        ]
    )

    compare_frame = build_experiment_compare_frame(
        training_runs=pd.DataFrame(),
        backtest_runs=pd.DataFrame(),
        failed_cases=failed_cases,
    )
    summary = build_experiment_summary(
        suite_name="report_suite",
        run_label="report-exp",
        training_runs=pd.DataFrame(),
        backtest_runs=pd.DataFrame(),
        leaderboard=pd.DataFrame(),
        compare_frame=compare_frame,
        failed_cases=failed_cases,
    )
    report_text = render_experiment_report(
        summary,
        leaderboard=pd.DataFrame(),
        compare_frame=compare_frame,
        failed_cases=failed_cases,
    )

    assert compare_frame["case_key"].tolist() == ["sol:core:run-a:alt"]
    assert compare_frame["status"].tolist() == ["failed"]
    assert compare_frame["tags_json"].tolist() == ["[\"suite\"]"]
    assert summary["cases"] == 1
    assert summary["groups"] == 1
    assert summary["runs"] == 1
    assert summary["completed_cases"] == 0
    assert summary["failed_cases"] == 1
    assert "Group Summary" in report_text
    assert "Run Summary" in report_text
    assert "No per-run comparisons available." in report_text
    assert "prepare_datasets" in report_text
    assert "dataset boom" in report_text
