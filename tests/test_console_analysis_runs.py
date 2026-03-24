from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.console.read_models.backtests import (
    describe_console_backtest_run,
    describe_console_backtest_stake_sweep,
    list_console_backtest_runs,
)
from pm15min.console.read_models.experiments import (
    describe_console_experiment_matrix,
    describe_console_experiment_run,
    list_console_experiment_runs,
)
from pm15min.research.manifests import build_manifest, write_manifest


def test_console_backtest_read_models_list_and_detail(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    run_dir = (
        root
        / "research"
        / "backtests"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "spec=baseline_truth"
        / "run=bt_console"
    )
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "report.md").write_text("# Backtest Report\n\nconsole excerpt", encoding="utf-8")
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "market": "sol",
                "cycle": "15m",
                "profile": "deep_otm",
                "spec_name": "baseline_truth",
                "target": "direction",
                "bundle_label": "bundle_a",
                "variant_label": "default",
                "stake_usd": 5.0,
                "max_notional_usd": 50.0,
                "fallback_reasons": ["policy_low_confidence"],
                "parity": {"truth": True},
                "trades": 5,
                "rejects": 2,
                "wins": 3,
                "losses": 2,
                "pnl_sum": 1.25,
                "stake_sum": 5.0,
                "avg_roi_pct": 12.5,
                "roi_pct": 25.0,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"decision_ts": "2026-03-01T00:00:00Z", "trade_number": 1, "cumulative_pnl": 0.5, "cumulative_stake": 1.0, "cumulative_trades": 1, "cumulative_roi_pct": 50.0},
            {"decision_ts": "2026-03-01T00:15:00Z", "trade_number": 2, "cumulative_pnl": 1.25, "cumulative_stake": 5.0, "cumulative_trades": 5, "cumulative_roi_pct": 25.0},
        ]
    ).to_parquet(run_dir / "equity_curve.parquet", index=False)
    pd.DataFrame(
        [
            {"stake_usd": 1.0, "pnl_sum": 0.2, "avg_roi_pct": 20.0, "roi_pct": 20.0},
            {"stake_usd": 5.0, "pnl_sum": 1.25, "avg_roi_pct": 12.5, "roi_pct": 25.0},
        ]
    ).to_parquet(run_dir / "stake_sweep.parquet", index=False)
    pd.DataFrame(
        [
            {"offset": 7, "trades": 2, "wins": 1, "pnl_sum": 0.2, "stake_sum": 2.0, "avg_roi_pct": 10.0},
            {"offset": 14, "trades": 3, "wins": 2, "pnl_sum": 1.05, "stake_sum": 3.0, "avg_roi_pct": 35.0},
        ]
    ).to_parquet(run_dir / "offset_summary.parquet", index=False)
    pd.DataFrame(
        [
            {"feature": "f_a", "abs_pnl_correlation": 0.8, "pnl_correlation": 0.8, "pnl_sum": 1.1, "avg_roi_pct": 30.0},
            {"feature": "f_b", "abs_pnl_correlation": 0.6, "pnl_correlation": -0.6, "pnl_sum": -0.4, "avg_roi_pct": -10.0},
        ]
    ).to_parquet(run_dir / "factor_pnl.parquet", index=False)
    for name in ("decisions.parquet", "trades.parquet", "rejects.parquet", "markets.parquet"):
        (run_dir / name).write_bytes(b"PAR1")
    (run_dir / "logs" / "backtest.jsonl").write_text('{"event":"done"}\n', encoding="utf-8")
    write_manifest(
        run_dir / "manifest.json",
        build_manifest(
            object_type="backtest_run",
            object_id="backtest_run:deep_otm:baseline_truth:bt_console",
            market="sol",
            cycle="15m",
            path=run_dir,
            spec={"profile": "deep_otm", "spec_name": "baseline_truth", "run_label": "bt_console"},
        ),
    )

    rows = list_console_backtest_runs(market="sol", root=root)
    assert len(rows) == 1
    assert rows[0]["profile"] == "deep_otm"
    assert rows[0]["spec_name"] == "baseline_truth"
    assert rows[0]["trades"] == 5
    assert rows[0]["win_rate_pct"] == 60.0
    assert rows[0]["result_summary"] == {
        "trades": 5,
        "wins": 3,
        "losses": 2,
        "rejects": 2,
        "pnl_sum": 1.25,
        "stake_sum": 5.0,
        "roi_pct": 25.0,
        "avg_roi_pct": 12.5,
        "win_rate_pct": 60.0,
    }
    assert rows[0]["comparison_axes"]["bundle_label"] == "bundle_a"
    assert rows[0]["comparison_axes"]["parity_keys"] == ["truth"]
    assert rows[0]["overview_cards"][0] == {"card_id": "pnl_sum", "label": "PnL", "value": 1.25}
    assert rows[0]["artifacts"]["summary"]["exists"] is True
    assert rows[0]["artifacts"]["decisions"]["exists"] is True
    assert rows[0]["artifacts"]["stake_sweep"]["exists"] is True
    assert rows[0]["action_context"]["profile"] == "deep_otm"
    assert rows[0]["action_context"]["spec_name"] == "baseline_truth"
    assert rows[0]["action_context"]["run_label"] == "bt_console"

    detail = describe_console_backtest_run(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        run_label="bt_console",
        root=root,
    )
    assert detail["exists"] is True
    assert detail["summary"]["roi_pct"] == 25.0
    assert detail["manifest"]["object_type"] == "backtest_run"
    assert "Backtest Report" in str(detail["report_excerpt"])
    assert detail["action_context"]["target"] == "direction"
    assert detail["overview_cards"][1] == {"card_id": "roi_pct", "label": "ROI %", "value": 25.0}
    assert detail["equity_curve_preview"]["row_count"] == 2
    assert detail["equity_curve_preview"]["latest_point"]["cumulative_pnl"] == 1.25
    assert detail["stake_sweep_preview"]["best_by_roi"]["stake_usd"] == 5.0
    assert detail["stake_sweep_preview"]["top_by_pnl"][0]["stake_usd"] == 5.0
    assert detail["offset_summary_preview"]["best_by_pnl"]["offset"] == 14
    assert detail["offset_summary_preview"]["top_by_roi"][0]["offset"] == 14
    assert detail["factor_pnl_preview"]["top_positive"][0]["feature"] == "f_a"
    assert detail["factor_pnl_preview"]["top_negative"][0]["feature"] == "f_b"
    assert detail["factor_pnl_preview"]["top_positive_correlation"][0]["feature"] == "f_a"
    assert detail["factor_pnl_preview"]["top_negative_correlation"][0]["feature"] == "f_b"
    assert detail["sweep_highlights"] == {
        "best_stake_by_roi": 5.0,
        "best_stake_roi_pct": 25.0,
        "best_stake_by_pnl": 5.0,
        "best_stake_pnl_sum": 1.25,
        "best_offset_by_pnl": 14,
        "best_offset_pnl_sum": 1.05,
        "best_offset_by_roi": 14,
        "best_offset_roi_pct": 35.0,
        "top_positive_factor": "f_a",
        "top_negative_factor": "f_b",
        "top_positive_correlation_factor": "f_a",
        "top_negative_correlation_factor": "f_b",
        "latest_cumulative_pnl": 1.25,
    }

    stake_sweep_detail = describe_console_backtest_stake_sweep(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        run_label="bt_console",
        root=root,
    )
    assert stake_sweep_detail["dataset"] == "console_backtest_stake_sweep_detail"
    assert stake_sweep_detail["summary"]["roi_pct"] == 25.0
    assert stake_sweep_detail["stake_sweep_preview"]["best_by_roi"]["stake_usd"] == 5.0
    assert stake_sweep_detail["surface_summary"] == {
        "row_count": 2,
        "stake_min": 1.0,
        "stake_max": 5.0,
        "roi_min": 20.0,
        "roi_max": 25.0,
        "pnl_min": 0.2,
        "pnl_max": 1.25,
    }
    assert stake_sweep_detail["highlights"] == {
        "best_by_roi": {"stake_usd": 5.0, "pnl_sum": 1.25, "avg_roi_pct": 12.5, "roi_pct": 25.0},
        "best_by_pnl": {"stake_usd": 5.0, "pnl_sum": 1.25, "avg_roi_pct": 12.5, "roi_pct": 25.0},
        "roi_pct": 25.0,
        "win_rate_pct": 60.0,
        "reject_rate_pct": 28.5714,
        "stake_usd": 5.0,
        "max_notional_usd": 50.0,
    }
    assert stake_sweep_detail["chart_rows"] == [
        {"stake_usd": 1.0, "roi_pct": 20.0, "avg_roi_pct": 20.0, "pnl_sum": 0.2},
        {"stake_usd": 5.0, "roi_pct": 25.0, "avg_roi_pct": 12.5, "pnl_sum": 1.25},
    ]
    assert stake_sweep_detail["rows_by_theme"]["top_by_roi"][0]["stake_usd"] == 5.0
    assert stake_sweep_detail["rows_by_theme"]["top_by_pnl"][0]["stake_usd"] == 5.0


def test_console_experiment_read_models_list_and_detail(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    run_dir = (
        root
        / "research"
        / "experiments"
        / "runs"
        / "suite=console_suite"
        / "run=exp_console"
    )
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "report.md").write_text("# Experiment Summary\n\nconsole excerpt", encoding="utf-8")
    (run_dir / "summary.json").write_text(
        json.dumps(
            {
                "suite_name": "console_suite",
                "run_label": "exp_console",
                "cases": 6,
                "groups": 2,
                "matrices": 2,
                "runs": 2,
                "completed_cases": 6,
                "failed_cases": 1,
                "resumed_cases": 1,
                "training_reused_cases": 2,
                "bundle_reused_cases": 2,
                "secondary_training_reused_cases": 1,
                "secondary_bundle_reused_cases": 1,
                "markets": ["ada", "btc", "eth", "sol"],
                "leaderboard_rows": 6,
                "top_roi_pct": 38.0,
                "training_rows": 6,
                "backtest_rows": 6,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {"case_key": "case_a", "market": "sol", "group_name": "main", "run_name": "stake_matrix", "variant_label": "default"},
            {"case_key": "case_b", "market": "sol", "group_name": "main", "run_name": "stake_matrix", "variant_label": "aggressive"},
        ]
    ).to_parquet(run_dir / "training_runs.parquet", index=False)
    pd.DataFrame(
        [
            {"case_key": "case_a", "roi_pct": 18.0},
            {"case_key": "case_b", "roi_pct": 38.0},
        ]
    ).to_parquet(run_dir / "backtest_runs.parquet", index=False)
    pd.DataFrame(
        [
            {
                "case_key": "case_c",
                "market": "sol",
                "group_name": "main",
                "run_name": "stake_matrix",
                "variant_label": "defensive",
                "target": "direction",
                "status": "failed",
                "failure_stage": "backtest",
                "error_type": "RuntimeError",
                "error_message": "depth missing",
            }
        ]
    ).to_parquet(run_dir / "failed_cases.parquet", index=False)
    pd.DataFrame(
        [
            {
                "rank": 1,
                "market": "sol",
                "group_name": "main",
                "run_name": "stake_matrix",
                "variant_label": "aggressive",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 12,
                "pnl_sum": 3.8,
                "roi_pct": 38.0,
                "bundle_dir": "/tmp/bundles/aggressive",
                "backtest_run_dir": "/tmp/backtests/aggressive",
            },
            {
                "rank": 2,
                "market": "sol",
                "group_name": "main",
                "run_name": "stake_matrix",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 10,
                "pnl_sum": 3.2,
                "roi_pct": 32.0,
                "bundle_dir": "/tmp/bundles/default-sol",
                "backtest_run_dir": "/tmp/backtests/default-sol",
            },
            {
                "rank": 3,
                "market": "btc",
                "group_name": "main",
                "run_name": "alt_matrix",
                "variant_label": "balanced",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 8,
                "pnl_sum": 2.4,
                "roi_pct": 24.0,
                "bundle_dir": "/tmp/bundles/balanced",
                "backtest_run_dir": "/tmp/backtests/balanced",
            },
            {
                "rank": 4,
                "market": "btc",
                "group_name": "main",
                "run_name": "alt_matrix",
                "variant_label": "defensive",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 7,
                "pnl_sum": 2.2,
                "roi_pct": 22.0,
                "bundle_dir": "/tmp/bundles/defensive",
                "backtest_run_dir": "/tmp/backtests/defensive",
            },
            {
                "rank": 5,
                "market": "eth",
                "group_name": "main",
                "run_name": "alt_matrix",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 6,
                "pnl_sum": 1.8,
                "roi_pct": 18.0,
                "bundle_dir": "/tmp/bundles/default-eth",
                "backtest_run_dir": "/tmp/backtests/default-eth",
            },
            {
                "rank": 6,
                "market": "ada",
                "group_name": "main",
                "run_name": "alt_matrix",
                "variant_label": "scout",
                "profile": "deep_otm",
                "target": "direction",
                "trades": 5,
                "pnl_sum": 1.1,
                "roi_pct": 11.0,
                "bundle_dir": "/tmp/bundles/scout",
                "backtest_run_dir": "/tmp/backtests/scout",
            },
        ]
    ).to_parquet(run_dir / "leaderboard.parquet", index=False)
    pd.DataFrame(
        [
            {
                "case_key": "case_a",
                "market": "sol",
                "group_name": "main",
                "matrix_parent_run_name": "stake_matrix",
                "matrix_stake_label": "stake_1",
                "run_name": "stake_matrix",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 1.0,
                "max_notional_usd": 5.0,
                "status": "completed",
                "trades": 8,
                "pnl_sum": 1.9,
                "roi_pct": 19.0,
                "bundle_dir": "/tmp/bundles/default",
            },
            {
                "case_key": "case_b",
                "market": "sol",
                "group_name": "main",
                "matrix_parent_run_name": "stake_matrix",
                "matrix_stake_label": "stake_5",
                "run_name": "stake_matrix",
                "variant_label": "aggressive",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 5.0,
                "max_notional_usd": 10.0,
                "status": "completed",
                "trades": 12,
                "pnl_sum": 3.8,
                "roi_pct": 38.0,
                "bundle_dir": "/tmp/bundles/aggressive",
            },
            {
                "case_key": "case_d",
                "market": "btc",
                "group_name": "main",
                "matrix_parent_run_name": "alt_matrix",
                "matrix_stake_label": "stake_2",
                "run_name": "alt_matrix",
                "variant_label": "balanced",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 2.0,
                "max_notional_usd": 6.0,
                "status": "completed",
                "trades": 8,
                "pnl_sum": 2.4,
                "roi_pct": 24.0,
                "bundle_dir": "/tmp/bundles/balanced",
            },
            {
                "case_key": "case_e",
                "market": "btc",
                "group_name": "main",
                "matrix_parent_run_name": "alt_matrix",
                "matrix_stake_label": "stake_3",
                "run_name": "alt_matrix",
                "variant_label": "defensive",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 3.0,
                "max_notional_usd": 7.0,
                "status": "completed",
                "trades": 7,
                "pnl_sum": 2.2,
                "roi_pct": 22.0,
                "bundle_dir": "/tmp/bundles/defensive",
            },
            {
                "case_key": "case_f",
                "market": "eth",
                "group_name": "main",
                "matrix_parent_run_name": "alt_matrix",
                "matrix_stake_label": "stake_4",
                "run_name": "alt_matrix",
                "variant_label": "default",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 4.0,
                "max_notional_usd": 8.0,
                "status": "completed",
                "trades": 6,
                "pnl_sum": 1.8,
                "roi_pct": 18.0,
                "bundle_dir": "/tmp/bundles/default-eth",
            },
            {
                "case_key": "case_g",
                "market": "ada",
                "group_name": "main",
                "matrix_parent_run_name": "alt_matrix",
                "matrix_stake_label": "stake_9",
                "run_name": "alt_matrix",
                "variant_label": "scout",
                "profile": "deep_otm",
                "target": "direction",
                "stake_usd": 9.0,
                "max_notional_usd": 12.0,
                "status": "completed",
                "trades": 5,
                "pnl_sum": 1.1,
                "roi_pct": 11.0,
                "bundle_dir": "/tmp/bundles/scout",
            },
        ]
    ).to_parquet(run_dir / "compare.parquet", index=False)
    pd.DataFrame(
        [
            {
                "market": "sol",
                "group_name": "main",
                "matrix_parent_run_name": "stake_matrix",
                "target": "direction",
                "cases": 2,
                "completed_cases": 2,
                "failed_cases": 0,
                "avg_roi_pct": 28.5,
                "stake_usd_values": [1.0, 5.0],
                "max_notional_usd_values": [5.0, 10.0],
                "best_run_name": "stake_matrix",
                "best_matrix_stake_label": "stake_5",
                "best_variant_label": "aggressive",
                "best_roi_pct": 38.0,
                "total_pnl_sum": 5.7,
                "total_trades": 20,
            }
        ]
    ).to_parquet(run_dir / "matrix_summary.parquet", index=False)
    pd.DataFrame(
        [
            {
                "market": "sol",
                "group_name": "main",
                "run_name": "stake_matrix",
                "target": "direction",
                "variant_label": "aggressive",
                "status": "completed",
                "reference_variant_label": "default",
                "best_completed_variant_label": "aggressive",
                "best_completed_roi_pct": 38.0,
                "rank_in_run_by_roi": 1,
                "roi_pct_delta_vs_reference": 19.0,
                "pnl_sum_delta_vs_reference": 1.9,
                "comparison_vs_reference": "better",
                "is_best_completed_variant": True,
            },
            {
                "market": "sol",
                "group_name": "main",
                "run_name": "stake_matrix",
                "target": "direction",
                "variant_label": "default",
                "status": "completed",
                "reference_variant_label": "default",
                "best_completed_variant_label": "aggressive",
                "best_completed_roi_pct": 38.0,
                "rank_in_run_by_roi": 2,
                "roi_pct_delta_vs_reference": 0.0,
                "pnl_sum_delta_vs_reference": 0.0,
                "comparison_vs_reference": "reference",
                "is_best_completed_variant": False,
            },
        ]
    ).to_parquet(run_dir / "variant_compare.parquet", index=False)
    (run_dir / "logs" / "suite.jsonl").write_text('{"event":"done"}\n', encoding="utf-8")
    write_manifest(
        run_dir / "manifest.json",
        build_manifest(
            object_type="experiment_run",
            object_id="experiment_run:console_suite:exp_console",
            market="sol",
            cycle="15m",
            path=run_dir,
            spec={"suite_name": "console_suite", "run_label": "exp_console"},
        ),
    )

    rows = list_console_experiment_runs(root=root)
    assert len(rows) == 1
    assert rows[0]["suite_name"] == "console_suite"
    assert rows[0]["cases"] == 6
    assert rows[0]["failed_cases"] == 1
    assert rows[0]["markets_count"] == 4
    assert rows[0]["comparison_overview"]["best_case_variant_label"] == "aggressive"
    assert rows[0]["best_case"]["roi_pct"] == 38.0
    assert rows[0]["best_matrix"]["best_matrix_stake_label"] == "stake_5"
    assert rows[0]["best_variant"]["comparison_vs_reference"] == "better"
    assert rows[0]["artifacts"]["leaderboard"]["exists"] is True
    assert rows[0]["action_context"]["suite_name"] == "console_suite"
    assert rows[0]["action_context"]["run_label"] == "exp_console"

    detail = describe_console_experiment_run(
        suite_name="console_suite",
        run_label="exp_console",
        root=root,
    )
    assert detail["exists"] is True
    assert detail["summary"]["completed_cases"] == 6
    assert detail["manifest"]["object_type"] == "experiment_run"
    assert "Experiment Summary" in str(detail["report_excerpt"])
    assert detail["action_context"]["run_dir"] == str(run_dir)
    assert detail["leaderboard_preview"]["row_count"] == 6
    assert detail["leaderboard_preview"]["rows"][0]["variant_label"] == "aggressive"
    assert detail["compare_preview"]["rows"][0]["matrix_stake_label"] == "stake_5"
    assert detail["matrix_summary_preview"]["rows"][0]["best_matrix_stake_label"] == "stake_5"
    assert detail["variant_compare_preview"]["rows"][0]["variant_label"] == "aggressive"
    assert detail["failed_cases_preview"]["rows"][0]["error_type"] == "RuntimeError"
    assert detail["best_by_market_preview"]["rows"][0]["market"] == "sol"
    assert detail["best_by_market_preview"]["rows"][-1]["market"] == "ada"
    assert detail["best_by_group_preview"]["rows"][0]["group_name"] == "main"
    assert detail["best_by_market_group_preview"]["rows"][0]["market"] == "sol"
    assert detail["best_by_market_group_preview"]["rows"][-1]["market"] == "ada"
    assert detail["best_by_run_preview"]["rows"][0]["run_name"] == "stake_matrix"
    assert detail["compare_surface_summary"] == {
        "cases": 6,
        "completed_cases": 6,
        "failed_cases": 1,
        "market_count": 4,
        "group_count": 1,
        "run_name_count": 2,
        "variant_count": 5,
        "target_count": 1,
        "stake_point_count": 6,
        "notional_point_count": 6,
        "bundle_count": 6,
        "leaderboard_row_count": 6,
        "compare_row_count": 6,
        "matrix_row_count": 1,
        "variant_row_count": 1,
        "failed_row_count": 1,
    }
    assert detail["leaderboard_surface_summary"] == {
        "market_leader_count": 4,
        "group_leader_count": 1,
        "run_leader_count": 4,
        "best_market": "sol",
        "best_market_run_name": "stake_matrix",
        "best_market_variant_label": "aggressive",
        "best_market_roi_pct": 38.0,
        "best_group_name": "main",
        "best_group_run_name": "stake_matrix",
        "best_group_variant_label": "aggressive",
        "best_group_roi_pct": 38.0,
        "best_run_market": "sol",
        "best_run_group_name": "main",
        "best_run_name": "stake_matrix",
        "best_run_variant_label": "aggressive",
        "best_run_roi_pct": 38.0,
    }
    assert detail["best_combo_summary"] == {
        "suite_name": "console_suite",
        "run_label": "exp_console",
        "best_case_market": "sol",
        "best_case_group_name": "main",
        "best_case_run_name": "stake_matrix",
        "best_case_variant_label": "aggressive",
        "best_case_bundle_dir": "/tmp/bundles/aggressive",
        "best_case_roi_pct": 38.0,
        "best_case_pnl_sum": 3.8,
        "best_matrix_market": "sol",
        "best_matrix_group_name": "main",
        "best_matrix_run_name": "stake_matrix",
        "best_matrix_stake_label": "stake_5",
        "best_matrix_variant_label": "aggressive",
        "best_matrix_roi_pct": 38.0,
        "best_variant_market": "sol",
        "best_variant_group_name": "main",
        "best_variant_run_name": "stake_matrix",
        "best_variant_label": "aggressive",
        "reference_variant_label": "default",
        "best_variant_roi_delta_vs_reference": 19.0,
        "best_variant_pnl_delta_vs_reference": 1.9,
        "best_variant_comparison": "better",
    }
    assert detail["variant_surface_summary"] == {
        "path": str(run_dir / "variant_compare.parquet"),
        "exists": True,
        "row_count": 2,
        "status_counts": {"completed": 2},
        "comparison_counts": {"better": 1, "reference": 1},
        "run_count": 1,
        "variant_count": 2,
        "reference_variant_count": 1,
        "best_variant_label": "aggressive",
        "best_variant_run_name": "stake_matrix",
        "best_variant_roi_delta_vs_reference": 19.0,
        "best_variant_pnl_delta_vs_reference": 1.9,
        "best_variant_comparison": "better",
    }
    assert detail["failure_overview"] == {
        "path": str(run_dir / "failed_cases.parquet"),
        "exists": True,
        "failed_cases": 1,
        "row_count": 1,
        "failure_stage_counts": {"backtest": 1},
        "error_type_counts": {"RuntimeError": 1},
        "market_counts": {"sol": 1},
        "group_counts": {"main": 1},
        "first_failed_case": {
            "case_key": "case_c",
            "market": "sol",
            "group_name": "main",
            "run_name": "stake_matrix",
            "variant_label": "defensive",
            "failure_stage": "backtest",
            "error_type": "RuntimeError",
        },
    }
    assert detail["compare_facets"] == {
        "markets": ["ada", "btc", "eth", "sol"],
        "groups": ["main"],
        "run_names": ["alt_matrix", "stake_matrix"],
        "variant_labels": ["aggressive", "balanced", "default", "defensive", "scout"],
        "targets": ["direction"],
        "stake_usd_values": [1.0, 2.0, 3.0, 4.0, 5.0, 9.0],
        "max_notional_usd_values": [5.0, 6.0, 7.0, 8.0, 10.0, 12.0],
        "bundle_dirs": [
            "/tmp/bundles/aggressive",
            "/tmp/bundles/balanced",
            "/tmp/bundles/default",
            "/tmp/bundles/default-eth",
            "/tmp/bundles/defensive",
            "/tmp/bundles/scout",
        ],
    }

    matrix_detail = describe_console_experiment_matrix(
        suite_name="console_suite",
        run_label="exp_console",
        root=root,
    )
    assert matrix_detail["dataset"] == "console_experiment_matrix_detail"
    assert matrix_detail["summary"]["completed_cases"] == 6
    assert matrix_detail["matrix_summary_preview"]["rows"][0]["best_matrix_stake_label"] == "stake_5"
    assert matrix_detail["surface_summary"] == {
        "path": str(run_dir / "matrix_summary.parquet"),
        "exists": True,
        "row_count": 1,
        "matrix_parent_run_count": 1,
        "market_count": 1,
        "group_count": 1,
        "target_count": 1,
        "stake_label_count": 6,
        "compare_row_count": 6,
        "completed_cases": 6,
        "failed_cases": 1,
        "best_run_name": "stake_matrix",
        "best_matrix_stake_label": "stake_5",
        "best_variant_label": "aggressive",
        "best_roi_pct": 38.0,
        "total_pnl_sum": 5.7,
        "total_trades": 20,
    }
    assert matrix_detail["highlights"] == {
        "best_matrix": {
            "market": "sol",
            "group_name": "main",
            "matrix_parent_run_name": "stake_matrix",
            "target": "direction",
            "cases": 2,
            "completed_cases": 2,
            "failed_cases": 0,
            "avg_roi_pct": 28.5,
            "stake_usd_values": [1.0, 5.0],
            "max_notional_usd_values": [5.0, 10.0],
            "best_run_name": "stake_matrix",
            "best_matrix_stake_label": "stake_5",
            "best_variant_label": "aggressive",
            "best_roi_pct": 38.0,
            "total_pnl_sum": 5.7,
            "total_trades": 20,
        },
        "best_case": {
            "rank": 1,
            "market": "sol",
            "group_name": "main",
            "run_name": "stake_matrix",
            "variant_label": "aggressive",
            "profile": "deep_otm",
            "target": "direction",
            "trades": 12,
            "pnl_sum": 3.8,
            "roi_pct": 38.0,
            "bundle_dir": "/tmp/bundles/aggressive",
            "backtest_run_dir": "/tmp/backtests/aggressive",
        },
        "best_variant": {
            "market": "sol",
            "group_name": "main",
            "run_name": "stake_matrix",
            "target": "direction",
            "variant_label": "aggressive",
            "status": "completed",
            "reference_variant_label": "default",
            "best_completed_variant_label": "aggressive",
            "rank_in_run_by_roi": 1,
            "roi_pct_delta_vs_reference": 19.0,
            "pnl_sum_delta_vs_reference": 1.9,
            "comparison_vs_reference": "better",
        },
        "best_run": {
            "rank": 1,
            "market": "sol",
            "group_name": "main",
            "run_name": "stake_matrix",
            "variant_label": "aggressive",
            "profile": "deep_otm",
            "target": "direction",
            "trades": 12,
            "pnl_sum": 3.8,
            "roi_pct": 38.0,
            "bundle_dir": "/tmp/bundles/aggressive",
            "backtest_run_dir": "/tmp/backtests/aggressive",
        },
    }
    assert matrix_detail["chart_rows"] == [
        {
            "market": "sol",
            "group_name": "main",
            "matrix_parent_run_name": "stake_matrix",
            "target": "direction",
            "cases": 2,
            "completed_cases": 2,
            "failed_cases": 0,
            "avg_roi_pct": 28.5,
            "stake_usd_values": [1.0, 5.0],
            "max_notional_usd_values": [5.0, 10.0],
            "best_run_name": "stake_matrix",
            "best_matrix_stake_label": "stake_5",
            "best_variant_label": "aggressive",
            "best_roi_pct": 38.0,
            "total_pnl_sum": 5.7,
            "total_trades": 20,
        }
    ]
    assert matrix_detail["rows_by_theme"]["compare_rows"][0]["matrix_stake_label"] == "stake_5"
    assert matrix_detail["rows_by_theme"]["leaders_by_run"][0]["run_name"] == "stake_matrix"
