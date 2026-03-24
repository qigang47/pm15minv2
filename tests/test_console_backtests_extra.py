from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.console.read_models.backtests import (
    describe_console_backtest_run,
    list_console_backtest_runs,
)
from pm15min.research.manifests import build_manifest, write_manifest


def test_console_backtest_extra_summaries(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    run_dir = (
        root
        / "research"
        / "backtests"
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm"
        / "spec=baseline_truth"
        / "run=bt_extra"
    )
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "report.md").write_text("# Backtest Report\n", encoding="utf-8")
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
                "max_notional_usd": 15.0,
                "secondary_bundle_label": "bundle_b",
                "fallback_reasons": ["policy_low_confidence"],
                "parity": {"regime_enabled": True},
                "trades": 6,
                "rejects": 2,
                "wins": 4,
                "losses": 2,
                "pnl_sum": 1.6,
                "stake_sum": 6.0,
                "avg_roi_pct": 15.0,
                "roi_pct": 26.6667,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-01T00:00:00Z",
                "trade_number": 1,
                "cumulative_pnl": 1.0,
                "cumulative_trades": 1,
                "cumulative_roi_pct": 100.0,
            },
            {
                "decision_ts": "2026-03-01T00:15:00Z",
                "trade_number": 2,
                "cumulative_pnl": 0.4,
                "cumulative_trades": 2,
                "cumulative_roi_pct": 20.0,
            },
            {
                "decision_ts": "2026-03-01T00:30:00Z",
                "trade_number": 3,
                "cumulative_pnl": 1.6,
                "cumulative_trades": 6,
                "cumulative_roi_pct": 26.6667,
            },
        ]
    ).to_parquet(run_dir / "equity_curve.parquet", index=False)
    pd.DataFrame(
        [
            {"stake_usd": 1.0, "max_notional_usd": 5.0, "trades": 3, "pnl_sum": 0.2, "roi_pct": 20.0},
            {"stake_usd": 5.0, "max_notional_usd": 15.0, "trades": 6, "pnl_sum": 1.6, "roi_pct": 26.6667},
            {"stake_usd": 10.0, "max_notional_usd": 20.0, "trades": 6, "pnl_sum": 1.2, "roi_pct": 12.0},
        ]
    ).to_parquet(run_dir / "stake_sweep.parquet", index=False)
    pd.DataFrame(
        [
            {"offset": 7, "trades": 2, "pnl_sum": 0.2, "avg_roi_pct": 10.0},
            {"offset": 14, "trades": 2, "pnl_sum": 0.4, "avg_roi_pct": 20.0},
            {"offset": 21, "trades": 2, "pnl_sum": 1.0, "avg_roi_pct": 50.0},
        ]
    ).to_parquet(run_dir / "offset_summary.parquet", index=False)
    pd.DataFrame(
        [
            {"feature": "f_a", "pnl_sum": 1.1, "avg_roi_pct": 25.0, "pnl_correlation": 0.7, "abs_pnl_correlation": 0.7},
            {"feature": "f_b", "pnl_sum": -0.5, "avg_roi_pct": -8.0, "pnl_correlation": -0.6, "abs_pnl_correlation": 0.6},
            {"feature": "f_c", "pnl_sum": 0.2, "avg_roi_pct": 3.0, "pnl_correlation": 0.2, "abs_pnl_correlation": 0.2},
        ]
    ).to_parquet(run_dir / "factor_pnl.parquet", index=False)
    for name in ("decisions.parquet", "trades.parquet", "rejects.parquet", "markets.parquet"):
        (run_dir / name).write_bytes(b"PAR1")
    (run_dir / "logs" / "backtest.jsonl").write_text('{"event":"done"}\n', encoding="utf-8")
    write_manifest(
        run_dir / "manifest.json",
        build_manifest(
            object_type="backtest_run",
            object_id="backtest_run:deep_otm:baseline_truth:bt_extra",
            market="sol",
            cycle="15m",
            path=run_dir,
            spec={"profile": "deep_otm", "spec_name": "baseline_truth", "run_label": "bt_extra"},
        ),
    )

    rows = list_console_backtest_runs(market="sol", root=root)
    assert len(rows) == 1
    row = rows[0]
    assert row["performance_summary"] == {
        "trades": 6,
        "wins": 4,
        "losses": 2,
        "rejects": 2,
        "pnl_sum": 1.6,
        "stake_sum": 6.0,
        "roi_pct": 26.6667,
        "avg_roi_pct": 15.0,
        "win_rate_pct": 66.6667,
        "stake_usd": 5.0,
        "max_notional_usd": 15.0,
        "secondary_bundle_label": "bundle_b",
        "considered_signals": 8,
        "reject_rate_pct": 25.0,
    }

    detail = describe_console_backtest_run(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        spec_name="baseline_truth",
        run_label="bt_extra",
        root=root,
    )
    assert detail["equity_curve_preview"]["equity_summary"] == {
        "point_count": 3,
        "latest_cumulative_pnl": 1.6,
        "latest_cumulative_roi_pct": 26.6667,
        "latest_cumulative_trades": 6,
        "max_cumulative_pnl": 1.6,
        "min_cumulative_pnl": 0.4,
        "max_drawdown_abs": -0.6,
        "max_drawdown_pct": -60.0,
    }
    assert detail["equity_summary"]["max_drawdown_abs"] == -0.6
    assert detail["stake_sweep_preview"]["surface_summary"] == {
        "row_count": 3,
        "stake_min": 1.0,
        "stake_max": 10.0,
        "roi_min": 12.0,
        "roi_max": 26.6667,
        "pnl_min": 0.2,
        "pnl_max": 1.6,
    }
    assert detail["stake_surface_summary"]["stake_max"] == 10.0
    assert detail["offset_summary_preview"]["surface_summary"] == {
        "row_count": 3,
        "offset_min": 7,
        "offset_max": 21,
        "avg_roi_min": 10.0,
        "avg_roi_max": 50.0,
        "pnl_min": 0.2,
        "pnl_max": 1.0,
    }
    assert detail["offset_surface_summary"]["offset_max"] == 21
    assert detail["factor_pnl_preview"]["surface_summary"] == {
        "row_count": 3,
        "positive_factor_count": 2,
        "negative_factor_count": 1,
        "positive_correlation_count": 2,
        "negative_correlation_count": 1,
        "best_factor_pnl_sum": 1.1,
        "worst_factor_pnl_sum": -0.5,
        "strongest_abs_correlation": 0.7,
    }
    assert detail["factor_surface_summary"]["negative_factor_count"] == 1
    assert detail["decision_summary"] == {
        "roi_pct": 26.6667,
        "win_rate_pct": 66.6667,
        "reject_rate_pct": 25.0,
        "best_stake_by_roi": 5.0,
        "best_offset_by_pnl": 21,
        "top_positive_factor": "f_a",
        "top_negative_factor": "f_b",
        "max_drawdown_abs": -0.6,
        "max_drawdown_pct": -60.0,
        "latest_cumulative_pnl": 1.6,
    }
