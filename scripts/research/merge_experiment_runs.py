from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.experiments.leaderboard import build_leaderboard
from pm15min.research.experiments.merge import merge_compare_frames
from pm15min.research.experiments.reports import (
    build_experiment_summary,
    render_experiment_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge a primary experiment run with a supplemental backfill run.")
    parser.add_argument("--primary-run-dir", required=True, help="Original experiment run directory.")
    parser.add_argument("--supplemental-run-dir", required=True, help="Supplemental experiment run directory.")
    parser.add_argument("--out-run-dir", required=True, help="Output directory for merged artifacts.")
    parser.add_argument("--suite-name", default=None, help="Optional merged suite name override.")
    parser.add_argument("--run-label", default=None, help="Optional merged run label override.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    primary_run_dir = Path(args.primary_run_dir)
    supplemental_run_dir = Path(args.supplemental_run_dir)
    out_run_dir = Path(args.out_run_dir)
    out_run_dir.mkdir(parents=True, exist_ok=True)

    primary_compare = pd.read_parquet(primary_run_dir / "compare.parquet")
    supplemental_compare = pd.read_parquet(supplemental_run_dir / "compare.parquet")
    merged_compare = merge_compare_frames(primary_compare, supplemental_compare)
    completed_backtests = merged_compare.loc[merged_compare.get("status", pd.Series(dtype="string")).astype("string").eq("completed")].copy()
    leaderboard = build_leaderboard(completed_backtests)
    failed_cases = merged_compare.loc[merged_compare.get("status", pd.Series(dtype="string")).astype("string").eq("failed")].copy()

    primary_summary = json.loads((primary_run_dir / "summary.json").read_text(encoding="utf-8"))
    suite_name = str(args.suite_name or primary_summary.get("suite_name") or "merged_experiment")
    run_label = str(args.run_label or primary_summary.get("run_label") or "merged")
    summary = build_experiment_summary(
        suite_name=suite_name,
        run_label=run_label,
        training_runs=pd.DataFrame(),
        backtest_runs=completed_backtests,
        leaderboard=leaderboard,
        compare_frame=merged_compare,
        failed_cases=failed_cases,
    )
    report = render_experiment_report(
        summary,
        leaderboard=leaderboard,
        compare_frame=merged_compare,
        failed_cases=failed_cases,
    )

    write_parquet_atomic(merged_compare, out_run_dir / "compare.parquet")
    merged_compare.to_csv(out_run_dir / "compare.csv", index=False)
    write_parquet_atomic(leaderboard, out_run_dir / "leaderboard.parquet")
    leaderboard.to_csv(out_run_dir / "leaderboard.csv", index=False)
    write_parquet_atomic(failed_cases, out_run_dir / "failed_cases.parquet")
    failed_cases.to_csv(out_run_dir / "failed_cases.csv", index=False)
    (out_run_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )
    (out_run_dir / "report.md").write_text(report, encoding="utf-8")
    (out_run_dir / "merge_inputs.json").write_text(
        json.dumps(
            {
                "primary_run_dir": str(primary_run_dir),
                "supplemental_run_dir": str(supplemental_run_dir),
                "out_run_dir": str(out_run_dir),
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
