#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from pm15min.research.automation.quick_screen import (
    ensure_training_and_bundle,
    quick_screen_rank_tuple,
    run_bundle_quick_screen,
)
from pm15min.research.config import ResearchConfig
from pm15min.research.experiments.runner import (
    _bundle_label,
    _case_key,
    _group_name,
    _run_name,
    _training_run_label,
)
from pm15min.research.experiments.specs import load_suite_definition
from pm15min.research.layout import ResearchLayout


def main() -> int:
    parser = argparse.ArgumentParser(description="Run fast factor quick-screen over a suite without full backtests")
    parser.add_argument("--suite", required=True)
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--top-k", type=int, default=2)
    args = parser.parse_args()

    layout = ResearchLayout.discover(ROOT)
    suite_path = layout.suite_spec_path(args.suite)
    suite = load_suite_definition(suite_path)
    run_dir = layout.experiment_run_dir(suite.suite_name, args.run_label)
    run_dir.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, object]] = []
    for market_spec in suite.markets:
        cfg = ResearchConfig.build(
            market=market_spec.market,
            cycle=suite.cycle,
            profile=market_spec.profile,
            source_surface="backtest",
            feature_set=market_spec.feature_set,
            label_set=market_spec.label_set,
            target=market_spec.target,
            model_family=market_spec.model_family,
            root=ROOT,
        )
        case_key = _case_key(market_spec)
        training_run_label = _training_run_label(
            run_label=args.run_label,
            market=market_spec.market,
            target=market_spec.target,
            offsets=market_spec.offsets,
            cache_key=case_key,
        )
        bundle_label = _bundle_label(
            run_label=args.run_label,
            market=market_spec.market,
            target=market_spec.target,
            offsets=market_spec.offsets,
            cache_key=case_key,
        )
        train_result, bundle_result = ensure_training_and_bundle(
            cfg=cfg,
            market_spec=market_spec,
            training_run_label=training_run_label,
            bundle_label=bundle_label,
        )
        quick_summary, _decisions = run_bundle_quick_screen(
            cfg=cfg,
            bundle_dir=Path(str(bundle_result["bundle_dir"])),
            profile=market_spec.profile,
            target=market_spec.target,
            decision_start=market_spec.decision_start,
            decision_end=market_spec.decision_end,
            parity=market_spec.parity,
        )
        row = {
            "market": market_spec.market,
            "group_name": _group_name(market_spec),
            "run_name": _run_name(market_spec),
            "feature_set": market_spec.feature_set,
            "variant_label": market_spec.variant_label,
            "training_run_label": training_run_label,
            "bundle_label": bundle_label,
            "training_run_dir": train_result.get("run_dir"),
            "bundle_dir": bundle_result.get("bundle_dir"),
            **quick_summary,
        }
        row["_rank_tuple"] = list(quick_screen_rank_tuple(row))
        rows.append(row)

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise SystemExit("No quick-screen rows produced")

    sort_keys = frame["_rank_tuple"].apply(lambda item: tuple(int(v) for v in item))
    frame["_sort_key"] = sort_keys
    frame = frame.sort_values(
        by=["market", "_sort_key", "feature_set"],
        ascending=[True, False, True],
        kind="stable",
    ).reset_index(drop=True)
    frame["rank"] = frame.groupby("market").cumcount() + 1
    frame["selected_for_formal"] = frame["rank"] <= max(1, int(args.top_k))

    output_frame = frame.drop(columns=["_sort_key"])
    leaderboard_path = run_dir / "quick_screen_leaderboard.csv"
    leaderboard_path.write_text(output_frame.to_csv(index=False), encoding="utf-8")

    summary_payload = {
        "suite_name": suite.suite_name,
        "run_label": args.run_label,
        "top_k": int(args.top_k),
        "markets": sorted({str(value) for value in output_frame["market"].tolist()}),
        "rows": int(len(output_frame)),
        "selected_rows": int(output_frame["selected_for_formal"].sum()),
        "leaderboard_path": str(leaderboard_path),
    }
    summary_path = run_dir / "quick_screen_summary.json"
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    report_lines = [
        "# Quick Screen Summary",
        "",
        f"- suite_name: `{suite.suite_name}`",
        f"- run_label: `{args.run_label}`",
        f"- top_k: `{int(args.top_k)}`",
        "",
        "## Leaderboard",
        "",
        _render_markdown_table(output_frame),
        "",
    ]
    report_path = run_dir / "quick_screen_report.md"
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    print(f"leaderboard_path={leaderboard_path}")
    print(f"summary_path={summary_path}")
    print(f"report_path={report_path}")
    return 0

def _render_markdown_table(frame: pd.DataFrame) -> str:
    rendered = frame.astype("object").where(frame.notna(), "")
    try:
        return rendered.to_markdown(index=False)
    except ImportError:
        columns = [str(column) for column in rendered.columns.tolist()]
        header = "| " + " | ".join(_markdown_cell(column) for column in columns) + " |"
        divider = "| " + " | ".join("---" for _ in columns) + " |"
        rows = [
            "| " + " | ".join(_markdown_cell(value) for value in row) + " |"
            for row in rendered.itertuples(index=False, name=None)
        ]
        return "\n".join([header, divider, *rows])


def _markdown_cell(value: object) -> str:
    text = "" if value is None else str(value)
    return text.replace("\n", "<br>").replace("|", "\\|")


if __name__ == "__main__":
    raise SystemExit(main())
