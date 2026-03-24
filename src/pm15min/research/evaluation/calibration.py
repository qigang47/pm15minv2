from __future__ import annotations

import json

import numpy as np
import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import EvaluationRunSpec
from pm15min.research.evaluation.common import (
    load_backtest_summary,
    load_backtest_trades,
    resolve_backtest_run_dir,
)
from pm15min.research.evaluation.methods.binary_metrics import (
    calibration_bins,
    summarize_binary_predictions,
)
from pm15min.research.manifests import build_manifest, write_manifest


def run_calibration_evaluation(cfg: ResearchConfig, spec: EvaluationRunSpec) -> dict[str, object]:
    backtest_dir = resolve_backtest_run_dir(
        cfg,
        profile=cfg.profile,
        spec_name=spec.backtest_spec,
        run_label=spec.backtest_run_label,
    )
    trades = load_backtest_trades(backtest_dir)
    summary = load_backtest_summary(backtest_dir)

    run_dir = cfg.layout.storage.evaluation_run_dir(
        "calibration",
        asset=cfg.asset,
        scope_label=spec.scope_label,
        run_label_text=spec.run_label,
    )
    bins_path = run_dir / "reliability.parquet"
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"

    reliability = _build_reliability_bins(trades)
    write_parquet_atomic(reliability, bins_path)

    overall = _overall_calibration_summary(trades)
    payload = {
        "category": "calibration",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": spec.scope_label,
        "backtest_run_dir": str(backtest_dir),
        "backtest_spec": spec.backtest_spec,
        **overall,
    }
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    report_path.write_text(_render_calibration_report(payload, reliability), encoding="utf-8")

    manifest = build_manifest(
        object_type="evaluation_run",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec=spec.to_dict(),
        inputs=[
            {"kind": "backtest_run", "path": str(backtest_dir)},
            {"kind": "backtest_summary", "path": str(backtest_dir / "summary.json")},
            {"kind": "backtest_trades", "path": str(backtest_dir / "trades.parquet")},
        ],
        outputs=[
            {"kind": "reliability_parquet", "path": str(bins_path)},
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
        ],
        metadata={
            "trades": int(payload["trades"]),
            "brier": payload["brier"],
            "roi_pct": summary.get("roi_pct"),
        },
    )
    write_manifest(run_dir / "manifest.json", manifest)
    return {
        "dataset": "evaluation_run",
        "category": "calibration",
        "market": cfg.asset.slug,
        "scope_label": spec.scope_label,
        "run_label": spec.run_label,
        "backtest_run_dir": str(backtest_dir),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _build_reliability_bins(trades: pd.DataFrame, *, n_bins: int = 10) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame(columns=["bin_left", "bin_right", "n", "avg_pred", "empirical_rate", "brier"])
    grouped = calibration_bins(
        trades,
        prob_col="predicted_prob",
        outcome_col="win",
        n_bins=n_bins,
    ).rename(columns={"count": "n"})
    grouped = grouped[grouped["n"] > 0].reset_index(drop=True)
    grouped["brier"] = np.square(grouped["avg_pred"] - grouped["empirical_rate"])
    return grouped[["bin_left", "bin_right", "n", "avg_pred", "empirical_rate", "brier"]]


def _overall_calibration_summary(trades: pd.DataFrame) -> dict[str, object]:
    summary = summarize_binary_predictions(trades.get("predicted_prob", ()), trades.get("win", ()))
    return {
        "trades": int(summary["count"]),
        "brier": summary["brier"],
        "avg_pred": summary["avg_pred"],
        "empirical_rate": summary["empirical_rate"],
    }


def _render_calibration_report(payload: dict[str, object], reliability: pd.DataFrame) -> str:
    lines = [
        "# Calibration Evaluation",
        "",
        f"- market: `{payload['market']}`",
        f"- profile: `{payload['profile']}`",
        f"- trades: `{payload['trades']}`",
        f"- brier: `{payload['brier']}`",
        f"- avg_pred: `{payload['avg_pred']}`",
        f"- empirical_rate: `{payload['empirical_rate']}`",
        "",
        "## Reliability Bins",
        "",
    ]
    if reliability.empty:
        lines.append("No trades available.")
    else:
        lines.append(reliability.to_markdown(index=False))
    lines.append("")
    return "\n".join(lines)
