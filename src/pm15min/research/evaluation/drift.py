from __future__ import annotations

import json

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import EvaluationRunSpec
from pm15min.research.evaluation.common import (
    load_backtest_summary,
    load_backtest_trades,
    resolve_backtest_run_dir,
)
from pm15min.research.evaluation.methods.trade_metrics import summarize_trade_drift_slices
from pm15min.research.manifests import build_manifest, write_manifest


def run_drift_evaluation(cfg: ResearchConfig, spec: EvaluationRunSpec) -> dict[str, object]:
    backtest_dir = resolve_backtest_run_dir(
        cfg,
        profile=cfg.profile,
        spec_name=spec.backtest_spec,
        run_label=spec.backtest_run_label,
    )
    trades = load_backtest_trades(backtest_dir)
    backtest_summary = load_backtest_summary(backtest_dir)

    run_dir = cfg.layout.storage.evaluation_run_dir(
        "drift",
        asset=cfg.asset,
        scope_label=spec.scope_label,
        run_label_text=spec.run_label,
    )
    slices_path = run_dir / "slices.parquet"
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"

    slices = _build_drift_slices(trades)
    write_parquet_atomic(slices, slices_path)

    payload = {
        "category": "drift",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": spec.scope_label,
        "backtest_run_dir": str(backtest_dir),
        "backtest_spec": spec.backtest_spec,
        "days": int(len(slices)),
        "trades": int(len(trades)),
        "pnl_sum": float(trades["pnl"].sum()) if not trades.empty else 0.0,
        "roi_pct": backtest_summary.get("roi_pct"),
    }
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    report_path.write_text(_render_drift_report(payload, slices), encoding="utf-8")

    manifest = build_manifest(
        object_type="evaluation_run",
        object_id=spec.object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec=spec.to_dict(),
        inputs=[
            {"kind": "backtest_run", "path": str(backtest_dir)},
            {"kind": "backtest_trades", "path": str(backtest_dir / "trades.parquet")},
        ],
        outputs=[
            {"kind": "slices_parquet", "path": str(slices_path)},
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
        ],
        metadata={
            "days": int(len(slices)),
            "trades": int(len(trades)),
        },
    )
    write_manifest(run_dir / "manifest.json", manifest)
    return {
        "dataset": "evaluation_run",
        "category": "drift",
        "market": cfg.asset.slug,
        "scope_label": spec.scope_label,
        "run_label": spec.run_label,
        "backtest_run_dir": str(backtest_dir),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _build_drift_slices(trades: pd.DataFrame) -> pd.DataFrame:
    return summarize_trade_drift_slices(trades, ts_col="decision_ts")


def _render_drift_report(payload: dict[str, object], slices: pd.DataFrame) -> str:
    lines = [
        "# Drift Evaluation",
        "",
        f"- market: `{payload['market']}`",
        f"- profile: `{payload['profile']}`",
        f"- days: `{payload['days']}`",
        f"- trades: `{payload['trades']}`",
        f"- pnl_sum: `{payload['pnl_sum']}`",
        "",
        "## Daily Slices",
        "",
    ]
    if slices.empty:
        lines.append("No trades available.")
    else:
        lines.append(slices.to_markdown(index=False))
    lines.append("")
    return "\n".join(lines)
