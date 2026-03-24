from __future__ import annotations

import json
from dataclasses import dataclass

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import EvaluationRunSpec
from pm15min.research.evaluation import methods as evaluation_methods
from pm15min.research.evaluation.common import (
    load_backtest_summary,
    load_backtest_trades,
    resolve_backtest_run_dir,
)
from pm15min.research.manifests import build_manifest, write_manifest


@dataclass(frozen=True)
class PolyEvalArtifacts:
    payload: dict[str, object]
    scope_summary: pd.DataFrame
    trade_metrics: pd.DataFrame
    drift_slices: pd.DataFrame
    reliability: pd.DataFrame


def run_poly_eval_report(cfg: ResearchConfig, spec: EvaluationRunSpec) -> dict[str, object]:
    backtest_dir = resolve_backtest_run_dir(
        cfg,
        profile=cfg.profile,
        spec_name=spec.backtest_spec,
        run_label=spec.backtest_run_label,
    )
    trades = load_backtest_trades(backtest_dir)
    backtest_summary = load_backtest_summary(backtest_dir)

    run_dir = cfg.layout.storage.evaluation_run_dir(
        "poly_eval",
        asset=cfg.asset,
        scope_label=spec.scope_label,
        run_label_text=spec.run_label,
    )
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    report_path = run_dir / "report.md"
    summary_path = run_dir / "summary.json"
    metrics_path = outputs_dir / "trade_metrics.parquet"
    scope_summary_path = outputs_dir / "scope_summary.parquet"
    drift_slices_path = outputs_dir / "drift_slices.parquet"
    reliability_path = outputs_dir / "reliability.parquet"

    artifacts = _build_poly_eval_artifacts(
        trades,
        backtest_summary=backtest_summary,
        scope_label=spec.scope_label,
        backtest_dir=backtest_dir,
        market=cfg.asset.slug,
        profile=cfg.profile,
        backtest_spec=spec.backtest_spec,
    )

    write_parquet_atomic(artifacts.trade_metrics, metrics_path)
    write_parquet_atomic(artifacts.scope_summary, scope_summary_path)
    write_parquet_atomic(artifacts.drift_slices, drift_slices_path)
    write_parquet_atomic(artifacts.reliability, reliability_path)

    summary_path.write_text(json.dumps(artifacts.payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    report_path.write_text(_render_poly_eval_report(artifacts), encoding="utf-8")

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
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "trade_metrics_parquet", "path": str(metrics_path)},
            {"kind": "scope_summary_parquet", "path": str(scope_summary_path)},
            {"kind": "drift_slices_parquet", "path": str(drift_slices_path)},
            {"kind": "reliability_parquet", "path": str(reliability_path)},
        ],
        metadata={
            "trades": int(artifacts.payload["trades"]),
            "scored_trades": int(artifacts.payload["scored_trades"]),
            "scope_rows": int(artifacts.payload["scope_rows"]),
            "drift_days": int(artifacts.payload["drift_days"]),
        },
    )
    write_manifest(run_dir / "manifest.json", manifest)
    return {
        "dataset": "evaluation_run",
        "category": "poly_eval",
        "market": cfg.asset.slug,
        "scope_label": spec.scope_label,
        "run_label": spec.run_label,
        "backtest_run_dir": str(backtest_dir),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(run_dir / "manifest.json"),
    }


def _build_poly_eval_artifacts(
    trades: pd.DataFrame,
    *,
    backtest_summary: dict[str, object],
    scope_label: str,
    backtest_dir: object,
    market: str,
    profile: str,
    backtest_spec: str,
) -> PolyEvalArtifacts:
    available_offsets = _resolve_available_offsets(trades, backtest_summary)
    overall_summary = evaluation_methods.summarize_binary_predictions(
        trades.get("predicted_prob", ()),
        trades.get("win", ()),
    )
    scope_summary = _build_scope_summary(trades, scope_label=scope_label, available_offsets=available_offsets)
    trade_metrics = _build_trade_metrics(trades, available_offsets=available_offsets)
    drift_slices = evaluation_methods.summarize_trade_drift_slices(trades, ts_col="decision_ts")
    reliability = _build_reliability_bins(trades)

    computed_pnl_sum = float(pd.to_numeric(trades.get("pnl", 0.0), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    computed_stake_sum = float(pd.to_numeric(trades.get("stake", 0.0), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    computed_roi_pct = float((computed_pnl_sum / computed_stake_sum) * 100.0) if computed_stake_sum > 0.0 else 0.0

    payload = {
        "category": "poly_eval",
        "market": market,
        "profile": profile,
        "scope_label": scope_label,
        "scope_labels": scope_summary["scope_label"].astype(str).tolist(),
        "backtest_run_dir": str(backtest_dir),
        "backtest_spec": backtest_spec,
        "trades": int(len(trades)),
        "scored_trades": int(overall_summary["count"]),
        "available_offsets": list(available_offsets),
        "scope_rows": int(len(scope_summary)),
        "drift_days": int(len(drift_slices)),
        "reliability_bins": int(len(reliability)),
        "avg_pred": overall_summary["avg_pred"],
        "empirical_rate": overall_summary["empirical_rate"],
        "brier": overall_summary["brier"],
        "baseline_brier": overall_summary["baseline_brier"],
        "delta_vs_baseline": overall_summary["delta_vs_baseline"],
        "bias": overall_summary["bias"],
        "pnl_sum": backtest_summary.get("pnl_sum", computed_pnl_sum),
        "stake_sum": backtest_summary.get("stake_sum", computed_stake_sum),
        "roi_pct": backtest_summary.get("roi_pct", computed_roi_pct),
    }
    return PolyEvalArtifacts(
        payload=payload,
        scope_summary=scope_summary,
        trade_metrics=trade_metrics,
        drift_slices=drift_slices,
        reliability=reliability,
    )


def _resolve_available_offsets(
    trades: pd.DataFrame,
    backtest_summary: dict[str, object],
) -> tuple[int, ...]:
    offsets: set[int] = set()
    raw_offsets = backtest_summary.get("available_offsets")
    if isinstance(raw_offsets, (list, tuple)):
        for value in raw_offsets:
            try:
                offsets.add(int(value))
            except (TypeError, ValueError):
                continue
    if "offset" in trades.columns:
        trade_offsets = pd.to_numeric(trades["offset"], errors="coerce").dropna().astype(int)
        offsets.update(int(value) for value in trade_offsets.tolist())
    return tuple(sorted(offsets))


def _build_scope_summary(
    trades: pd.DataFrame,
    *,
    scope_label: str,
    available_offsets: tuple[int, ...],
) -> pd.DataFrame:
    rows = [_build_scope_row(trades, scope_kind="requested", scope_label=scope_label, offset=None)]
    if "offset" in trades.columns:
        offset_values = pd.to_numeric(trades["offset"], errors="coerce")
    else:
        offset_values = pd.Series(index=trades.index, dtype=float)
    for offset in available_offsets:
        rows.append(
            _build_scope_row(
                trades.loc[offset_values.eq(offset)].copy(),
                scope_kind="offset",
                scope_label=f"offset={offset}",
                offset=offset,
            )
        )
    columns = [
        "scope_kind",
        "scope_label",
        "offset",
        "trades",
        "pnl_sum",
        "stake_sum",
        "roi_pct",
        "avg_pred",
        "empirical_rate",
        "brier",
        "baseline_brier",
        "delta_vs_baseline",
        "bias",
    ]
    return pd.DataFrame(rows, columns=columns)


def _build_scope_row(
    trades: pd.DataFrame,
    *,
    scope_kind: str,
    scope_label: str,
    offset: int | None,
) -> dict[str, object]:
    summary = evaluation_methods.summarize_binary_predictions(
        trades.get("predicted_prob", ()),
        trades.get("win", ()),
    )
    pnl_sum = float(pd.to_numeric(trades.get("pnl", 0.0), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    stake_sum = float(pd.to_numeric(trades.get("stake", 0.0), errors="coerce").fillna(0.0).sum()) if not trades.empty else 0.0
    roi_pct = float((pnl_sum / stake_sum) * 100.0) if stake_sum > 0.0 else 0.0
    return {
        "scope_kind": scope_kind,
        "scope_label": scope_label,
        "offset": offset,
        "trades": int(summary["count"]),
        "pnl_sum": pnl_sum,
        "stake_sum": stake_sum,
        "roi_pct": roi_pct,
        "avg_pred": summary["avg_pred"],
        "empirical_rate": summary["empirical_rate"],
        "brier": summary["brier"],
        "baseline_brier": summary["baseline_brier"],
        "delta_vs_baseline": summary["delta_vs_baseline"],
        "bias": summary["bias"],
    }


def _build_trade_metrics(
    trades: pd.DataFrame,
    *,
    available_offsets: tuple[int, ...],
) -> pd.DataFrame:
    columns = ["offset", "trades", "win_rate", "avg_pred", "pnl_sum"]
    metrics = evaluation_methods.summarize_trade_groups(trades, group_col="offset")
    if not available_offsets:
        return metrics[columns] if not metrics.empty else pd.DataFrame(columns=columns)

    scope_frame = pd.DataFrame({"offset": list(available_offsets)})
    out = scope_frame.merge(metrics, on="offset", how="left")
    out["trades"] = out["trades"].fillna(0).astype(int)
    out["pnl_sum"] = out["pnl_sum"].fillna(0.0)
    return out[columns]


def _build_reliability_bins(trades: pd.DataFrame, *, n_bins: int = 10) -> pd.DataFrame:
    columns = ["bin_left", "bin_right", "n", "avg_pred", "empirical_rate", "brier"]
    if trades.empty:
        return pd.DataFrame(columns=columns)

    grouped = evaluation_methods.calibration_bins(
        trades,
        prob_col="predicted_prob",
        outcome_col="win",
        n_bins=n_bins,
    ).rename(columns={"count": "n"})
    grouped = grouped[grouped["n"] > 0].reset_index(drop=True)
    grouped["brier"] = (grouped["avg_pred"] - grouped["empirical_rate"]) ** 2
    return grouped[columns]


def _render_poly_eval_report(artifacts: PolyEvalArtifacts) -> str:
    payload = artifacts.payload
    lines = [
        "# Poly Eval Summary",
        "",
        f"- market: `{payload['market']}`",
        f"- profile: `{payload['profile']}`",
        f"- trades: `{payload['trades']}`",
        f"- scored_trades: `{payload['scored_trades']}`",
        f"- available_offsets: `{payload['available_offsets']}`",
        f"- roi_pct: `{payload['roi_pct']}`",
        f"- pnl_sum: `{payload['pnl_sum']}`",
        f"- brier: `{payload['brier']}`",
        f"- empirical_rate: `{payload['empirical_rate']}`",
        "",
        "## Scope Summary",
        "",
    ]
    lines.extend(_render_table_or_empty(artifacts.scope_summary))
    lines.extend(
        [
            "",
            "## Offset Metrics",
            "",
        ]
    )
    lines.extend(_render_table_or_empty(artifacts.trade_metrics))
    lines.extend(
        [
            "",
            "## Drift Slices",
            "",
        ]
    )
    lines.extend(_render_table_or_empty(artifacts.drift_slices))
    lines.extend(
        [
            "",
            "## Reliability Bins",
            "",
        ]
    )
    lines.extend(_render_table_or_empty(artifacts.reliability))
    lines.append("")
    return "\n".join(lines)


def _render_table_or_empty(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return ["No trades available."]
    return [frame.to_markdown(index=False)]
