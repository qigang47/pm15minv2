from __future__ import annotations

import json
from dataclasses import asdict

import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.config import ResearchConfig
from pm15min.research.evaluation.methods.abm import ABMConfig, run_abm_simulation
from pm15min.research.manifests import build_manifest, write_manifest


def run_abm_evaluation(
    cfg: ResearchConfig,
    *,
    category: str,
    scope_label: str,
    run_label: str,
    config: ABMConfig,
    n_steps: int,
    seed: int,
) -> dict[str, object]:
    run_dir = cfg.layout.storage.evaluation_run_dir(
        category,
        asset=cfg.asset,
        scope_label=scope_label,
        run_label_text=run_label,
    )
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)

    simulation, summary = run_abm_simulation(
        config=config,
        n_steps=int(n_steps),
        seed=int(seed),
    )

    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    manifest_path = run_dir / "manifest.json"
    simulation_path = outputs_dir / "simulation.parquet"

    write_parquet_atomic(simulation, simulation_path)

    config_payload = asdict(config)
    summary_payload = {
        "category": str(category),
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": str(scope_label),
        "run_label": str(run_label),
        "n_steps": int(n_steps),
        "seed": int(seed),
        "simulation_rows": int(len(simulation)),
        "config": config_payload,
        **summary,
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    report_path.write_text(_render_abm_report(summary_payload, simulation=simulation), encoding="utf-8")

    manifest = build_manifest(
        object_type="evaluation_run",
        object_id=f"evaluation_run:{cfg.asset.slug}:{category}:{scope_label}:{run_label}",
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec={
            "category": str(category),
            "scope_label": str(scope_label),
            "run_label": str(run_label),
            "n_steps": int(n_steps),
            "seed": int(seed),
            "config": config_payload,
        },
        inputs=[],
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "simulation_parquet", "path": str(simulation_path)},
        ],
        metadata={
            "simulation_rows": int(len(simulation)),
            "trade_count": int(summary.get("trade_count", 0)),
            "total_volume": float(summary.get("total_volume", 0.0)),
            "final_price": float(summary.get("final_price", 0.0)),
        },
    )
    write_manifest(manifest_path, manifest)
    return {
        "dataset": "evaluation_run",
        "category": str(category),
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": str(scope_label),
        "run_label": str(run_label),
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _render_abm_report(summary: dict[str, object], *, simulation: pd.DataFrame) -> str:
    config = summary.get("config", {})
    lines = [
        "# ABM Evaluation",
        "",
        f"- market: `{summary.get('market')}`",
        f"- profile: `{summary.get('profile')}`",
        f"- scope_label: `{summary.get('scope_label')}`",
        f"- run_label: `{summary.get('run_label')}`",
        f"- n_steps: `{summary.get('n_steps')}`",
        f"- simulation_rows: `{summary.get('simulation_rows')}`",
        f"- final_price: `{summary.get('final_price')}`",
        f"- final_abs_error: `{summary.get('final_abs_error')}`",
        f"- mean_abs_error: `{summary.get('mean_abs_error')}`",
        f"- total_volume: `{summary.get('total_volume')}`",
        f"- trade_count: `{summary.get('trade_count')}`",
        "",
        "## Config",
        "",
        pd.DataFrame([config]).to_markdown(index=False),
        "",
        "## Tail Snapshot",
        "",
        simulation.tail(5).to_markdown(index=False),
        "",
    ]
    return "\n".join(lines)
