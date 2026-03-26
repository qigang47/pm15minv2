from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from pm15min.data.io.parquet import write_parquet_atomic
from pm15min.research.evaluation.abm_eval import run_abm_evaluation
from pm15min.research.evaluation.methods.abm import ABMConfig
from pm15min.research.evaluation.methods.copula_risk import CopulaRiskConfig, run_copula_tail_risk
from pm15min.research.evaluation.methods.events import EventSpec
from pm15min.research.evaluation.methods.pipeline import (
    EstimationConfig,
    MarketConfig,
    render_pipeline_markdown,
    run_deep_otm_pipeline,
)
from pm15min.research.evaluation.methods.probability.path_models import GaussianRandomWalk
from pm15min.research.evaluation.methods.production_stack import (
    ProductionStackConfig,
    render_production_stack_markdown,
    run_production_stack_demo,
)
from pm15min.research.evaluation.methods.smc.particle_filter import ParticleFilterConfig, run_particle_filter
from pm15min.research.evaluation.poly_eval_scopes import (
    POLY_EVAL_ROUTED_SCOPES,
    normalize_poly_eval_scope,
    resolve_poly_eval_alias_scope,
)
from pm15min.research.manifests import build_manifest, write_manifest


@dataclass(frozen=True)
class ResearchCliDeps:
    ResearchConfig: type
    DateWindow: type
    TrainingSetSpec: type
    TrainingRunSpec: type
    ModelBundleSpec: type
    BacktestRunSpec: type
    EvaluationRunSpec: type
    describe_research_runtime: Callable[..., dict[str, Any]]
    list_training_runs: Callable[..., list[dict[str, Any]]]
    list_model_bundles: Callable[..., list[dict[str, Any]]]
    get_active_bundle_selection: Callable[..., dict[str, Any]]
    activate_model_bundle: Callable[..., dict[str, Any]]
    build_feature_frame_dataset: Callable[..., dict[str, Any]]
    build_label_frame_dataset: Callable[..., dict[str, Any]]
    build_training_set_dataset: Callable[..., dict[str, Any]]
    train_research_run: Callable[..., dict[str, Any]]
    build_model_bundle: Callable[..., dict[str, Any]]
    run_research_backtest: Callable[..., dict[str, Any]]
    run_experiment_suite: Callable[..., dict[str, Any]]
    run_calibration_evaluation: Callable[..., dict[str, Any]]
    run_drift_evaluation: Callable[..., dict[str, Any]]
    run_poly_eval_report: Callable[..., dict[str, Any]]


def _print_payload(payload: Any, *, sort_keys: bool = True) -> int:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=sort_keys))
    return 0


def _build_config(args: argparse.Namespace, deps: ResearchCliDeps):
    return deps.ResearchConfig.build(
        market=args.market,
        cycle=getattr(args, "cycle", "15m"),
        profile=getattr(args, "profile", "default"),
        source_surface=getattr(args, "source_surface", "backtest"),
        feature_set=getattr(args, "feature_set", "deep_otm_v1"),
        label_set=getattr(args, "label_set", "truth"),
        target=getattr(args, "target", "direction"),
        model_family=getattr(args, "model_family", "deep_otm"),
        run_prefix=getattr(args, "run_prefix", None),
    )


def _parse_offsets(raw: str) -> tuple[int, ...]:
    items = [part.strip() for part in str(raw or "").split(",") if part.strip()]
    if not items:
        raise ValueError("offset list cannot be empty")
    return tuple(int(part) for part in items)


def _parse_float_list(raw: str | None) -> list[float]:
    if raw is None:
        return []
    values = []
    for token in str(raw).split(","):
        text = token.strip()
        if text:
            values.append(float(text))
    return values


def _parse_string_list(raw: str | None) -> tuple[str, ...]:
    if raw is None:
        return ()
    return tuple(token.strip() for token in str(raw).split(",") if token.strip())


def _parse_json_mapping(raw: str | None) -> dict[str, Any]:
    if raw is None or not str(raw).strip():
        return {}
    payload = json.loads(str(raw))
    if not isinstance(payload, dict):
        raise ValueError("Expected a JSON object.")
    return {str(key): value for key, value in payload.items()}


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


def _write_json(path: Path, payload: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path


def _prepare_evaluation_run_dir(cfg: Any, *, category: str, scope_label: str, run_label: str) -> tuple[Path, Path]:
    run_dir = cfg.layout.storage.evaluation_run_dir(
        category,
        asset=cfg.asset,
        scope_label=scope_label,
        run_label_text=run_label,
    )
    outputs_dir = run_dir / "outputs"
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return run_dir, outputs_dir


def _write_evaluation_manifest(
    cfg: Any,
    deps: ResearchCliDeps,
    *,
    category: str,
    scope_label: str,
    run_label: str,
    run_dir: Path,
    spec: dict[str, Any],
    inputs: list[dict[str, Any]],
    outputs: list[dict[str, Any]],
    metadata: dict[str, Any],
) -> Path:
    object_id = deps.EvaluationRunSpec(
        category=category,
        scope_label=scope_label,
        run_label=run_label,
    ).object_id
    manifest = build_manifest(
        object_type="evaluation_run",
        object_id=object_id,
        market=cfg.asset.slug,
        cycle=cfg.cycle,
        path=run_dir,
        spec=_json_ready(spec),
        inputs=_json_ready(inputs),
        outputs=_json_ready(outputs),
        metadata=_json_ready(metadata),
    )
    return write_manifest(run_dir / "manifest.json", manifest)


def _resolve_eval_category(args: argparse.Namespace, *, category: str | None = None) -> str:
    return str(category or args.research_evaluate_command)


def _resolve_eval_scope_label(args: argparse.Namespace, *, scope_label: str | None = None) -> str:
    return str(args.scope if scope_label is None else scope_label)


def _run_deep_otm_demo_evaluation(
    cfg: Any,
    args: argparse.Namespace,
    deps: ResearchCliDeps,
    *,
    category: str | None = None,
    scope_label: str | None = None,
) -> dict[str, object]:
    eval_category = _resolve_eval_category(args, category=category)
    eval_scope_label = _resolve_eval_scope_label(args, scope_label=scope_label)
    run_dir, outputs_dir = _prepare_evaluation_run_dir(
        cfg,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
    )
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    estimate_path = outputs_dir / "estimate.json"
    decisions_path = outputs_dir / "decisions.json"

    model = GaussianRandomWalk(
        step_mean=float(args.step_mean),
        step_std=float(args.step_std),
        start=float(args.start),
    )
    if args.event_kind == "terminal_cross":
        event_spec = EventSpec(kind="terminal_cross", threshold=float(args.threshold), direction=str(args.direction))
    else:
        event_spec = EventSpec(
            kind="last_n_comeback",
            lookback_steps=int(args.lookback_steps),
            min_deficit=float(args.min_deficit),
            recovery_level=float(args.recovery_level),
        )
    estimation = EstimationConfig(
        method=str(args.method),
        n_paths=int(args.n_paths),
        n_steps=int(args.n_steps),
        gamma=float(args.gamma),
        target_hit_rate=float(args.target_hit_rate),
        n_strata=int(args.n_strata),
        pilot_paths_per_stratum=int(args.pilot_paths_per_stratum),
        self_normalized_is=bool(args.self_normalized_is),
        vol_of_vol=float(args.vol_of_vol),
    )
    market = MarketConfig(
        yes_ask=float(args.yes_ask),
        no_ask=float(args.no_ask),
        fee_rate_entry=float(args.fee_rate_entry),
        fee_rate_exit=float(args.fee_rate_exit),
        half_spread=float(args.half_spread),
        extra_slippage=float(args.extra_slippage),
        min_ev=float(args.min_ev),
        min_roi=float(args.min_roi),
    )
    result = run_deep_otm_pipeline(
        model=model,
        event_spec=event_spec,
        estimation=estimation,
        market=market,
        seed=int(args.seed),
    )
    estimate = result["estimate"]
    yes = result["yes_decision"]
    no = result["no_decision"]
    best = result["best_decision"]

    summary_payload = {
        "category": eval_category,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "estimate_method": estimate.method,
        "p_hat": estimate.p_hat,
        "stderr": estimate.stderr,
        "n_paths": estimate.n_paths,
        "best_side": best.side,
        "best_should_trade": best.should_trade,
        "best_expected_value": best.expected_value,
    }
    _write_json(summary_path, summary_payload)
    _write_json(estimate_path, estimate.as_dict())
    _write_json(
        decisions_path,
        {
            "yes": yes.as_dict(),
            "no": no.as_dict(),
            "best": best.as_dict(),
        },
    )
    report_path.write_text(render_pipeline_markdown(result), encoding="utf-8")

    manifest_path = _write_evaluation_manifest(
        cfg,
        deps,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
        run_dir=run_dir,
        spec={
            "category": eval_category,
            "scope_label": eval_scope_label,
            "run_label": args.run_label,
            "seed": int(args.seed),
            "model": {
                "step_mean": float(args.step_mean),
                "step_std": float(args.step_std),
                "start": float(args.start),
            },
            "event": {
                "kind": args.event_kind,
                "threshold": float(args.threshold),
                "direction": args.direction,
                "lookback_steps": int(args.lookback_steps),
                "min_deficit": float(args.min_deficit),
                "recovery_level": float(args.recovery_level),
            },
            "estimation": {
                "method": args.method,
                "n_paths": int(args.n_paths),
                "n_steps": int(args.n_steps),
                "gamma": float(args.gamma),
                "target_hit_rate": float(args.target_hit_rate),
                "n_strata": int(args.n_strata),
                "pilot_paths_per_stratum": int(args.pilot_paths_per_stratum),
                "self_normalized_is": bool(args.self_normalized_is),
                "vol_of_vol": float(args.vol_of_vol),
            },
            "market": {
                "yes_ask": float(args.yes_ask),
                "no_ask": float(args.no_ask),
                "fee_rate_entry": float(args.fee_rate_entry),
                "fee_rate_exit": float(args.fee_rate_exit),
                "half_spread": float(args.half_spread),
                "extra_slippage": float(args.extra_slippage),
                "min_ev": float(args.min_ev),
                "min_roi": float(args.min_roi),
            },
        },
        inputs=[],
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "estimate_json", "path": str(estimate_path)},
            {"kind": "decisions_json", "path": str(decisions_path)},
        ],
        metadata={
            "estimate_method": estimate.method,
            "p_hat": estimate.p_hat,
            "best_side": best.side,
            "best_should_trade": best.should_trade,
        },
    )
    return {
        "dataset": "evaluation_run",
        "category": eval_category,
        "market": cfg.asset.slug,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _run_smc_demo_evaluation(
    cfg: Any,
    args: argparse.Namespace,
    deps: ResearchCliDeps,
    *,
    category: str | None = None,
    scope_label: str | None = None,
) -> dict[str, object]:
    eval_category = _resolve_eval_category(args, category=category)
    eval_scope_label = _resolve_eval_scope_label(args, scope_label=scope_label)
    run_dir, outputs_dir = _prepare_evaluation_run_dir(
        cfg,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
    )
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    posterior_path = outputs_dir / "posterior.parquet"
    inputs: list[dict[str, Any]] = []

    latent = None
    input_path = None
    if args.input:
        input_path = Path(args.input)
        if not input_path.exists():
            raise FileNotFoundError(input_path)
        frame = pd.read_csv(input_path, low_memory=False)
        if args.obs_col not in frame.columns:
            raise KeyError(f"Missing obs col: {args.obs_col}")
        observations = pd.to_numeric(frame[args.obs_col], errors="coerce").astype(float).to_numpy()
        inputs.append({"kind": "input_csv", "path": str(input_path)})
    else:
        n = int(args.synthetic_n)
        rng = np.random.default_rng(int(args.seed))
        z = np.empty(n, dtype=float)
        observations = np.empty(n, dtype=float)
        q = min(1.0 - 1e-6, max(1e-6, float(args.synthetic_start_prob)))
        z[0] = math.log(q / (1.0 - q))
        for idx in range(1, n):
            z[idx] = z[idx - 1] + float(args.synthetic_process_sigma) * rng.standard_normal()
        latent = 1.0 / (1.0 + np.exp(-z))
        observations = latent + float(args.synthetic_obs_sigma) * rng.standard_normal(n)
        observations = np.clip(observations, 0.0, 1.0)

    pf_cfg = ParticleFilterConfig(
        n_particles=int(args.n_particles),
        process_sigma=float(args.process_sigma),
        obs_sigma=float(args.obs_sigma),
        resample_ess_ratio=float(args.resample_ess_ratio),
        prior_yes_prob=float(args.prior_yes_prob),
        prior_logit_std=float(args.prior_logit_std),
    )
    posterior = run_particle_filter(observations=observations, config=pf_cfg, seed=int(args.seed) + 1)
    if latent is not None:
        posterior["latent_yes_prob"] = latent
        posterior["abs_err"] = (posterior["posterior_mean"] - posterior["latent_yes_prob"]).abs()
    write_parquet_atomic(posterior, posterior_path)

    avg_interval_width = float((posterior["posterior_q95"] - posterior["posterior_q05"]).mean())
    summary_payload = {
        "category": eval_category,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "rows": int(len(posterior)),
        "mean_ess": float(posterior["ess"].mean()),
        "resample_count": int(posterior["resampled"].sum()),
        "avg_interval_width": avg_interval_width,
        "input_path": str(input_path) if input_path is not None else None,
        "synthetic": input_path is None,
    }
    if latent is not None:
        summary_payload["mean_abs_error"] = float(posterior["abs_err"].mean())
    _write_json(summary_path, summary_payload)
    lines = [
        "# SMC Demo",
        "",
        f"- market: `{cfg.asset.slug}`",
        f"- profile: `{cfg.profile}`",
        f"- rows: `{len(posterior)}`",
        f"- mean_ess: `{float(posterior['ess'].mean()):.6f}`",
        f"- resample_count: `{int(posterior['resampled'].sum())}`",
        f"- avg_90pct_interval_width: `{avg_interval_width:.6f}`",
    ]
    if latent is not None:
        lines.append(f"- mean_abs_error: `{float(posterior['abs_err'].mean()):.6f}`")
    lines.extend(["", f"- posterior_path: `{posterior_path}`", ""])
    report_path.write_text("\n".join(lines), encoding="utf-8")

    manifest_path = _write_evaluation_manifest(
        cfg,
        deps,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
        run_dir=run_dir,
        spec={
            "category": eval_category,
            "scope_label": eval_scope_label,
            "run_label": args.run_label,
            "seed": int(args.seed),
            "input_path": str(input_path) if input_path is not None else None,
            "obs_col": args.obs_col,
            "config": {
                "n_particles": int(args.n_particles),
                "process_sigma": float(args.process_sigma),
                "obs_sigma": float(args.obs_sigma),
                "resample_ess_ratio": float(args.resample_ess_ratio),
                "prior_yes_prob": float(args.prior_yes_prob),
                "prior_logit_std": float(args.prior_logit_std),
            },
            "synthetic": {
                "enabled": input_path is None,
                "n": int(args.synthetic_n),
                "start_prob": float(args.synthetic_start_prob),
                "process_sigma": float(args.synthetic_process_sigma),
                "obs_sigma": float(args.synthetic_obs_sigma),
            },
        },
        inputs=inputs,
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "posterior_parquet", "path": str(posterior_path)},
        ],
        metadata={
            "rows": int(len(posterior)),
            "mean_ess": float(posterior["ess"].mean()),
            "resample_count": int(posterior["resampled"].sum()),
        },
    )
    return {
        "dataset": "evaluation_run",
        "category": eval_category,
        "market": cfg.asset.slug,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _run_copula_risk_evaluation(
    cfg: Any,
    args: argparse.Namespace,
    deps: ResearchCliDeps,
    *,
    category: str | None = None,
    scope_label: str | None = None,
) -> dict[str, object]:
    if not str(args.input or "").strip():
        raise ValueError("copula-risk requires --input")
    if not str(args.cols or "").strip():
        raise ValueError("copula-risk requires --cols")
    eval_category = _resolve_eval_category(args, category=category)
    eval_scope_label = _resolve_eval_scope_label(args, scope_label=scope_label)
    run_dir, outputs_dir = _prepare_evaluation_run_dir(
        cfg,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
    )
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    pairwise_path = outputs_dir / "pairwise_tail.parquet"
    fit_params_path = outputs_dir / "fit_params.json"
    event_probs_path = outputs_dir / "event_probs.json"
    event_thresholds_path = outputs_dir / "event_thresholds.json"

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    frame = pd.read_csv(input_path, low_memory=False)
    cols = [part.strip() for part in str(args.cols).split(",") if part.strip()]
    if len(cols) < 2:
        raise ValueError("copula-risk requires at least 2 variables")
    missing = [col for col in cols if col not in frame.columns]
    if missing:
        raise KeyError(f"Missing columns: {missing}")

    numeric = frame[cols].apply(pd.to_numeric, errors="coerce").dropna()
    if len(numeric) < 100:
        raise ValueError(f"Not enough valid rows after dropna: {len(numeric)} (need >=100)")

    event_probs = _parse_float_list(args.event_probs) if args.event_probs else None
    if event_probs is not None and len(event_probs) != len(cols):
        raise ValueError(f"--event-probs length mismatch: expected {len(cols)}, got {len(event_probs)}")
    losses = _parse_float_list(args.losses) if args.losses else None
    if losses is not None and len(losses) != len(cols):
        raise ValueError(f"--losses length mismatch: expected {len(cols)}, got {len(losses)}")

    nu_grid = tuple(_parse_float_list(args.nu_grid)) if args.nu_grid else (2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 20.0, 30.0)
    copula_cfg = CopulaRiskConfig(
        family=str(args.family),
        tail=str(args.tail),
        n_sim=int(args.n_sim),
        quantile=float(args.quantile),
        alpha=float(args.alpha),
        tail_q=float(args.tail_q),
        nu=None if args.nu is None else float(args.nu),
        nu_grid=nu_grid,
        theta=None if args.theta is None else float(args.theta),
        seed=int(args.seed),
    )
    result = run_copula_tail_risk(
        data=numeric.to_numpy(dtype=float),
        col_names=cols,
        config=copula_cfg,
        event_probs=event_probs,
        losses=losses,
    )
    write_parquet_atomic(result.pairwise_tail, pairwise_path)
    _write_json(fit_params_path, result.fit_params)
    _write_json(event_probs_path, result.event_probs)
    _write_json(event_thresholds_path, result.event_thresholds)

    summary_payload = {
        "category": eval_category,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "input_path": str(input_path),
        "columns": cols,
        "rows_used": int(len(numeric)),
        **_json_ready(result.summary),
    }
    _write_json(summary_path, summary_payload)

    summary_df = pd.DataFrame([_json_ready(result.summary)])
    fit_df = pd.DataFrame([_json_ready(result.fit_params)])
    event_probs_df = pd.DataFrame([_json_ready(result.event_probs)])
    event_thresholds_df = pd.DataFrame([_json_ready(result.event_thresholds)])
    lines = [
        "# Copula Tail-Risk Evaluation",
        "",
        f"- market: `{cfg.asset.slug}`",
        f"- profile: `{cfg.profile}`",
        f"- input_path: `{input_path}`",
        f"- rows_used: `{len(numeric)}`",
        "",
        "## Summary",
        "",
        summary_df.to_markdown(index=False),
        "",
        "## Fit Params",
        "",
        fit_df.to_markdown(index=False),
        "",
        "## Event Probabilities",
        "",
        event_probs_df.to_markdown(index=False),
        "",
        "## Event Thresholds",
        "",
        event_thresholds_df.to_markdown(index=False),
        "",
        "## Pairwise Tail Dependence",
        "",
        result.pairwise_tail.to_markdown(index=False),
        "",
    ]
    report_path.write_text("\n".join(lines), encoding="utf-8")

    manifest_path = _write_evaluation_manifest(
        cfg,
        deps,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
        run_dir=run_dir,
        spec={
            "category": eval_category,
            "scope_label": eval_scope_label,
            "run_label": args.run_label,
            "input_path": str(input_path),
            "columns": cols,
            "family": args.family,
            "tail": args.tail,
            "quantile": float(args.quantile),
            "event_probs": event_probs,
            "losses": losses,
            "n_sim": int(args.n_sim),
            "alpha": float(args.alpha),
            "tail_q": float(args.tail_q),
            "nu": None if args.nu is None else float(args.nu),
            "nu_grid": list(nu_grid),
            "theta": None if args.theta is None else float(args.theta),
            "seed": int(args.seed),
        },
        inputs=[{"kind": "input_csv", "path": str(input_path)}],
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "pairwise_tail_parquet", "path": str(pairwise_path)},
            {"kind": "fit_params_json", "path": str(fit_params_path)},
            {"kind": "event_probs_json", "path": str(event_probs_path)},
            {"kind": "event_thresholds_json", "path": str(event_thresholds_path)},
        ],
        metadata={
            "rows_used": int(len(numeric)),
            "n_vars": len(cols),
            "family": args.family,
            "tail": args.tail,
        },
    )
    return {
        "dataset": "evaluation_run",
        "category": eval_category,
        "market": cfg.asset.slug,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _run_stack_demo_evaluation(
    cfg: Any,
    args: argparse.Namespace,
    deps: ResearchCliDeps,
    *,
    category: str | None = None,
    scope_label: str | None = None,
) -> dict[str, object]:
    eval_category = _resolve_eval_category(args, category=category)
    eval_scope_label = _resolve_eval_scope_label(args, scope_label=scope_label)
    run_dir, outputs_dir = _prepare_evaluation_run_dir(
        cfg,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
    )
    summary_path = run_dir / "summary.json"
    report_path = run_dir / "report.md"
    layer1_path = outputs_dir / "layer1_feed.parquet"
    layer2_path = outputs_dir / "layer2_probs.parquet"
    layer3_path = outputs_dir / "layer3_pairwise_tail.parquet"
    layer_summaries_path = outputs_dir / "layer_summaries.json"

    stack_cfg = ProductionStackConfig(
        true_prob=float(args.true_prob),
        init_price=float(args.init_price),
        n_steps=int(args.n_steps),
        seed=int(args.seed),
        n_informed=int(args.n_informed),
        n_noise=int(args.n_noise),
        n_mm=int(args.n_mm),
        n_particles=int(args.n_particles),
        process_sigma=float(args.process_sigma),
        obs_sigma=float(args.obs_sigma),
        copula_family=str(args.copula_family),
        copula_tail=str(args.copula_tail),
        copula_n_sim=int(args.copula_n_sim),
        copula_quantile=float(args.copula_quantile),
        risk_alpha=float(args.risk_alpha),
        drawdown_alert=float(args.drawdown_alert),
    )
    result = run_production_stack_demo(stack_cfg)

    write_parquet_atomic(result.layer1_feed, layer1_path)
    write_parquet_atomic(result.layer2_probs, layer2_path)
    write_parquet_atomic(result.layer3_pairwise_tail, layer3_path)
    _write_json(layer_summaries_path, result.layer_summaries)

    summary_payload = {
        "category": eval_category,
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "layers": list(result.layer_summaries.keys()),
        "layer_summaries": _json_ready(result.layer_summaries),
    }
    _write_json(summary_path, summary_payload)
    report_path.write_text(render_production_stack_markdown(result), encoding="utf-8")

    manifest_path = _write_evaluation_manifest(
        cfg,
        deps,
        category=eval_category,
        scope_label=eval_scope_label,
        run_label=args.run_label,
        run_dir=run_dir,
        spec={
            "category": eval_category,
            "scope_label": eval_scope_label,
            "run_label": args.run_label,
            "seed": int(args.seed),
            "true_prob": float(args.true_prob),
            "init_price": float(args.init_price),
            "n_steps": int(args.n_steps),
            "n_informed": int(args.n_informed),
            "n_noise": int(args.n_noise),
            "n_mm": int(args.n_mm),
            "n_particles": int(args.n_particles),
            "process_sigma": float(args.process_sigma),
            "obs_sigma": float(args.obs_sigma),
            "copula_family": args.copula_family,
            "copula_tail": args.copula_tail,
            "copula_n_sim": int(args.copula_n_sim),
            "copula_quantile": float(args.copula_quantile),
            "risk_alpha": float(args.risk_alpha),
            "drawdown_alert": float(args.drawdown_alert),
        },
        inputs=[],
        outputs=[
            {"kind": "summary_json", "path": str(summary_path)},
            {"kind": "report_md", "path": str(report_path)},
            {"kind": "layer1_feed_parquet", "path": str(layer1_path)},
            {"kind": "layer2_probs_parquet", "path": str(layer2_path)},
            {"kind": "layer3_pairwise_tail_parquet", "path": str(layer3_path)},
            {"kind": "layer_summaries_json", "path": str(layer_summaries_path)},
        ],
        metadata={
            "layers": list(result.layer_summaries.keys()),
            "copula_family": args.copula_family,
            "copula_tail": args.copula_tail,
        },
    )
    return {
        "dataset": "evaluation_run",
        "category": eval_category,
        "market": cfg.asset.slug,
        "scope_label": eval_scope_label,
        "run_label": args.run_label,
        "run_dir": str(run_dir),
        "summary_path": str(summary_path),
        "report_path": str(report_path),
        "manifest_path": str(manifest_path),
    }


def _run_poly_eval_scope_dispatch(
    cfg: Any,
    args: argparse.Namespace,
    deps: ResearchCliDeps,
    *,
    category: str,
    scope_name: str,
) -> dict[str, object]:
    normalized_scope = normalize_poly_eval_scope(scope_name)
    scope_label = normalized_scope if category == "poly_eval" else args.scope
    if normalized_scope == "abm":
        return run_abm_evaluation(
            cfg,
            category=category,
            scope_label=scope_label,
            run_label=args.run_label,
            n_steps=int(args.n_steps),
            seed=int(args.seed),
            config=ABMConfig(
                true_prob=float(args.true_prob),
                init_price=float(args.init_price),
                n_informed=int(args.n_informed),
                n_noise=int(args.n_noise),
                n_mm=int(args.n_mm),
            ),
        )
    if normalized_scope == "deep_otm":
        return _run_deep_otm_demo_evaluation(cfg, args, deps, category=category, scope_label=scope_label)
    if normalized_scope == "smc":
        return _run_smc_demo_evaluation(cfg, args, deps, category=category, scope_label=scope_label)
    if normalized_scope == "copula_risk":
        return _run_copula_risk_evaluation(cfg, args, deps, category=category, scope_label=scope_label)
    if normalized_scope == "production_stack":
        return _run_stack_demo_evaluation(cfg, args, deps, category=category, scope_label=scope_label)
    raise ValueError(f"Unsupported poly-eval scope: {scope_name!r}; expected one of {sorted(POLY_EVAL_ROUTED_SCOPES)}")


def run_research_command(args: argparse.Namespace, *, deps: ResearchCliDeps) -> int:
    if args.research_command == "show-config":
        cfg = _build_config(args, deps)
        return _print_payload(deps.describe_research_runtime(cfg))

    if args.research_command == "show-layout":
        cfg = _build_config(args, deps)
        return _print_payload(cfg.layout.to_dict())

    if args.research_command == "list-runs":
        cfg = _build_config(args, deps)
        runs = deps.list_training_runs(
            cfg,
            model_family=args.model_family,
            target=args.target,
            prefix=args.prefix,
        )
        return _print_payload(runs, sort_keys=False)

    if args.research_command == "list-bundles":
        cfg = _build_config(args, deps)
        bundles = deps.list_model_bundles(
            cfg,
            profile=args.profile,
            target=args.target,
            prefix=args.prefix,
        )
        return _print_payload(bundles, sort_keys=False)

    if args.research_command == "show-active-bundle":
        cfg = _build_config(args, deps)
        payload = deps.get_active_bundle_selection(cfg, profile=args.profile, target=args.target)
        return _print_payload(payload)

    if args.research_command == "activate-bundle":
        cfg = _build_config(args, deps)
        payload = deps.activate_model_bundle(
            cfg,
            profile=args.profile,
            target=args.target,
            bundle_label=args.bundle_label,
            notes=args.notes,
        )
        return _print_payload(payload)

    if args.research_command == "build" and args.research_build_command == "feature-frame":
        cfg = _build_config(args, deps)
        return _print_payload(deps.build_feature_frame_dataset(cfg))

    if args.research_command == "build" and args.research_build_command == "label-frame":
        cfg = _build_config(args, deps)
        return _print_payload(deps.build_label_frame_dataset(cfg))

    if args.research_command == "build" and args.research_build_command == "training-set":
        cfg = _build_config(args, deps)
        spec = deps.TrainingSetSpec(
            feature_set=args.feature_set,
            label_set=args.label_set,
            target=args.target,
            window=deps.DateWindow.from_bounds(args.window_start, args.window_end),
            offset=args.offset,
        )
        return _print_payload(deps.build_training_set_dataset(cfg, spec))

    if args.research_command == "train" and args.research_train_command == "run":
        cfg = _build_config(args, deps)
        spec = deps.TrainingRunSpec(
            model_family=args.model_family,
            feature_set=args.feature_set,
            label_set=args.label_set,
            target=args.target,
            window=deps.DateWindow.from_bounds(args.window_start, args.window_end),
            run_label=args.run_label,
            offsets=_parse_offsets(args.offsets),
            parallel_workers=args.parallel_workers,
        )
        return _print_payload(deps.train_research_run(cfg, spec))

    if args.research_command == "bundle" and args.research_bundle_command == "build":
        cfg = _build_config(args, deps)
        spec = deps.ModelBundleSpec(
            profile=args.profile,
            target=args.target,
            bundle_label=args.bundle_label,
            offsets=_parse_offsets(args.offsets),
            source_training_run=args.source_training_run,
        )
        return _print_payload(deps.build_model_bundle(cfg, spec))

    if args.research_command == "backtest" and args.research_backtest_command == "run":
        cfg = _build_config(args, deps)
        spec = deps.BacktestRunSpec(
            profile=args.profile,
            spec_name=args.spec,
            run_label=args.run_label,
            target=args.target,
            decision_start=args.decision_start,
            decision_end=args.decision_end,
            bundle_label=args.bundle_label,
            secondary_bundle_label=args.secondary_bundle_label,
            fallback_reasons=_parse_string_list(args.fallback_reasons),
            stake_usd=args.stake_usd,
            max_notional_usd=args.max_notional_usd,
            parity=_parse_json_mapping(args.parity_json),
        )
        return _print_payload(deps.run_research_backtest(cfg, spec))

    if args.research_command == "experiment" and args.research_experiment_command == "run-suite":
        cfg = _build_config(args, deps)
        summary = deps.run_experiment_suite(
            cfg=cfg,
            suite_name=args.suite,
            run_label=args.run_label,
        )
        return _print_payload(summary)

    if args.research_command == "evaluate" and args.research_evaluate_command in {"calibration", "drift", "poly-eval"}:
        cfg = _build_config(args, deps)
        routed_scope = normalize_poly_eval_scope(args.scope) if args.research_evaluate_command == "poly-eval" else ""
        if args.research_evaluate_command == "poly-eval" and routed_scope in POLY_EVAL_ROUTED_SCOPES:
            return _print_payload(
                _run_poly_eval_scope_dispatch(
                    cfg,
                    args,
                    deps,
                    category="poly_eval",
                    scope_name=routed_scope,
                )
            )
        spec = deps.EvaluationRunSpec(
            category=args.research_evaluate_command,
            scope_label=args.scope,
            run_label=args.run_label,
            backtest_spec=args.backtest_spec,
            backtest_run_label=args.backtest_run_label,
        )
        if args.research_evaluate_command == "calibration":
            summary = deps.run_calibration_evaluation(cfg, spec)
        elif args.research_evaluate_command == "drift":
            summary = deps.run_drift_evaluation(cfg, spec)
        else:
            summary = deps.run_poly_eval_report(cfg, spec)
        return _print_payload(summary)

    alias_scope = resolve_poly_eval_alias_scope(
        args.research_evaluate_command if args.research_command == "evaluate" else None
    )
    if args.research_command == "evaluate" and alias_scope is not None:
        cfg = _build_config(args, deps)
        return _print_payload(
            _run_poly_eval_scope_dispatch(
                cfg,
                args,
                deps,
                category=str(args.research_evaluate_command),
                scope_name=alias_scope,
            )
        )

    raise SystemExit("Missing research subcommand.")
