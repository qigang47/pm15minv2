from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from pm15min.research.evaluation.methods.binary_metrics import brier_score
from pm15min.research.evaluation.methods.control_variate import estimate_probability_with_cv_sv_vs_gaussian
from pm15min.research.evaluation.methods.decision import TakerDecision, evaluate_two_sided_taker
from pm15min.research.evaluation.methods.events import EventSpec, build_event_fn
from pm15min.research.evaluation.methods.probability.importance_sampling import (
    estimate_is_exponential_tilting,
    is_estimate_with_auto_gamma,
)
from pm15min.research.evaluation.methods.probability.mc_estimators import (
    estimate_antithetic_probability,
    estimate_crude_probability,
    estimate_stratified_probability,
)
from pm15min.research.evaluation.methods.probability.path_models import GaussianRandomWalk
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate


@dataclass(frozen=True)
class EstimationConfig:
    method: str = "crude"
    n_paths: int = 50_000
    n_steps: int = 30
    gamma: float = 0.0
    target_hit_rate: float = 0.15
    n_strata: int = 16
    pilot_paths_per_stratum: int = 50
    self_normalized_is: bool = False
    vol_of_vol: float = 0.35


@dataclass(frozen=True)
class MarketConfig:
    yes_ask: float
    no_ask: float
    fee_rate_entry: float = 0.0
    fee_rate_exit: float = 0.0
    half_spread: float = 0.0
    extra_slippage: float = 0.0
    min_ev: float = 0.0
    min_roi: float = 0.0


def estimate_event_probability(
    *,
    model: GaussianRandomWalk,
    event_spec: EventSpec,
    config: EstimationConfig,
    seed: int | None = None,
) -> ProbabilityEstimate:
    """Unified probability-estimation entry point for all implemented methods."""

    event_fn = build_event_fn(event_spec)
    method = str(config.method).strip().lower()

    if method == "crude":
        return estimate_crude_probability(
            model=model,
            event_fn=event_fn,
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            seed=seed,
        )

    if method == "antithetic":
        n_pairs = max(1, int(config.n_paths // 2))
        return estimate_antithetic_probability(
            model=model,
            event_fn=event_fn,
            n_pairs=n_pairs,
            n_steps=config.n_steps,
            seed=seed,
        )

    if method == "stratified":
        return estimate_stratified_probability(
            model=model,
            event_fn=event_fn,
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            n_strata=config.n_strata,
            use_neyman=False,
            seed=seed,
        )

    if method == "stratified_neyman":
        return estimate_stratified_probability(
            model=model,
            event_fn=event_fn,
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            n_strata=config.n_strata,
            use_neyman=True,
            pilot_paths_per_stratum=config.pilot_paths_per_stratum,
            seed=seed,
        )

    if method == "is_exp_tilt":
        return estimate_is_exponential_tilting(
            model=model,
            event_fn=event_fn,
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            gamma=config.gamma,
            seed=seed,
            self_normalized=config.self_normalized_is,
        )

    if method == "is_auto":
        return is_estimate_with_auto_gamma(
            model=model,
            event_fn=event_fn,
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            target_hit_rate=config.target_hit_rate,
            seed=seed,
            self_normalized=config.self_normalized_is,
        )

    if method == "cv_sv":
        if event_spec.kind != "terminal_cross":
            raise ValueError("cv_sv only supports event kind='terminal_cross'")

        raw, cv = estimate_probability_with_cv_sv_vs_gaussian(
            base_model=model,
            threshold=float(event_spec.threshold),
            n_paths=config.n_paths,
            n_steps=config.n_steps,
            vol_of_vol=config.vol_of_vol,
            seed=seed,
        )
        cv.diagnostics["raw_p_hat"] = float(raw.p_hat)
        cv.diagnostics["raw_stderr"] = float(raw.stderr)
        return cv

    raise ValueError(
        "Unsupported method. Use one of: "
        "crude, antithetic, stratified, stratified_neyman, is_exp_tilt, is_auto, cv_sv"
    )


def run_deep_otm_pipeline(
    *,
    model: GaussianRandomWalk,
    event_spec: EventSpec,
    estimation: EstimationConfig,
    market: MarketConfig,
    seed: int | None = None,
) -> dict[str, object]:
    """End-to-end: estimate q, apply taker costs, output YES/NO decisions."""

    est = estimate_event_probability(model=model, event_spec=event_spec, config=estimation, seed=seed)

    yes_decision, no_decision, best = evaluate_two_sided_taker(
        yes_probability=float(est.p_hat),
        yes_price=float(market.yes_ask),
        no_price=float(market.no_ask),
        fee_rate_entry=float(market.fee_rate_entry),
        fee_rate_exit=float(market.fee_rate_exit),
        half_spread=float(market.half_spread),
        extra_slippage=float(market.extra_slippage),
        min_ev=float(market.min_ev),
        min_roi=float(market.min_roi),
    )

    return {
        "estimate": est,
        "yes_decision": yes_decision,
        "no_decision": no_decision,
        "best_decision": best,
    }


def brier_from_backtest(*, predicted_probs: np.ndarray, outcomes: np.ndarray) -> float:
    """Thin wrapper to keep pipeline-level API complete."""

    return brier_score(predicted_probs, outcomes)


def render_pipeline_markdown(result: dict[str, object]) -> str:
    """Human-readable report for CLI/file output."""

    est: ProbabilityEstimate = result["estimate"]  # type: ignore[assignment]
    yes: TakerDecision = result["yes_decision"]  # type: ignore[assignment]
    no: TakerDecision = result["no_decision"]  # type: ignore[assignment]
    best: TakerDecision = result["best_decision"]  # type: ignore[assignment]

    lo, hi = est.ci95()
    lines = [
        "# Deep OTM Evaluation",
        "",
        "## Probability Estimate",
        "",
        f"- method: `{est.method}`",
        f"- p_hat: `{est.p_hat:.6f}`",
        f"- stderr: `{est.stderr:.6f}`",
        f"- 95% CI: `[{lo:.6f}, {hi:.6f}]`",
        f"- n_paths: `{est.n_paths}`",
        f"- hit_rate_in_sim: `{est.hit_rate:.6f}`",
        f"- ess: `{est.ess if est.ess is not None else 'n/a'}`",
        f"- gamma: `{est.gamma if est.gamma is not None else 'n/a'}`",
        "",
        "## Taker Decisions",
        "",
        f"- YES EV/share: `{yes.expected_value:.6f}` (trade={yes.should_trade})",
        f"- NO EV/share: `{no.expected_value:.6f}` (trade={no.should_trade})",
        f"- BEST side: `{best.side}` with EV/share `{best.expected_value:.6f}`",
    ]

    if est.diagnostics:
        lines.append("")
        lines.append("## Diagnostics")
        lines.append("")
        for key in sorted(est.diagnostics.keys()):
            value = est.diagnostics[key]
            lines.append(f"- {key}: `{value:.6f}`")

    lines.append("")
    return "\n".join(lines)
