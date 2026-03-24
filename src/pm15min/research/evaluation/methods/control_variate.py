from __future__ import annotations

import math
from statistics import NormalDist

import numpy as np

from pm15min.research.evaluation.methods.probability.path_models import (
    GaussianRandomWalk,
    simulate_stochastic_vol_paths,
)
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate


_STD_NORMAL = NormalDist(mu=0.0, sigma=1.0)


def bs_digital_call_probability(
    *,
    s0: float,
    strike: float,
    vol: float,
    maturity: float,
    drift: float = 0.0,
) -> float:
    """Closed-form P(S_T >= K) for lognormal model under drift."""

    s0 = float(s0)
    strike = float(strike)
    vol = float(vol)
    maturity = float(maturity)
    drift = float(drift)

    if s0 <= 0.0 or strike <= 0.0:
        raise ValueError(f"s0/strike must be positive, got {s0=}, {strike=}")
    if maturity <= 0.0:
        raise ValueError(f"maturity must be positive, got {maturity}")
    if vol < 0.0:
        raise ValueError(f"vol must be >=0, got {vol}")

    if vol == 0.0:
        st = s0 * math.exp(drift * maturity)
        return 1.0 if st >= strike else 0.0

    d = (math.log(s0 / strike) + (drift - 0.5 * vol * vol) * maturity) / (vol * math.sqrt(maturity))
    return float(_STD_NORMAL.cdf(d))


def control_variate_estimate(
    *,
    target_samples: np.ndarray,
    control_samples: np.ndarray,
    control_mean_exact: float,
    beta: float | None = None,
    method_name: str = "control_variate",
) -> ProbabilityEstimate:
    """Apply control variate correction on path-wise samples."""

    y = np.asarray(target_samples, dtype=float)
    c = np.asarray(control_samples, dtype=float)
    if y.shape != c.shape:
        raise ValueError(f"shape mismatch: target {y.shape} vs control {c.shape}")
    if y.size == 0:
        raise ValueError("samples must be non-empty")

    if beta is None:
        var_c = float(np.var(c, ddof=1)) if c.size > 1 else 0.0
        if var_c <= 0.0:
            beta_hat = 0.0
        else:
            cov_yc = float(np.cov(y, c, ddof=1)[0, 1])
            beta_hat = cov_yc / var_c
    else:
        beta_hat = float(beta)

    adjusted = y - beta_hat * (c - float(control_mean_exact))

    p_hat = float(np.mean(adjusted))
    stderr = float(np.std(adjusted, ddof=1) / math.sqrt(adjusted.size)) if adjusted.size > 1 else 0.0

    corr = math.nan
    if np.std(y) > 0 and np.std(c) > 0:
        corr = float(np.corrcoef(y, c)[0, 1])

    return ProbabilityEstimate(
        method=method_name,
        p_hat=p_hat,
        stderr=stderr,
        n_paths=int(adjusted.size),
        hit_rate=float(np.mean(y)),
        diagnostics={
            "beta": float(beta_hat),
            "corr_y_c": corr,
            "target_raw_mean": float(np.mean(y)),
            "control_sample_mean": float(np.mean(c)),
            "control_exact_mean": float(control_mean_exact),
        },
    )


def estimate_probability_with_cv_sv_vs_gaussian(
    *,
    base_model: GaussianRandomWalk,
    threshold: float,
    n_paths: int,
    n_steps: int,
    vol_of_vol: float = 0.35,
    seed: int | None = None,
) -> tuple[ProbabilityEstimate, ProbabilityEstimate]:
    """Demo CV setup: stochastic-vol target vs Gaussian control with shared random numbers."""

    n_paths = int(n_paths)
    n_steps = int(n_steps)
    if n_paths <= 1 or n_steps <= 0:
        raise ValueError(f"n_paths must be >1 and n_steps >0, got {n_paths=}, {n_steps=}")

    rng = np.random.default_rng(seed)
    normals = rng.standard_normal((n_paths, n_steps))

    target_paths, _, _ = simulate_stochastic_vol_paths(
        base_model=base_model,
        n_paths=n_paths,
        n_steps=n_steps,
        vol_of_vol=float(vol_of_vol),
        rng=rng,
        normals=normals,
    )
    y = (target_paths[:, -1] >= float(threshold)).astype(float)

    control_paths, _ = base_model.simulate_paths(n_paths=n_paths, n_steps=n_steps, rng=rng, normals=normals)
    c = (control_paths[:, -1] >= float(threshold)).astype(float)

    raw = ProbabilityEstimate(
        method="sv_raw",
        p_hat=float(np.mean(y)),
        stderr=float(np.std(y, ddof=1) / math.sqrt(y.size)),
        n_paths=int(y.size),
        hit_rate=float(np.mean(y)),
    )

    control_exact = base_model.terminal_prob_ge(n_steps=n_steps, threshold=float(threshold))
    cv = control_variate_estimate(
        target_samples=y,
        control_samples=c,
        control_mean_exact=float(control_exact),
        method_name="sv_with_cv",
    )
    return raw, cv
