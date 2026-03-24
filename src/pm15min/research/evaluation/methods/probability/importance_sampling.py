from __future__ import annotations

import math
from typing import Callable

import numpy as np

from pm15min.research.evaluation.methods.probability.path_models import GaussianRandomWalk
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate


EventFn = Callable[[np.ndarray], np.ndarray]


def exponential_tilt_log_weights(
    *,
    model: GaussianRandomWalk,
    terminal_values: np.ndarray,
    n_steps: int,
    gamma: float,
) -> np.ndarray:
    """Compute log(f/g) path weights for tilted Gaussian random walk."""

    term = np.asarray(terminal_values, dtype=float)
    n_steps = int(n_steps)
    gamma = float(gamma)
    sum_increments = term - float(model.start)
    return n_steps * float(model.log_mgf(gamma)) - gamma * sum_increments


def estimate_is_exponential_tilting(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_paths: int,
    n_steps: int,
    gamma: float,
    seed: int | None = None,
    self_normalized: bool = False,
) -> ProbabilityEstimate:
    """Importance sampling with exponential tilting for Gaussian increments."""

    n_paths = int(n_paths)
    n_steps = int(n_steps)
    if n_paths <= 0 or n_steps <= 0:
        raise ValueError(f"n_paths/n_steps must be >0, got {n_paths=}, {n_steps=}")

    rng = np.random.default_rng(seed)
    tilted_model = model.tilted(float(gamma))
    paths, _ = tilted_model.simulate_paths(n_paths=n_paths, n_steps=n_steps, rng=rng)

    hits = np.asarray(event_fn(paths), dtype=float)
    terminal = paths[:, -1]
    log_w = exponential_tilt_log_weights(model=model, terminal_values=terminal, n_steps=n_steps, gamma=float(gamma))

    m = float(np.max(log_w))
    w_centered = np.exp(log_w - m)

    if self_normalized:
        denom = float(np.sum(w_centered))
        if denom <= 0:
            raise ValueError("Degenerate IS weights: denominator <= 0")
        p_hat = float(np.sum(w_centered * hits) / denom)

        ratio = (w_centered * hits) / denom
        if n_paths > 1:
            stderr = float(np.std(ratio, ddof=1) * math.sqrt(n_paths))
        else:
            stderr = 0.0
    else:
        scale = math.exp(m)
        contrib = w_centered * hits
        p_hat = float(scale * np.mean(contrib))
        if n_paths > 1:
            stderr = float(scale * np.std(contrib, ddof=1) / math.sqrt(n_paths))
        else:
            stderr = 0.0

    ess = float((np.sum(w_centered) ** 2) / np.sum(w_centered**2))
    return ProbabilityEstimate(
        method="is_exp_tilt_sn" if self_normalized else "is_exp_tilt",
        p_hat=p_hat,
        stderr=stderr,
        n_paths=n_paths,
        hit_rate=float(np.mean(hits)),
        ess=ess,
        gamma=float(gamma),
        diagnostics={
            "max_log_weight": float(np.max(log_w)),
            "min_log_weight": float(np.min(log_w)),
            "weight_cv": float(np.std(w_centered) / np.mean(w_centered)) if np.mean(w_centered) > 0 else math.inf,
        },
    )


def tune_gamma_for_target_hit_rate(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_steps: int,
    target_hit_rate: float = 0.15,
    n_probe_paths: int = 5000,
    gamma_low: float = -3.0,
    gamma_high: float = 3.0,
    increasing: bool = True,
    max_expand: int = 10,
    max_iter: int = 20,
    seed: int | None = None,
) -> tuple[float, list[tuple[float, float]]]:
    """Binary-search gamma so tilted hit-rate lands near target range."""

    target = float(target_hit_rate)
    if not (0.0 < target < 1.0):
        raise ValueError(f"target_hit_rate must be in (0,1), got {target}")

    n_steps = int(n_steps)
    n_probe_paths = int(n_probe_paths)
    if n_steps <= 0 or n_probe_paths <= 0:
        raise ValueError(f"n_steps/n_probe_paths must be >0, got {n_steps=}, {n_probe_paths=}")

    rng = np.random.default_rng(seed)
    normals = rng.standard_normal((n_probe_paths, n_steps))

    trace: list[tuple[float, float]] = []

    def probe(gamma: float) -> float:
        gm = float(gamma)
        tilted = model.tilted(gm)
        paths, _ = tilted.simulate_paths(n_paths=n_probe_paths, n_steps=n_steps, rng=rng, normals=normals)
        hit = float(np.mean(np.asarray(event_fn(paths), dtype=float)))
        trace.append((gm, hit))
        return hit

    lo = float(gamma_low)
    hi = float(gamma_high)
    h_lo = probe(lo)
    h_hi = probe(hi)

    step = max(1.0, abs(hi - lo))
    for _ in range(int(max_expand)):
        if increasing:
            bracketed = h_lo <= target <= h_hi
            if bracketed:
                break
            if h_lo > target:
                hi, h_hi = lo, h_lo
                lo = lo - step
                h_lo = probe(lo)
            else:
                lo, h_lo = hi, h_hi
                hi = hi + step
                h_hi = probe(hi)
        else:
            bracketed = h_hi <= target <= h_lo
            if bracketed:
                break
            if h_hi > target:
                lo, h_lo = hi, h_hi
                hi = hi + step
                h_hi = probe(hi)
            else:
                hi, h_hi = lo, h_lo
                lo = lo - step
                h_lo = probe(lo)
        step *= 1.8
    else:
        best = min(trace, key=lambda item: abs(item[1] - target))
        return (float(best[0]), trace)

    for _ in range(int(max_iter)):
        mid = 0.5 * (lo + hi)
        h_mid = probe(mid)
        if increasing:
            if h_mid < target:
                lo = mid
            else:
                hi = mid
        else:
            if h_mid > target:
                lo = mid
            else:
                hi = mid

    gamma = 0.5 * (lo + hi)
    return (float(gamma), trace)


def is_estimate_with_auto_gamma(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_paths: int,
    n_steps: int,
    target_hit_rate: float = 0.15,
    seed: int | None = None,
    self_normalized: bool = False,
) -> ProbabilityEstimate:
    """Convenience wrapper: tune gamma then run IS estimation."""

    gamma, trace = tune_gamma_for_target_hit_rate(
        model=model,
        event_fn=event_fn,
        n_steps=n_steps,
        target_hit_rate=target_hit_rate,
        seed=seed,
    )
    out = estimate_is_exponential_tilting(
        model=model,
        event_fn=event_fn,
        n_paths=n_paths,
        n_steps=n_steps,
        gamma=gamma,
        seed=None if seed is None else int(seed) + 17,
        self_normalized=self_normalized,
    )
    out.diagnostics["target_hit_rate"] = float(target_hit_rate)
    out.diagnostics["gamma_search_steps"] = float(len(trace))
    return out
