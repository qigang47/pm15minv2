from __future__ import annotations

import math
from statistics import NormalDist
from typing import Callable

import numpy as np

from pm15min.research.evaluation.methods.probability.path_models import GaussianRandomWalk
from pm15min.research.evaluation.methods.probability.types import ProbabilityEstimate


EventFn = Callable[[np.ndarray], np.ndarray]
_STD_NORMAL = NormalDist(mu=0.0, sigma=1.0)


def _bernoulli_stderr_from_samples(samples: np.ndarray) -> float:
    arr = np.asarray(samples, dtype=float)
    n = int(arr.size)
    if n <= 1:
        return 0.0
    return float(np.std(arr, ddof=1) / math.sqrt(n))


def _estimate_from_hits(
    *,
    method: str,
    hits: np.ndarray,
    diagnostics: dict[str, float] | None = None,
) -> ProbabilityEstimate:
    h = np.asarray(hits, dtype=float)
    n = int(h.size)
    if n <= 0:
        raise ValueError("hits must be non-empty")
    p_hat = float(np.mean(h))
    stderr = _bernoulli_stderr_from_samples(h)
    return ProbabilityEstimate(
        method=method,
        p_hat=p_hat,
        stderr=stderr,
        n_paths=n,
        hit_rate=p_hat,
        diagnostics=diagnostics or {},
    )


def estimate_crude_probability(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_paths: int,
    n_steps: int,
    seed: int | None = None,
) -> ProbabilityEstimate:
    """Crude Monte Carlo event probability estimator."""

    rng = np.random.default_rng(seed)
    paths, _ = model.simulate_paths(n_paths=int(n_paths), n_steps=int(n_steps), rng=rng)
    hits = np.asarray(event_fn(paths), dtype=bool)
    return _estimate_from_hits(method="crude_mc", hits=hits.astype(float))


def estimate_antithetic_probability(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_pairs: int,
    n_steps: int,
    seed: int | None = None,
) -> ProbabilityEstimate:
    """Antithetic variates estimator using Z and -Z paired paths."""

    n_pairs = int(n_pairs)
    n_steps = int(n_steps)
    if n_pairs <= 0:
        raise ValueError(f"n_pairs must be >0, got {n_pairs}")

    rng = np.random.default_rng(seed)
    z = rng.standard_normal((n_pairs, n_steps))

    paths_a, _ = model.simulate_paths(n_paths=n_pairs, n_steps=n_steps, rng=rng, normals=z)
    paths_b, _ = model.simulate_paths(n_paths=n_pairs, n_steps=n_steps, rng=rng, normals=-z)

    hits_a = np.asarray(event_fn(paths_a), dtype=float)
    hits_b = np.asarray(event_fn(paths_b), dtype=float)

    pair_vals = 0.5 * (hits_a + hits_b)
    p_hat = float(np.mean(pair_vals))
    stderr = _bernoulli_stderr_from_samples(pair_vals)

    corr = math.nan
    if np.std(hits_a) > 0 and np.std(hits_b) > 0:
        corr = float(np.corrcoef(hits_a, hits_b)[0, 1])

    return ProbabilityEstimate(
        method="antithetic",
        p_hat=p_hat,
        stderr=stderr,
        n_paths=int(2 * n_pairs),
        hit_rate=float(np.mean(np.concatenate([hits_a, hits_b]))),
        diagnostics={"pair_corr": corr},
    )


def _equal_allocation(n_total: int, n_strata: int) -> np.ndarray:
    base = n_total // n_strata
    rem = n_total % n_strata
    alloc = np.full(n_strata, base, dtype=int)
    if rem > 0:
        alloc[:rem] += 1
    return alloc


def _neyman_allocation(*, n_total: int, weights: np.ndarray, sigmas: np.ndarray) -> np.ndarray:
    n_total = int(n_total)
    if n_total < 0:
        raise ValueError(f"n_total must be >=0, got {n_total}")

    raw = np.asarray(weights, dtype=float) * np.asarray(sigmas, dtype=float)
    if np.all(raw <= 0):
        return _equal_allocation(n_total, len(weights))

    share = raw / raw.sum()
    exact = n_total * share
    alloc = np.floor(exact).astype(int)
    shortfall = int(n_total - alloc.sum())
    if shortfall > 0:
        frac = exact - alloc
        order = np.argsort(-frac)
        alloc[order[:shortfall]] += 1
    return alloc


def _sample_stratum_first_normal(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n: int,
    n_steps: int,
    u_low: float,
    u_high: float,
    rng: np.random.Generator,
) -> np.ndarray:
    if n <= 0:
        return np.empty(0, dtype=float)

    u = rng.uniform(u_low, u_high, size=n)
    z0 = np.fromiter((_STD_NORMAL.inv_cdf(float(x)) for x in u), dtype=float, count=n)
    z_rest = rng.standard_normal((n, n_steps))
    z_rest[:, 0] = z0

    paths, _ = model.simulate_paths(n_paths=n, n_steps=n_steps, rng=rng, normals=z_rest)
    return np.asarray(event_fn(paths), dtype=float)


def estimate_stratified_probability(
    *,
    model: GaussianRandomWalk,
    event_fn: EventFn,
    n_paths: int,
    n_steps: int,
    n_strata: int = 16,
    use_neyman: bool = False,
    pilot_paths_per_stratum: int = 50,
    seed: int | None = None,
) -> ProbabilityEstimate:
    """Stratified estimator over first normal draw (via U stratification)."""

    n_paths = int(n_paths)
    n_steps = int(n_steps)
    n_strata = int(n_strata)
    if n_paths <= 0 or n_steps <= 0:
        raise ValueError(f"n_paths/n_steps must be >0, got {n_paths=}, {n_steps=}")
    if n_strata <= 1:
        raise ValueError(f"n_strata must be >1, got {n_strata}")
    if n_paths < n_strata:
        raise ValueError(f"n_paths must be >= n_strata for coverage, got {n_paths=} < {n_strata=}")

    rng = np.random.default_rng(seed)
    weights = np.full(n_strata, 1.0 / n_strata, dtype=float)
    bounds = np.linspace(0.0, 1.0, n_strata + 1)

    stratum_hits: list[list[np.ndarray]] = [[] for _ in range(n_strata)]

    def _simulate_into(j: int, n: int) -> None:
        if n <= 0:
            return
        h = _sample_stratum_first_normal(
            model=model,
            event_fn=event_fn,
            n=int(n),
            n_steps=n_steps,
            u_low=float(bounds[j]),
            u_high=float(bounds[j + 1]),
            rng=rng,
        )
        stratum_hits[j].append(h)

    if use_neyman:
        pilot = max(2, int(pilot_paths_per_stratum))
        pilot_total = pilot * n_strata
        if pilot_total > n_paths:
            pilot = max(1, n_paths // n_strata)
            pilot_total = pilot * n_strata

        for j in range(n_strata):
            _simulate_into(j, pilot)

        sigmas = np.zeros(n_strata, dtype=float)
        for j in range(n_strata):
            h = np.concatenate(stratum_hits[j]) if stratum_hits[j] else np.empty(0, dtype=float)
            if h.size <= 1:
                sigmas[j] = 0.0
            else:
                sigmas[j] = float(np.std(h, ddof=1))

        remaining = int(n_paths - pilot_total)
        extra_alloc = _neyman_allocation(n_total=remaining, weights=weights, sigmas=sigmas)
        for j in range(n_strata):
            _simulate_into(j, int(extra_alloc[j]))
    else:
        alloc = _equal_allocation(n_paths, n_strata)
        for j in range(n_strata):
            _simulate_into(j, int(alloc[j]))

    means = np.zeros(n_strata, dtype=float)
    variances = np.zeros(n_strata, dtype=float)
    counts = np.zeros(n_strata, dtype=int)

    for j in range(n_strata):
        h = np.concatenate(stratum_hits[j]) if stratum_hits[j] else np.empty(0, dtype=float)
        counts[j] = int(h.size)
        if h.size == 0:
            means[j] = 0.0
            variances[j] = 0.0
            continue
        means[j] = float(np.mean(h))
        variances[j] = float(np.var(h, ddof=1)) if h.size > 1 else 0.0

    p_hat = float(np.sum(weights * means))
    var_hat = float(np.sum((weights**2) * (variances / np.maximum(counts, 1))))
    stderr = float(math.sqrt(max(0.0, var_hat)))
    total_n = int(counts.sum())
    hit_rate = float(np.sum(means * counts) / total_n) if total_n > 0 else 0.0

    diagnostics = {
        "n_strata": float(n_strata),
        "min_stratum_n": float(np.min(counts)),
        "max_stratum_n": float(np.max(counts)),
    }
    return ProbabilityEstimate(
        method="stratified" + ("_neyman" if use_neyman else ""),
        p_hat=p_hat,
        stderr=stderr,
        n_paths=total_n,
        hit_rate=hit_rate,
        diagnostics=diagnostics,
    )
