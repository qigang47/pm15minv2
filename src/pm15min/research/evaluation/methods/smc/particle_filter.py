from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def _logit_clipped(p: float, eps: float = 1e-6) -> float:
    q = min(1.0 - eps, max(eps, float(p)))
    return math.log(q / (1.0 - q))


def _systematic_resample(weights: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    n = len(weights)
    positions = (rng.random() + np.arange(n)) / n
    cum = np.cumsum(weights)
    return np.searchsorted(cum, positions, side="left")


def _weighted_quantile(values: np.ndarray, weights: np.ndarray, qs: Sequence[float]) -> np.ndarray:
    v = np.asarray(values, dtype=float)
    w = np.asarray(weights, dtype=float)
    if v.size == 0:
        return np.full(len(qs), np.nan, dtype=float)

    order = np.argsort(v)
    v_sorted = v[order]
    w_sorted = w[order]
    cw = np.cumsum(w_sorted)
    cw = cw / cw[-1]
    out = []
    for q in qs:
        out.append(float(v_sorted[np.searchsorted(cw, float(q), side="left")]))
    return np.asarray(out, dtype=float)


@dataclass(frozen=True)
class ParticleFilterConfig:
    n_particles: int = 4000
    process_sigma: float = 0.20
    obs_sigma: float = 0.04
    resample_ess_ratio: float = 0.5
    prior_yes_prob: float = 0.5
    prior_logit_std: float = 1.2

    def __post_init__(self) -> None:
        if int(self.n_particles) <= 10:
            raise ValueError(f"n_particles must be >10, got {self.n_particles}")
        if float(self.process_sigma) <= 0.0:
            raise ValueError(f"process_sigma must be positive, got {self.process_sigma}")
        if float(self.obs_sigma) <= 0.0:
            raise ValueError(f"obs_sigma must be positive, got {self.obs_sigma}")
        if not (0.0 < float(self.resample_ess_ratio) < 1.0):
            raise ValueError(f"resample_ess_ratio must be in (0,1), got {self.resample_ess_ratio}")
        if not (0.0 < float(self.prior_yes_prob) < 1.0):
            raise ValueError(f"prior_yes_prob must be in (0,1), got {self.prior_yes_prob}")
        if float(self.prior_logit_std) <= 0.0:
            raise ValueError(f"prior_logit_std must be positive, got {self.prior_logit_std}")


def run_particle_filter(
    *,
    observations: Iterable[float],
    config: ParticleFilterConfig,
    seed: int | None = None,
) -> pd.DataFrame:
    """Particle filter on latent YES-probability with logit random walk dynamics."""

    ys = np.asarray(list(observations), dtype=float)
    if ys.size == 0:
        raise ValueError("observations must be non-empty")

    cfg = config
    n = int(cfg.n_particles)
    rng = np.random.default_rng(seed)

    z0 = _logit_clipped(float(cfg.prior_yes_prob))
    z_particles = z0 + float(cfg.prior_logit_std) * rng.standard_normal(n)
    weights = np.full(n, 1.0 / n, dtype=float)

    records = []
    ess_threshold = float(cfg.resample_ess_ratio) * n

    for t, y in enumerate(ys):
        z_particles = z_particles + float(cfg.process_sigma) * rng.standard_normal(n)
        x_particles = _sigmoid(z_particles)

        if math.isfinite(float(y)):
            ll = -0.5 * ((float(y) - x_particles) / float(cfg.obs_sigma)) ** 2
            ll = ll - np.max(ll)
            weights = weights * np.exp(ll)
            w_sum = float(np.sum(weights))
            if w_sum <= 0.0 or not math.isfinite(w_sum):
                weights.fill(1.0 / n)
            else:
                weights = weights / w_sum

        ess = float(1.0 / np.sum(weights**2))
        q05, q50, q95 = _weighted_quantile(x_particles, weights, qs=[0.05, 0.50, 0.95])
        mean = float(np.sum(weights * x_particles))

        resampled = 0
        if ess < ess_threshold:
            idx = _systematic_resample(weights, rng)
            z_particles = z_particles[idx]
            weights.fill(1.0 / n)
            x_particles = _sigmoid(z_particles)
            ess = float(n)
            resampled = 1

            q05, q50, q95 = _weighted_quantile(x_particles, weights, qs=[0.05, 0.50, 0.95])
            mean = float(np.mean(x_particles))

        records.append(
            {
                "t": int(t),
                "obs": float(y) if math.isfinite(float(y)) else math.nan,
                "posterior_mean": mean,
                "posterior_q05": float(q05),
                "posterior_q50": float(q50),
                "posterior_q95": float(q95),
                "ess": ess,
                "resampled": int(resampled),
            }
        )

    return pd.DataFrame(records)
