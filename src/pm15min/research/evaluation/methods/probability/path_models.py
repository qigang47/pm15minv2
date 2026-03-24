from __future__ import annotations

import math
from dataclasses import dataclass
from statistics import NormalDist

import numpy as np


_STD_NORMAL = NormalDist(mu=0.0, sigma=1.0)


@dataclass(frozen=True)
class GaussianRandomWalk:
    """Additive Gaussian random walk X_t = X_{t-1} + mu + sigma * Z_t."""

    step_mean: float = 0.0
    step_std: float = 1.0
    start: float = 0.0

    def __post_init__(self) -> None:
        if float(self.step_std) <= 0.0:
            raise ValueError(f"step_std must be positive, got {self.step_std}")

    def simulate_paths(
        self,
        *,
        n_paths: int,
        n_steps: int,
        rng: np.random.Generator,
        normals: np.ndarray | None = None,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Return (paths, increments) with shape (n_paths, n_steps)."""

        n_paths = int(n_paths)
        n_steps = int(n_steps)
        if n_paths <= 0 or n_steps <= 0:
            raise ValueError(f"n_paths/n_steps must be >0, got {n_paths=}, {n_steps=}")

        if normals is None:
            normals = rng.standard_normal((n_paths, n_steps))
        else:
            normals = np.asarray(normals, dtype=float)
            if normals.shape != (n_paths, n_steps):
                raise ValueError(f"normals shape mismatch: expected {(n_paths, n_steps)}, got {normals.shape}")

        increments = float(self.step_mean) + float(self.step_std) * normals
        paths = float(self.start) + np.cumsum(increments, axis=1)
        return paths, increments

    def tilted(self, gamma: float) -> "GaussianRandomWalk":
        """Exponentially tilted measure for Gaussian increments."""

        gamma = float(gamma)
        new_mu = float(self.step_mean) + (float(self.step_std) ** 2) * gamma
        return GaussianRandomWalk(step_mean=new_mu, step_std=float(self.step_std), start=float(self.start))

    def log_mgf(self, gamma: float) -> float:
        """log M(gamma) for one step increment."""

        gamma = float(gamma)
        sigma2 = float(self.step_std) ** 2
        return float(self.step_mean) * gamma + 0.5 * sigma2 * gamma * gamma

    def mgf(self, gamma: float) -> float:
        """M(gamma) for one step increment."""

        return math.exp(self.log_mgf(gamma))

    def lundberg_root(self) -> float | None:
        """Positive non-zero root solving M(gamma)=1, if it exists."""

        sigma2 = float(self.step_std) ** 2
        mu = float(self.step_mean)
        root = -2.0 * mu / sigma2
        if root <= 0.0:
            return None
        return float(root)

    def terminal_prob_ge(self, *, n_steps: int, threshold: float) -> float:
        """Closed-form P(X_T >= threshold) for Gaussian walk terminal X_T."""

        n_steps = int(n_steps)
        if n_steps <= 0:
            raise ValueError(f"n_steps must be >0, got {n_steps}")

        mean_t = float(self.start) + n_steps * float(self.step_mean)
        std_t = math.sqrt(n_steps) * float(self.step_std)
        if std_t <= 0:
            return 1.0 if mean_t >= float(threshold) else 0.0
        z = (float(threshold) - mean_t) / std_t
        return float(1.0 - _STD_NORMAL.cdf(z))


def simulate_stochastic_vol_paths(
    *,
    base_model: GaussianRandomWalk,
    n_paths: int,
    n_steps: int,
    vol_of_vol: float,
    rng: np.random.Generator,
    normals: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Path simulator with path-level stochastic volatility multiplier."""

    n_paths = int(n_paths)
    n_steps = int(n_steps)
    vol_of_vol = float(vol_of_vol)
    if n_paths <= 0 or n_steps <= 0:
        raise ValueError(f"n_paths/n_steps must be >0, got {n_paths=}, {n_steps=}")
    if vol_of_vol < 0:
        raise ValueError(f"vol_of_vol must be >=0, got {vol_of_vol}")

    if normals is None:
        normals = rng.standard_normal((n_paths, n_steps))
    else:
        normals = np.asarray(normals, dtype=float)
        if normals.shape != (n_paths, n_steps):
            raise ValueError(f"normals shape mismatch: expected {(n_paths, n_steps)}, got {normals.shape}")

    scales = np.exp(vol_of_vol * rng.standard_normal(n_paths) - 0.5 * (vol_of_vol**2))
    increments = float(base_model.step_mean) + (float(base_model.step_std) * scales[:, None]) * normals
    paths = float(base_model.start) + np.cumsum(increments, axis=1)
    return paths, increments, scales


def gbm_terminal_price(
    *,
    s0: float,
    mu: float,
    sigma: float,
    maturity: float,
    z: float | np.ndarray,
) -> np.ndarray:
    """S_T = S_0 * exp((mu - 0.5*sigma^2)T + sigma*sqrt(T)*Z)."""

    s0 = float(s0)
    mu = float(mu)
    sigma = float(sigma)
    maturity = float(maturity)
    if s0 <= 0.0:
        raise ValueError(f"s0 must be positive, got {s0}")
    if sigma < 0.0:
        raise ValueError(f"sigma must be >=0, got {sigma}")
    if maturity <= 0.0:
        raise ValueError(f"maturity must be positive, got {maturity}")

    z_arr = np.asarray(z, dtype=float)
    drift = (mu - 0.5 * sigma * sigma) * maturity
    diffusion = sigma * math.sqrt(maturity) * z_arr
    return s0 * np.exp(drift + diffusion)


def simulate_gbm_terminal_prices(
    *,
    s0: float,
    mu: float,
    sigma: float,
    maturity: float,
    n_paths: int,
    seed: int | None = None,
    rng: np.random.Generator | None = None,
    normals: np.ndarray | None = None,
) -> np.ndarray:
    """Sample GBM terminal prices under the exact one-step lognormal formula."""

    n_paths = int(n_paths)
    if n_paths <= 0:
        raise ValueError(f"n_paths must be >0, got {n_paths}")

    if rng is None:
        rng = np.random.default_rng(seed)
    if normals is None:
        normals = rng.standard_normal(n_paths)
    else:
        normals = np.asarray(normals, dtype=float)
        if normals.shape != (n_paths,):
            raise ValueError(f"normals shape mismatch: expected {(n_paths,)}, got {normals.shape}")

    return np.asarray(
        gbm_terminal_price(s0=s0, mu=mu, sigma=sigma, maturity=maturity, z=normals),
        dtype=float,
    )
