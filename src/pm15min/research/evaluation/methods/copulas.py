from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Iterable, Sequence

import numpy as np
import pandas as pd
from scipy import stats
from scipy.special import gammaln


@dataclass(frozen=True)
class GaussianCopulaFit:
    corr: np.ndarray


@dataclass(frozen=True)
class TCopulaFit:
    corr: np.ndarray
    nu: float
    loglik: float


@dataclass(frozen=True)
class ArchimedeanFit:
    theta: float
    avg_kendall_tau: float


def _as_2d_array(x: np.ndarray | pd.DataFrame | Sequence[Sequence[float]]) -> np.ndarray:
    if isinstance(x, pd.DataFrame):
        arr = x.to_numpy(dtype=float)
    else:
        arr = np.asarray(x, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D array, got shape {arr.shape}")
    if arr.shape[0] == 0 or arr.shape[1] == 0:
        raise ValueError(f"Empty input not allowed, got shape {arr.shape}")
    return arr


def _clip_unit_interval(u: np.ndarray, eps: float = 1e-6) -> np.ndarray:
    return np.clip(np.asarray(u, dtype=float), eps, 1.0 - eps)


def make_correlation_psd(corr: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    """Project a symmetric matrix to a valid correlation matrix."""

    c = np.asarray(corr, dtype=float)
    if c.ndim != 2 or c.shape[0] != c.shape[1]:
        raise ValueError(f"corr must be square matrix, got shape {c.shape}")

    c = 0.5 * (c + c.T)
    np.fill_diagonal(c, 1.0)

    eigvals, eigvecs = np.linalg.eigh(c)
    eigvals = np.clip(eigvals, float(eps), None)
    c_psd = eigvecs @ np.diag(eigvals) @ eigvecs.T

    scale = np.sqrt(np.diag(c_psd))
    c_psd = c_psd / np.outer(scale, scale)
    np.fill_diagonal(c_psd, 1.0)
    return 0.5 * (c_psd + c_psd.T)


def cholesky_factor(corr: np.ndarray, eps: float = 1e-8) -> np.ndarray:
    """Return a lower-triangular Cholesky factor for a correlation matrix."""

    c = make_correlation_psd(corr, eps=eps)
    try:
        return np.linalg.cholesky(c)
    except np.linalg.LinAlgError:
        return np.linalg.cholesky(make_correlation_psd(c, eps=max(float(eps), 1e-6)))


def vine_pair_copula_count(dim: int) -> int:
    """Return the number of pair copulas in a d-dimensional vine copula."""

    d = int(dim)
    if d <= 1:
        raise ValueError(f"dim must be >1, got {dim}")
    return d * (d - 1) // 2


def pseudo_observations(x: np.ndarray | pd.DataFrame | Sequence[Sequence[float]]) -> np.ndarray:
    """Transform each marginal into pseudo-uniform observations via ranks."""

    arr = _as_2d_array(x)
    n, d = arr.shape
    u = np.empty_like(arr, dtype=float)
    for column in range(d):
        ranks = stats.rankdata(arr[:, column], method="average")
        u[:, column] = ranks / (n + 1.0)
    return _clip_unit_interval(u)


def apply_sklar_inverse_cdfs(
    u: np.ndarray | pd.DataFrame | Sequence[Sequence[float]],
    inverse_cdfs: Sequence[Callable[[np.ndarray], np.ndarray] | Callable[[float], float]],
) -> np.ndarray:
    """Map copula uniforms to marginals through inverse CDF callables."""

    arr = _clip_unit_interval(_as_2d_array(u))
    inverse_functions = list(inverse_cdfs)
    dim = arr.shape[1]
    if len(inverse_functions) != dim:
        raise ValueError(f"inverse_cdfs length mismatch: expected {dim}, got {len(inverse_functions)}")

    out = np.empty_like(arr, dtype=float)
    for column, inverse_cdf in enumerate(inverse_functions):
        values = arr[:, column]
        mapped: np.ndarray | None = None
        try:
            raw = inverse_cdf(values)
            cast = np.asarray(raw, dtype=float)
            if cast.shape == values.shape:
                mapped = cast
        except Exception:
            mapped = None

        if mapped is None:
            mapped = np.fromiter((float(inverse_cdf(float(v))) for v in values), dtype=float, count=values.size)
        out[:, column] = mapped
    return out


def estimate_average_kendall_tau(u: np.ndarray) -> float:
    arr = _as_2d_array(u)
    dim = arr.shape[1]
    if dim < 2:
        return 0.0

    taus: list[float] = []
    for i in range(dim):
        for j in range(i + 1, dim):
            tau, _ = stats.kendalltau(arr[:, i], arr[:, j], nan_policy="omit")
            if math.isfinite(float(tau)):
                taus.append(float(tau))
    if not taus:
        return 0.0
    return float(np.mean(taus))


def fit_gaussian_copula(u: np.ndarray) -> GaussianCopulaFit:
    arr = _clip_unit_interval(_as_2d_array(u))
    z = stats.norm.ppf(arr)
    corr = make_correlation_psd(np.corrcoef(z, rowvar=False))
    return GaussianCopulaFit(corr=corr)


def _t_copula_loglik(u: np.ndarray, corr: np.ndarray, nu: float) -> float:
    arr = _clip_unit_interval(_as_2d_array(u))
    degrees = float(nu)
    if degrees <= 1.0:
        return -math.inf

    x = stats.t.ppf(arr, df=degrees)
    _, dim = x.shape

    corr_psd = make_correlation_psd(corr)
    sign, logdet = np.linalg.slogdet(corr_psd)
    if sign <= 0:
        return -math.inf

    inv_corr = np.linalg.inv(corr_psd)
    quad = np.einsum("ij,jk,ik->i", x, inv_corr, x)

    log_f_d = (
        gammaln((degrees + dim) / 2.0)
        - gammaln(degrees / 2.0)
        - 0.5 * dim * math.log(degrees * math.pi)
        - 0.5 * logdet
        - ((degrees + dim) / 2.0) * np.log1p(quad / degrees)
    )
    log_f_1 = (
        gammaln((degrees + 1.0) / 2.0)
        - gammaln(degrees / 2.0)
        - 0.5 * math.log(degrees * math.pi)
        - ((degrees + 1.0) / 2.0) * np.log1p((x * x) / degrees)
    )

    log_copula_density = log_f_d - np.sum(log_f_1, axis=1)
    if not np.isfinite(log_copula_density).all():
        return -math.inf
    return float(np.sum(log_copula_density))


def fit_t_copula(
    u: np.ndarray,
    *,
    nu: float | None = None,
    nu_grid: Iterable[float] = (2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 20.0, 30.0),
) -> TCopulaFit:
    arr = _clip_unit_interval(_as_2d_array(u))

    if nu is not None:
        degrees = float(nu)
        x = stats.t.ppf(arr, df=degrees)
        corr = make_correlation_psd(np.corrcoef(x, rowvar=False))
        return TCopulaFit(corr=corr, nu=degrees, loglik=_t_copula_loglik(arr, corr, degrees))

    best_fit: TCopulaFit | None = None
    for candidate in nu_grid:
        degrees = float(candidate)
        if degrees <= 1.0:
            continue
        x = stats.t.ppf(arr, df=degrees)
        corr = make_correlation_psd(np.corrcoef(x, rowvar=False))
        fit = TCopulaFit(corr=corr, nu=degrees, loglik=_t_copula_loglik(arr, corr, degrees))
        if best_fit is None or fit.loglik > best_fit.loglik:
            best_fit = fit

    if best_fit is None:
        raise ValueError("No valid nu candidate for t-copula fitting")
    return best_fit


def fit_clayton_theta_from_tau(avg_kendall_tau: float) -> ArchimedeanFit:
    tau = float(avg_kendall_tau)
    if tau <= 0.0:
        theta = 1e-4
    else:
        theta = max(1e-4, 2.0 * tau / max(1e-6, 1.0 - tau))
    return ArchimedeanFit(theta=float(theta), avg_kendall_tau=tau)


def fit_gumbel_theta_from_tau(avg_kendall_tau: float) -> ArchimedeanFit:
    tau = float(avg_kendall_tau)
    if tau <= 0.0:
        theta = 1.0
    else:
        theta = max(1.0, 1.0 / max(1e-6, 1.0 - tau))
    return ArchimedeanFit(theta=float(theta), avg_kendall_tau=tau)


def simulate_gaussian_copula(corr: np.ndarray, *, n_samples: int, seed: int | None = None) -> np.ndarray:
    corr_psd = make_correlation_psd(corr)
    count = int(n_samples)
    if count <= 0:
        raise ValueError(f"n_samples must be >0, got {n_samples}")

    rng = np.random.default_rng(seed)
    chol = cholesky_factor(corr_psd)
    z = rng.standard_normal((count, corr_psd.shape[0]))
    return _clip_unit_interval(stats.norm.cdf(z @ chol.T))


def simulate_t_copula(corr: np.ndarray, *, nu: float, n_samples: int, seed: int | None = None) -> np.ndarray:
    corr_psd = make_correlation_psd(corr)
    degrees = float(nu)
    count = int(n_samples)
    if degrees <= 1.0:
        raise ValueError(f"nu must be >1, got {nu}")
    if count <= 0:
        raise ValueError(f"n_samples must be >0, got {n_samples}")

    rng = np.random.default_rng(seed)
    chol = cholesky_factor(corr_psd)
    z = rng.standard_normal((count, corr_psd.shape[0]))
    x = z @ chol.T
    g = rng.chisquare(df=degrees, size=count)
    t_latent = x / np.sqrt(g[:, None] / degrees)
    return _clip_unit_interval(stats.t.cdf(t_latent, df=degrees))


def simulate_clayton_copula(*, theta: float, dim: int, n_samples: int, seed: int | None = None) -> np.ndarray:
    """Use the Marshall-Olkin algorithm for a d-dimensional Clayton copula."""

    dependence = float(theta)
    dimension = int(dim)
    count = int(n_samples)
    if dependence <= 0.0:
        raise ValueError(f"theta must be >0 for Clayton, got {theta}")
    if dimension <= 1:
        raise ValueError(f"dim must be >1, got {dim}")
    if count <= 0:
        raise ValueError(f"n_samples must be >0, got {n_samples}")

    rng = np.random.default_rng(seed)
    w = rng.gamma(shape=1.0 / dependence, scale=1.0, size=count)
    e = rng.exponential(scale=1.0, size=(count, dimension))
    return _clip_unit_interval((1.0 + e / w[:, None]) ** (-1.0 / dependence))


def _sample_positive_stable(alpha: float, n_samples: int, rng: np.random.Generator) -> np.ndarray:
    """Sample a positive alpha-stable random variable."""

    if not (0.0 < alpha <= 1.0):
        raise ValueError(f"alpha must be in (0,1], got {alpha}")
    if alpha == 1.0:
        return np.ones(n_samples, dtype=float)

    v = rng.uniform(-math.pi / 2.0, math.pi / 2.0, size=n_samples)
    w = rng.exponential(scale=1.0, size=n_samples)

    part1 = np.sin(alpha * (v + math.pi / 2.0)) / np.power(np.cos(v), 1.0 / alpha)
    part2 = np.power(np.cos(v - alpha * (v + math.pi / 2.0)) / w, (1.0 - alpha) / alpha)
    s = part1 * part2

    s = np.where(np.isfinite(s) & (s > 0.0), s, np.nan)
    fill = np.nanmedian(s)
    if not math.isfinite(float(fill)) or float(fill) <= 0.0:
        fill = 1.0
    return np.where(np.isfinite(s) & (s > 0.0), s, float(fill)).astype(float)


def simulate_gumbel_copula(*, theta: float, dim: int, n_samples: int, seed: int | None = None) -> np.ndarray:
    """Use a Marshall-Olkin style algorithm for a d-dimensional Gumbel copula."""

    dependence = float(theta)
    dimension = int(dim)
    count = int(n_samples)
    if dependence < 1.0:
        raise ValueError(f"theta must be >=1 for Gumbel, got {theta}")
    if dimension <= 1:
        raise ValueError(f"dim must be >1, got {dim}")
    if count <= 0:
        raise ValueError(f"n_samples must be >0, got {n_samples}")

    rng = np.random.default_rng(seed)
    if dependence == 1.0:
        return _clip_unit_interval(rng.uniform(size=(count, dimension)))

    alpha = 1.0 / dependence
    s = _sample_positive_stable(alpha=alpha, n_samples=count, rng=rng)
    e = rng.exponential(scale=1.0, size=(count, dimension))
    return _clip_unit_interval(np.exp(-np.power(e / s[:, None], alpha)))


def tail_dependence_t(rho: float, nu: float) -> tuple[float, float]:
    """Return lower and upper tail dependence for a bivariate t-copula."""

    correlation = float(rho)
    degrees = float(nu)
    if degrees <= 0.0:
        raise ValueError(f"nu must be >0, got {nu}")
    if correlation >= 1.0:
        return 1.0, 1.0
    if correlation <= -1.0:
        return 0.0, 0.0

    arg = -math.sqrt(((degrees + 1.0) * (1.0 - correlation)) / (1.0 + correlation))
    lam = 2.0 * float(stats.t.cdf(arg, df=degrees + 1.0))
    lam = max(0.0, min(1.0, lam))
    return lam, lam


def tail_dependence_gaussian(rho: float) -> tuple[float, float]:
    """Return lower and upper tail dependence for a Gaussian copula."""

    if float(rho) >= 1.0:
        return 1.0, 1.0
    return 0.0, 0.0


def tail_dependence_clayton(theta: float) -> tuple[float, float]:
    """Return lower and upper tail dependence for a Clayton copula."""

    dependence = float(theta)
    if dependence <= 0.0:
        raise ValueError(f"theta must be >0, got {theta}")
    return 2.0 ** (-1.0 / dependence), 0.0


def tail_dependence_gumbel(theta: float) -> tuple[float, float]:
    """Return lower and upper tail dependence for a Gumbel copula."""

    dependence = float(theta)
    if dependence < 1.0:
        raise ValueError(f"theta must be >=1, got {theta}")
    return 0.0, 2.0 - 2.0 ** (1.0 / dependence)


def empirical_pairwise_tail_dependence(
    u: np.ndarray,
    *,
    col_names: Sequence[str] | None = None,
    tail_q: float = 0.95,
) -> pd.DataFrame:
    """Estimate pairwise lower and upper tail dependence at a fixed quantile."""

    arr = _clip_unit_interval(_as_2d_array(u))
    n_obs, dim = arr.shape
    q_hi = float(tail_q)
    if not (0.5 < q_hi < 1.0):
        raise ValueError(f"tail_q must be in (0.5,1), got {tail_q}")

    q_lo = 1.0 - q_hi
    denom = max(1e-12, 1.0 - q_hi)
    if col_names is None:
        names = [f"x{i}" for i in range(dim)]
    else:
        names = [str(name) for name in col_names]
        if len(names) != dim:
            raise ValueError(f"col_names length mismatch: expected {dim}, got {len(names)}")

    rows: list[dict[str, float | int | str]] = []
    for i in range(dim):
        for j in range(i + 1, dim):
            ui = arr[:, i]
            uj = arr[:, j]
            lower_joint = float(np.mean((ui <= q_lo) & (uj <= q_lo)))
            upper_joint = float(np.mean((ui >= q_hi) & (uj >= q_hi)))
            rows.append(
                {
                    "var_i": names[i],
                    "var_j": names[j],
                    "n": int(n_obs),
                    "tail_q": q_hi,
                    "lambda_lower_hat": lower_joint / denom,
                    "lambda_upper_hat": upper_joint / denom,
                    "joint_lower_prob": lower_joint,
                    "joint_upper_prob": upper_joint,
                }
            )
    return pd.DataFrame(rows)
