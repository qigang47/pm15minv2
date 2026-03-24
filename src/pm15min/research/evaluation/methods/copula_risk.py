from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from pm15min.research.evaluation.methods.copulas import (
    empirical_pairwise_tail_dependence,
    estimate_average_kendall_tau,
    fit_clayton_theta_from_tau,
    fit_gaussian_copula,
    fit_gumbel_theta_from_tau,
    fit_t_copula,
    pseudo_observations,
    simulate_clayton_copula,
    simulate_gaussian_copula,
    simulate_gumbel_copula,
    simulate_t_copula,
    tail_dependence_clayton,
    tail_dependence_gaussian,
    tail_dependence_gumbel,
    tail_dependence_t,
)


@dataclass(frozen=True)
class CopulaRiskConfig:
    family: str = "t"
    tail: str = "lower"
    n_sim: int = 200_000
    quantile: float = 0.05
    alpha: float = 0.99
    tail_q: float = 0.95
    nu: float | None = None
    nu_grid: tuple[float, ...] = (2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 20.0, 30.0)
    theta: float | None = None
    seed: int | None = 42


@dataclass(frozen=True)
class CopulaRiskResult:
    summary: dict[str, float | int | str]
    pairwise_tail: pd.DataFrame
    fit_params: dict[str, float | int | str]
    event_thresholds: dict[str, float]
    event_probs: dict[str, float]


def _as_2d_array(x: np.ndarray | pd.DataFrame | Sequence[Sequence[float]]) -> np.ndarray:
    if isinstance(x, pd.DataFrame):
        arr = x.to_numpy(dtype=float)
    else:
        arr = np.asarray(x, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D input, got shape {arr.shape}")
    if arr.shape[0] == 0 or arr.shape[1] == 0:
        raise ValueError(f"Empty input not allowed, got shape {arr.shape}")
    return arr


def _value_at_risk(losses: np.ndarray, alpha: float) -> float:
    level = float(alpha)
    if not (0.0 < level < 1.0):
        raise ValueError(f"alpha must be in (0,1), got {alpha}")
    return float(np.quantile(np.asarray(losses, dtype=float), level, method="higher"))


def _conditional_var(losses: np.ndarray, alpha: float) -> float:
    arr = np.asarray(losses, dtype=float)
    var = _value_at_risk(arr, alpha)
    tail_losses = arr[arr >= var]
    if tail_losses.size == 0:
        return float(var)
    return float(np.mean(tail_losses))


def _infer_event_probs_from_quantile(
    data: np.ndarray,
    *,
    quantile: float,
    tail: str,
    col_names: Sequence[str],
) -> tuple[np.ndarray, np.ndarray, dict[str, float], dict[str, float]]:
    q = float(quantile)
    if not (0.0 < q < 0.5):
        raise ValueError(f"quantile must be in (0,0.5), got {q}")

    if tail == "lower":
        thresholds = np.quantile(data, q, axis=0)
        probs = np.mean(data <= thresholds, axis=0)
    elif tail == "upper":
        thresholds = np.quantile(data, 1.0 - q, axis=0)
        probs = np.mean(data >= thresholds, axis=0)
    else:
        raise ValueError(f"tail must be 'lower' or 'upper', got {tail}")

    threshold_map = {str(name): float(value) for name, value in zip(col_names, thresholds)}
    probs_map = {str(name): float(value) for name, value in zip(col_names, probs)}
    return thresholds, probs, threshold_map, probs_map


def _simulate_uniforms(
    *,
    family: str,
    dim: int,
    n_sim: int,
    seed: int | None,
    corr: np.ndarray | None = None,
    nu: float | None = None,
    theta: float | None = None,
) -> np.ndarray:
    if family == "gaussian":
        if corr is None:
            raise ValueError("corr is required for gaussian copula simulation")
        return simulate_gaussian_copula(corr, n_samples=int(n_sim), seed=seed)
    if family == "t":
        if corr is None:
            raise ValueError("corr is required for t-copula simulation")
        if nu is None:
            raise ValueError("nu is required for t-copula simulation")
        return simulate_t_copula(corr, nu=float(nu), n_samples=int(n_sim), seed=seed)
    if family == "clayton":
        if theta is None:
            raise ValueError("theta is required for Clayton copula simulation")
        return simulate_clayton_copula(theta=float(theta), dim=int(dim), n_samples=int(n_sim), seed=seed)
    if family == "gumbel":
        if theta is None:
            raise ValueError("theta is required for Gumbel copula simulation")
        return simulate_gumbel_copula(theta=float(theta), dim=int(dim), n_samples=int(n_sim), seed=seed)
    raise ValueError(f"Unsupported family: {family}")


def _build_model_tail_reference(
    *,
    family: str,
    corr: np.ndarray | None,
    nu: float | None,
    theta: float | None,
    col_names: Sequence[str],
) -> pd.DataFrame:
    dim = len(col_names)
    rows: list[dict[str, float | str]] = []
    for i in range(dim):
        for j in range(i + 1, dim):
            lambda_lower = math.nan
            lambda_upper = math.nan
            if family == "gaussian" and corr is not None:
                lambda_lower, lambda_upper = tail_dependence_gaussian(float(corr[i, j]))
            elif family == "t" and corr is not None and nu is not None:
                lambda_lower, lambda_upper = tail_dependence_t(float(corr[i, j]), float(nu))
            elif family == "clayton" and theta is not None:
                lambda_lower, lambda_upper = tail_dependence_clayton(float(theta))
            elif family == "gumbel" and theta is not None:
                lambda_lower, lambda_upper = tail_dependence_gumbel(float(theta))

            rows.append(
                {
                    "var_i": str(col_names[i]),
                    "var_j": str(col_names[j]),
                    "lambda_lower_model": lambda_lower,
                    "lambda_upper_model": lambda_upper,
                }
            )
    return pd.DataFrame(rows)


def _joint_bad_matrix(u: np.ndarray, probs: np.ndarray, *, tail: str) -> np.ndarray:
    if tail == "lower":
        return u <= probs[None, :]
    if tail == "upper":
        return u >= (1.0 - probs[None, :])
    raise ValueError(f"tail must be lower/upper, got {tail}")


def _summarize_joint_risk(
    bad: np.ndarray,
    *,
    event_probs: np.ndarray,
    losses: np.ndarray,
    alpha: float,
    family: str,
    n_sim: int,
    tail: str,
) -> dict[str, float | int | str]:
    n_obs, dim = bad.shape
    bad_float = bad.astype(float)
    loss = bad_float @ losses

    all_bad = float(np.mean(np.all(bad, axis=1)))
    all_good = float(np.mean(np.all(~bad, axis=1)))
    any_bad = float(np.mean(np.any(bad, axis=1)))
    at_least_2 = float(np.mean(np.sum(bad, axis=1) >= min(2, dim)))
    at_least_3 = float(np.mean(np.sum(bad, axis=1) >= min(3, dim)))
    all_bad_indep = float(np.prod(event_probs))
    all_good_indep = float(np.prod(1.0 - event_probs))

    var_alpha = _value_at_risk(loss, alpha)
    cvar_alpha = _conditional_var(loss, alpha)

    return {
        "family": str(family),
        "tail": str(tail),
        "n_sim": int(n_sim),
        "n_vars": int(dim),
        "all_bad_prob": all_bad,
        "all_good_prob": all_good,
        "all_bad_prob_indep": all_bad_indep,
        "all_good_prob_indep": all_good_indep,
        "all_bad_minus_indep": float(all_bad - all_bad_indep),
        "all_bad_vs_indep_ratio": float(all_bad / max(1e-12, all_bad_indep)),
        "any_bad_prob": any_bad,
        "at_least_2_bad_prob": at_least_2,
        "at_least_3_bad_prob": at_least_3,
        "expected_loss": float(np.mean(loss)),
        "loss_std": float(np.std(loss, ddof=1)) if n_obs > 1 else 0.0,
        "var_alpha": float(alpha),
        "VaR": float(var_alpha),
        "CVaR": float(cvar_alpha),
    }


def run_copula_tail_risk(
    *,
    data: np.ndarray | pd.DataFrame,
    col_names: Sequence[str],
    config: CopulaRiskConfig,
    event_probs: Iterable[float] | None = None,
    losses: Iterable[float] | None = None,
) -> CopulaRiskResult:
    """Fit a copula, simulate joint outcomes, and summarize tail risk."""

    arr = _as_2d_array(data)
    n_obs, dim = arr.shape
    names = [str(name) for name in col_names]
    if len(names) != dim:
        raise ValueError(f"col_names length mismatch: expected {dim}, got {len(names)}")

    family = str(config.family).strip().lower()
    if family not in {"gaussian", "t", "clayton", "gumbel"}:
        raise ValueError(f"Unsupported family: {config.family}")

    historical_u = pseudo_observations(arr)
    corr = None
    nu = None
    theta = None
    fit_params: dict[str, float | int | str] = {"family": family, "n_obs": int(n_obs), "n_vars": int(dim)}

    if family == "gaussian":
        fit = fit_gaussian_copula(historical_u)
        corr = fit.corr
        fit_params["avg_abs_corr"] = float(np.mean(np.abs(corr[np.triu_indices(dim, 1)]))) if dim > 1 else 0.0
    elif family == "t":
        fit = fit_t_copula(historical_u, nu=config.nu, nu_grid=config.nu_grid)
        corr = fit.corr
        nu = float(fit.nu)
        fit_params["nu"] = nu
        fit_params["loglik"] = float(fit.loglik)
        fit_params["avg_abs_corr"] = float(np.mean(np.abs(corr[np.triu_indices(dim, 1)]))) if dim > 1 else 0.0
    elif family == "clayton":
        if config.theta is None:
            arch = fit_clayton_theta_from_tau(estimate_average_kendall_tau(historical_u))
            theta = float(arch.theta)
            fit_params["avg_kendall_tau"] = float(arch.avg_kendall_tau)
        else:
            theta = float(config.theta)
        fit_params["theta"] = theta
    else:
        if config.theta is None:
            arch = fit_gumbel_theta_from_tau(estimate_average_kendall_tau(historical_u))
            theta = float(arch.theta)
            fit_params["avg_kendall_tau"] = float(arch.avg_kendall_tau)
        else:
            theta = float(config.theta)
        fit_params["theta"] = theta

    if event_probs is None:
        _, probs, threshold_map, probs_map = _infer_event_probs_from_quantile(
            arr,
            quantile=float(config.quantile),
            tail=str(config.tail),
            col_names=names,
        )
    else:
        probs = np.asarray(list(event_probs), dtype=float)
        if probs.shape != (dim,):
            raise ValueError(f"event_probs shape mismatch: expected {(dim,)}, got {probs.shape}")
        if np.any(probs <= 0.0) or np.any(probs >= 1.0):
            raise ValueError("event_probs must be in (0,1)")
        threshold_map = {name: math.nan for name in names}
        probs_map = {name: float(value) for name, value in zip(names, probs)}

    if losses is None:
        loss_vec = np.ones(dim, dtype=float)
    else:
        loss_vec = np.asarray(list(losses), dtype=float)
        if loss_vec.shape != (dim,):
            raise ValueError(f"losses shape mismatch: expected {(dim,)}, got {loss_vec.shape}")

    simulated_u = _simulate_uniforms(
        family=family,
        dim=dim,
        n_sim=int(config.n_sim),
        seed=config.seed,
        corr=corr,
        nu=nu,
        theta=theta,
    )
    bad = _joint_bad_matrix(simulated_u, probs=probs, tail=str(config.tail))
    summary = _summarize_joint_risk(
        bad,
        event_probs=probs,
        losses=loss_vec,
        alpha=float(config.alpha),
        family=family,
        n_sim=int(config.n_sim),
        tail=str(config.tail),
    )

    tail_hist = empirical_pairwise_tail_dependence(historical_u, col_names=names, tail_q=float(config.tail_q)).rename(
        columns={
            "lambda_lower_hat": "lambda_lower_hist",
            "lambda_upper_hat": "lambda_upper_hist",
            "joint_lower_prob": "joint_lower_prob_hist",
            "joint_upper_prob": "joint_upper_prob_hist",
        }
    )
    tail_sim = empirical_pairwise_tail_dependence(simulated_u, col_names=names, tail_q=float(config.tail_q)).rename(
        columns={
            "lambda_lower_hat": "lambda_lower_sim",
            "lambda_upper_hat": "lambda_upper_sim",
            "joint_lower_prob": "joint_lower_prob_sim",
            "joint_upper_prob": "joint_upper_prob_sim",
        }
    )
    tail_model = _build_model_tail_reference(
        family=family,
        corr=corr,
        nu=nu,
        theta=theta,
        col_names=names,
    )

    pairwise = tail_hist.merge(
        tail_sim[
            [
                "var_i",
                "var_j",
                "lambda_lower_sim",
                "lambda_upper_sim",
                "joint_lower_prob_sim",
                "joint_upper_prob_sim",
            ]
        ],
        on=["var_i", "var_j"],
        how="left",
    ).merge(
        tail_model,
        on=["var_i", "var_j"],
        how="left",
    )

    summary["mean_event_prob"] = float(np.mean(probs))
    summary["max_event_prob"] = float(np.max(probs))
    summary["min_event_prob"] = float(np.min(probs))

    if dim > 1 and family in {"gaussian", "t"} and corr is not None:
        summary["mean_offdiag_corr"] = float(np.mean(corr[np.triu_indices(dim, 1)]))
    if family == "t" and nu is not None:
        summary["nu"] = float(nu)
    if family in {"clayton", "gumbel"} and theta is not None:
        summary["theta"] = float(theta)

    return CopulaRiskResult(
        summary=summary,
        pairwise_tail=pairwise,
        fit_params=fit_params,
        event_thresholds=threshold_map,
        event_probs=probs_map,
    )
