from __future__ import annotations

import math
from pathlib import Path
from typing import Iterable

import numpy as np


def bernoulli_mc_variance(*, p: float, n_samples: int) -> float:
    """Var(p_hat_N) = p(1-p)/N for Bernoulli Monte Carlo."""

    prob = float(p)
    n = int(n_samples)
    if not (0.0 <= prob <= 1.0):
        raise ValueError(f"p must be in [0,1], got {p}")
    if n <= 0:
        raise ValueError(f"n_samples must be positive, got {n_samples}")
    return prob * (1.0 - prob) / n


def bernoulli_mc_stderr(*, p: float, n_samples: int) -> float:
    """Standard error sqrt(p(1-p)/N) for Bernoulli Monte Carlo."""

    return math.sqrt(bernoulli_mc_variance(p=p, n_samples=n_samples))


def worst_case_bernoulli_variance() -> float:
    """max_p p(1-p) attained at p=0.5."""

    return 0.25


def required_mc_samples_for_margin(*, epsilon: float, p: float = 0.5, z_value: float = 1.96) -> int:
    """N needed for Wald half-width epsilon at confidence z_value."""

    eps = float(epsilon)
    prob = float(p)
    if eps <= 0.0:
        raise ValueError(f"epsilon must be positive, got {epsilon}")
    if not (0.0 <= prob <= 1.0):
        raise ValueError(f"p must be in [0,1], got {p}")
    if float(z_value) <= 0.0:
        raise ValueError(f"z_value must be positive, got {z_value}")
    n = (float(z_value) ** 2) * prob * (1.0 - prob) / (eps * eps)
    return int(math.ceil(n))


def simulate_running_p_hat(p: float, N_max: int, seed: int) -> tuple[np.ndarray, np.ndarray]:
    """Simulate Bernoulli(p) and compute running estimate p_hat_n."""

    p = float(p)
    N_max = int(N_max)
    seed = int(seed)
    if not (0.0 <= p <= 1.0):
        raise ValueError(f"p must be in [0,1], got {p}")
    if N_max <= 0:
        raise ValueError(f"N_max must be positive, got {N_max}")

    rng = np.random.default_rng(seed)
    xs = (rng.random(N_max) < p).astype(np.float64)
    cs = np.cumsum(xs)
    ns = np.arange(1, N_max + 1, dtype=np.int64)
    p_hats = cs / ns
    return ns, p_hats


def _parse_ns_logspace(ns_logspace: int | tuple[int, int] | tuple[int, int, int] | Iterable[int] | None) -> np.ndarray:
    if ns_logspace is None:
        return np.unique(np.logspace(1, 5, num=200).astype(int))

    if isinstance(ns_logspace, (int, np.integer)):
        nmax = int(ns_logspace)
        if nmax <= 0:
            raise ValueError("Ns_logspace as int must be positive")
        return np.arange(1, nmax + 1, dtype=np.int64)

    if isinstance(ns_logspace, (tuple, list)) and len(ns_logspace) in (2, 3):
        n_min = int(ns_logspace[0])
        n_max = int(ns_logspace[1])
        num = int(ns_logspace[2]) if len(ns_logspace) == 3 else 200
        if n_min <= 0 or n_max <= 0 or n_max < n_min:
            raise ValueError(f"Invalid Ns_logspace range: {ns_logspace}")
        start = math.log10(n_min)
        end = math.log10(n_max)
        return np.unique(np.logspace(start, end, num=num).astype(int))

    try:
        arr = np.array(list(ns_logspace), dtype=np.int64)
    except Exception as exc:
        raise ValueError(f"Unsupported Ns_logspace={ns_logspace!r}") from exc
    if arr.size == 0:
        raise ValueError("Ns_logspace produced empty Ns")
    if (arr <= 0).any():
        raise ValueError("Ns must be positive integers")
    return np.unique(arr)


def plot_mc_convergence(
    *,
    ps: Iterable[float] = (0.5, 0.8, 0.95, 0.99),
    Ns_logspace: int | tuple[int, int] | tuple[int, int, int] | Iterable[int] | None = (10, 100_000),
    seed: int = 42,
    outpath: str | Path = "reports/mc_convergence.png",
) -> Path:
    """Plot MC convergence curves and save to a png."""

    outpath = Path(outpath)
    outpath.parent.mkdir(parents=True, exist_ok=True)
    cache_root = outpath.parent / ".cache"
    cache_root.mkdir(parents=True, exist_ok=True)

    import os

    os.environ.setdefault("XDG_CACHE_HOME", str(cache_root))
    os.environ.setdefault("MPLCONFIGDIR", str(cache_root / "mpl"))

    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ns = _parse_ns_logspace(Ns_logspace)
    n_max = int(ns.max())

    fig, ax = plt.subplots(figsize=(10, 6))
    for idx, prob in enumerate(ps):
        ns_full, p_hats_full = simulate_running_p_hat(float(prob), N_max=n_max, seed=seed + idx)
        p_hats = p_hats_full[ns - 1]
        ax.plot(ns, p_hats, linewidth=2.0, label=f"p={float(prob):.2f}")

    ax.set_xscale("log")
    ax.set_xlabel("Number of Samples (log scale)")
    ax.set_ylabel("Estimated Probability")
    ax.set_title("MC Convergence: Harder Near p=0.50")
    ax.set_ylim(-0.02, 1.02)
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    ax.legend(loc="best")

    fig.tight_layout()
    fig.savefig(outpath, dpi=200)
    plt.close(fig)
    return outpath
