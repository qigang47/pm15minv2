from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import numpy as np


EventFn = Callable[[np.ndarray], np.ndarray]


@dataclass(frozen=True)
class EventSpec:
    """Configuration for event A in deep-OTM probability estimation."""

    kind: str = "terminal_cross"
    threshold: float = 0.0
    direction: str = "ge"
    lookback_steps: int = 5
    min_deficit: float = -1.0
    recovery_level: float = 0.0


def _ensure_paths(paths: np.ndarray) -> np.ndarray:
    arr = np.asarray(paths, dtype=float)
    if arr.ndim != 2:
        raise ValueError(f"paths must be 2D [n_paths, n_steps], got shape {arr.shape}")
    if arr.shape[0] == 0 or arr.shape[1] == 0:
        raise ValueError(f"paths must be non-empty, got shape {arr.shape}")
    return arr


def terminal_cross_event(paths: np.ndarray, *, threshold: float, direction: str = "ge") -> np.ndarray:
    """Event indicator on terminal value crossing a threshold."""

    arr = _ensure_paths(paths)
    terminal = arr[:, -1]
    if direction == "ge":
        return terminal >= float(threshold)
    if direction == "le":
        return terminal <= float(threshold)
    raise ValueError(f"direction must be 'ge' or 'le', got {direction}")


def last_n_comeback_event(
    paths: np.ndarray,
    *,
    lookback_steps: int,
    min_deficit: float,
    recovery_level: float,
) -> np.ndarray:
    """Event: path dips below deficit in last N steps, then recovers by terminal."""

    arr = _ensure_paths(paths)
    n_steps = arr.shape[1]
    n = int(lookback_steps)
    if n <= 0 or n > n_steps:
        raise ValueError(f"lookback_steps must be in [1,{n_steps}], got {n}")

    last = arr[:, -n:]
    dipped = np.min(last, axis=1) <= float(min_deficit)
    recovered = arr[:, -1] >= float(recovery_level)
    return dipped & recovered


def build_event_fn(spec: EventSpec) -> EventFn:
    """Return vectorized event evaluator f(paths)->bool[n_paths]."""

    if spec.kind == "terminal_cross":
        return lambda paths: terminal_cross_event(paths, threshold=spec.threshold, direction=spec.direction)

    if spec.kind == "last_n_comeback":
        return lambda paths: last_n_comeback_event(
            paths,
            lookback_steps=int(spec.lookback_steps),
            min_deficit=float(spec.min_deficit),
            recovery_level=float(spec.recovery_level),
        )

    raise ValueError(f"Unsupported event kind: {spec.kind}")
