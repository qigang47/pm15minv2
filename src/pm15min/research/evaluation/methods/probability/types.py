from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ProbabilityEstimate:
    """Container for probability estimation outputs."""

    method: str
    p_hat: float
    stderr: float
    n_paths: int
    hit_rate: float
    ess: float | None = None
    gamma: float | None = None
    diagnostics: dict[str, float] = field(default_factory=dict)

    def ci95(self) -> tuple[float, float]:
        """95% Wald confidence interval clipped to [0,1]."""

        half = 1.96 * float(self.stderr)
        lo = max(0.0, float(self.p_hat) - half)
        hi = min(1.0, float(self.p_hat) + half)
        return (lo, hi)

    def as_dict(self) -> dict[str, float | int | str | None]:
        lo, hi = self.ci95()
        return {
            "method": self.method,
            "p_hat": float(self.p_hat),
            "stderr": float(self.stderr),
            "ci95_lo": float(lo),
            "ci95_hi": float(hi),
            "n_paths": int(self.n_paths),
            "hit_rate": float(self.hit_rate),
            "ess": None if self.ess is None else float(self.ess),
            "gamma": None if self.gamma is None else float(self.gamma),
            **{key: float(value) for key, value in self.diagnostics.items()},
        }
