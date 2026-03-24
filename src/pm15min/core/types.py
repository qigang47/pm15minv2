from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class ArtifactRun:
    name: str
    path: Path


@dataclass(frozen=True)
class RuntimeSummary:
    domain: str
    market: str
    values: dict[str, object] = field(default_factory=dict)

    def to_dict(self) -> dict[str, object]:
        return {"domain": self.domain, "market": self.market, **self.values}
