from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from pm15min.research._contracts_frames import DateWindow
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.layout import normalize_target, slug_token


@dataclass(frozen=True)
class TrainingSetSpec:
    feature_set: str
    label_set: str
    target: str
    window: DateWindow
    offset: int
    label_source: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "feature_set", slug_token(self.feature_set))
        object.__setattr__(self, "label_set", normalize_label_set(self.label_set))
        object.__setattr__(self, "target", normalize_target(self.target))
        object.__setattr__(self, "offset", int(self.offset))
        object.__setattr__(self, "label_source", slug_token(self.label_source or self.label_set))

    @property
    def object_type(self) -> str:
        return "training_set"

    @property
    def object_id(self) -> str:
        return (
            f"{self.object_type}:{self.feature_set}:{self.label_set}:{self.target}:"
            f"{self.window.label}:offset={self.offset}"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature_set": self.feature_set,
            "label_set": self.label_set,
            "target": self.target,
            "window": self.window.to_dict(),
            "offset": self.offset,
            "label_source": self.label_source,
            "object_type": self.object_type,
            "object_id": self.object_id,
        }


@dataclass(frozen=True)
class TrainingRunSpec:
    model_family: str
    feature_set: str
    label_set: str
    target: str
    window: DateWindow
    run_label: str
    offsets: tuple[int, ...]
    label_source: str | None = None
    parallel_workers: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_family", slug_token(self.model_family))
        object.__setattr__(self, "feature_set", slug_token(self.feature_set))
        object.__setattr__(self, "label_set", normalize_label_set(self.label_set))
        object.__setattr__(self, "target", normalize_target(self.target))
        object.__setattr__(self, "run_label", slug_token(self.run_label, default="planned"))
        object.__setattr__(self, "offsets", tuple(int(offset) for offset in self.offsets))
        object.__setattr__(self, "label_source", slug_token(self.label_source or self.label_set))
        if self.parallel_workers is not None:
            object.__setattr__(self, "parallel_workers", max(1, int(self.parallel_workers)))

    @property
    def object_type(self) -> str:
        return "training_run"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.model_family}:{self.target}:{self.window.label}:{self.run_label}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_family": self.model_family,
            "feature_set": self.feature_set,
            "label_set": self.label_set,
            "target": self.target,
            "window": self.window.to_dict(),
            "run_label": self.run_label,
            "offsets": list(self.offsets),
            "label_source": self.label_source,
            "parallel_workers": self.parallel_workers,
            "object_type": self.object_type,
            "object_id": self.object_id,
        }
