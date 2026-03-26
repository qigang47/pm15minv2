from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any

from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.layout_helpers import normalize_window_bound
from pm15min.research.layout import normalize_source_surface, slug_token, window_label


@dataclass(frozen=True)
class DateWindow:
    start: str
    end: str

    @classmethod
    def from_bounds(cls, start: str | date | datetime, end: str | date | datetime) -> "DateWindow":
        return cls(start=normalize_window_bound(start), end=normalize_window_bound(end))

    @property
    def label(self) -> str:
        return window_label(self.start, self.end)

    def to_dict(self) -> dict[str, str]:
        return {"start": self.start, "end": self.end, "label": self.label}


@dataclass(frozen=True)
class FeatureFrameSpec:
    feature_set: str
    source_surface: str = "backtest"

    def __post_init__(self) -> None:
        object.__setattr__(self, "feature_set", slug_token(self.feature_set))
        object.__setattr__(self, "source_surface", normalize_source_surface(self.source_surface))

    @property
    def object_type(self) -> str:
        return "feature_frame"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.feature_set}:{self.source_surface}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}


@dataclass(frozen=True)
class LabelFrameSpec:
    label_set: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "label_set", normalize_label_set(self.label_set))

    @property
    def object_type(self) -> str:
        return "label_frame"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.label_set}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}
