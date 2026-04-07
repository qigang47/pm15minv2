from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from pm15min.research._contracts_frames import DateWindow
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.layout import normalize_target, slug_token


def _coerce_bool(raw: Any, *, field_name: str) -> bool:
    if isinstance(raw, bool):
        return raw
    token = str(raw or "").strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Unsupported boolean value for {field_name}: {raw!r}")


def normalize_offset_weight_overrides(raw: Any) -> dict[int, dict[str, Any]]:
    if raw is None or raw == "" or raw == ():
        return {}
    payload = raw
    if isinstance(payload, str):
        token = payload.strip()
        if not token:
            return {}
        payload = json.loads(token)
    if not isinstance(payload, Mapping):
        raise TypeError(f"offset_weight_overrides must be a mapping, got: {payload!r}")
    normalized: dict[int, dict[str, Any]] = {}
    for raw_offset, raw_override in payload.items():
        offset = int(raw_offset)
        if not isinstance(raw_override, Mapping):
            raise TypeError(f"offset_weight_overrides[{raw_offset!r}] must be a mapping, got: {raw_override!r}")
        item: dict[str, Any] = {}
        if raw_override.get("balance_classes") is not None:
            item["balance_classes"] = _coerce_bool(
                raw_override.get("balance_classes"),
                field_name=f"offset_weight_overrides[{raw_offset!r}].balance_classes",
            )
        if raw_override.get("weight_by_vol") is not None:
            item["weight_by_vol"] = _coerce_bool(
                raw_override.get("weight_by_vol"),
                field_name=f"offset_weight_overrides[{raw_offset!r}].weight_by_vol",
            )
        if raw_override.get("inverse_vol") is not None:
            item["inverse_vol"] = _coerce_bool(
                raw_override.get("inverse_vol"),
                field_name=f"offset_weight_overrides[{raw_offset!r}].inverse_vol",
            )
        if raw_override.get("contrarian_weight") is not None:
            item["contrarian_weight"] = float(raw_override.get("contrarian_weight"))
        if raw_override.get("contrarian_quantile") is not None:
            item["contrarian_quantile"] = float(raw_override.get("contrarian_quantile"))
        if raw_override.get("contrarian_return_col") is not None:
            token = str(raw_override.get("contrarian_return_col") or "").strip()
            if token:
                item["contrarian_return_col"] = token
        if item:
            normalized[offset] = item
    return {int(offset): dict(normalized[offset]) for offset in sorted(normalized)}


def offset_weight_overrides_payload(raw: Any) -> dict[str, dict[str, Any]]:
    normalized = normalize_offset_weight_overrides(raw)
    return {str(int(offset)): dict(overrides) for offset, overrides in normalized.items()}


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
    weight_variant_label: str = "default"
    balance_classes: bool | None = None
    weight_by_vol: bool | None = None
    inverse_vol: bool | None = None
    contrarian_weight: float | None = None
    contrarian_quantile: float | None = None
    contrarian_return_col: str | None = None
    offset_weight_overrides: dict[int, dict[str, Any]] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "model_family", slug_token(self.model_family))
        object.__setattr__(self, "feature_set", slug_token(self.feature_set))
        object.__setattr__(self, "label_set", normalize_label_set(self.label_set))
        object.__setattr__(self, "target", normalize_target(self.target))
        object.__setattr__(self, "run_label", slug_token(self.run_label, default="planned"))
        object.__setattr__(self, "offsets", tuple(int(offset) for offset in self.offsets))
        object.__setattr__(self, "label_source", slug_token(self.label_source or self.label_set))
        object.__setattr__(self, "weight_variant_label", slug_token(self.weight_variant_label, default="default"))
        if self.parallel_workers is not None:
            object.__setattr__(self, "parallel_workers", max(1, int(self.parallel_workers)))
        if self.balance_classes is not None:
            object.__setattr__(
                self,
                "balance_classes",
                _coerce_bool(self.balance_classes, field_name="balance_classes"),
            )
        if self.weight_by_vol is not None:
            object.__setattr__(
                self,
                "weight_by_vol",
                _coerce_bool(self.weight_by_vol, field_name="weight_by_vol"),
            )
        if self.inverse_vol is not None:
            object.__setattr__(
                self,
                "inverse_vol",
                _coerce_bool(self.inverse_vol, field_name="inverse_vol"),
            )
        if self.contrarian_weight is not None:
            object.__setattr__(self, "contrarian_weight", float(self.contrarian_weight))
        if self.contrarian_quantile is not None:
            object.__setattr__(self, "contrarian_quantile", float(self.contrarian_quantile))
        if self.contrarian_return_col is not None:
            object.__setattr__(self, "contrarian_return_col", str(self.contrarian_return_col).strip() or None)
        object.__setattr__(self, "offset_weight_overrides", normalize_offset_weight_overrides(self.offset_weight_overrides))

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
            "weight_variant_label": self.weight_variant_label,
            "balance_classes": self.balance_classes,
            "weight_by_vol": self.weight_by_vol,
            "inverse_vol": self.inverse_vol,
            "contrarian_weight": self.contrarian_weight,
            "contrarian_quantile": self.contrarian_quantile,
            "contrarian_return_col": self.contrarian_return_col,
            "object_type": self.object_type,
            "object_id": self.object_id,
            **(
                {"offset_weight_overrides": offset_weight_overrides_payload(self.offset_weight_overrides)}
                if self.offset_weight_overrides
                else {}
            ),
        }
