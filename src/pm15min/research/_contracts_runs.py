from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

from pm15min.research.layout import normalize_target, slug_token


_PARITY_BOOL_FIELDS = (
    "regime_enabled",
    "regime_apply_stake_scale",
    "regime_defense_force_with_pressure",
    "raw_depth_fak_refresh_enabled",
)
_PARITY_FLOAT_FIELDS = (
    "regime_caution_stake_multiplier",
    "regime_defense_stake_multiplier",
    "regime_caution_min_dir_prob_boost",
    "regime_defense_min_dir_prob_boost",
    "regime_caution_min_liquidity_ratio",
    "regime_defense_min_liquidity_ratio",
    "liquidity_min_spot_quote_volume_ratio",
    "liquidity_min_perp_quote_volume_ratio",
    "liquidity_min_spot_trades_ratio",
    "liquidity_min_perp_trades_ratio",
    "liquidity_min_spot_quote_volume_window",
    "liquidity_min_perp_quote_volume_window",
    "liquidity_min_spot_trades_window",
    "liquidity_min_perp_trades_window",
)
_PARITY_INT_FIELDS = (
    "regime_defense_max_trades_per_market",
    "regime_caution_soft_fail_count",
    "regime_defense_soft_fail_count",
    "liquidity_lookback_minutes",
    "liquidity_baseline_minutes",
    "liquidity_soft_fail_min_count",
)
_PARITY_INT_TUPLE_FIELDS = (
    "regime_caution_disable_offsets",
    "regime_defense_disable_offsets",
)
_PARITY_SLUG_FIELDS = ("liquidity_proxy_mode",)
_PARITY_FIELD_ALIASES = {
    "regime_controller_enabled": "regime_enabled",
    "regime_liquidity_proxy_mode": "liquidity_proxy_mode",
    "liquidity_guard_lookback_minutes": "liquidity_lookback_minutes",
    "liquidity_guard_baseline_minutes": "liquidity_baseline_minutes",
    "liquidity_guard_soft_fail_min_count": "liquidity_soft_fail_min_count",
}


@dataclass(frozen=True)
class BacktestParitySpec:
    regime_enabled: bool | None = None
    regime_apply_stake_scale: bool | None = None
    regime_defense_force_with_pressure: bool | None = None
    raw_depth_fak_refresh_enabled: bool | None = None
    regime_caution_stake_multiplier: float | None = None
    regime_defense_stake_multiplier: float | None = None
    regime_caution_min_dir_prob_boost: float | None = None
    regime_defense_min_dir_prob_boost: float | None = None
    regime_defense_max_trades_per_market: int | None = None
    regime_caution_min_liquidity_ratio: float | None = None
    regime_defense_min_liquidity_ratio: float | None = None
    regime_caution_soft_fail_count: int | None = None
    regime_defense_soft_fail_count: int | None = None
    regime_caution_disable_offsets: tuple[int, ...] | None = None
    regime_defense_disable_offsets: tuple[int, ...] | None = None
    liquidity_proxy_mode: str | None = None
    liquidity_lookback_minutes: int | None = None
    liquidity_baseline_minutes: int | None = None
    liquidity_soft_fail_min_count: int | None = None
    liquidity_min_spot_quote_volume_ratio: float | None = None
    liquidity_min_perp_quote_volume_ratio: float | None = None
    liquidity_min_spot_trades_ratio: float | None = None
    liquidity_min_perp_trades_ratio: float | None = None
    liquidity_min_spot_quote_volume_window: float | None = None
    liquidity_min_perp_quote_volume_window: float | None = None
    liquidity_min_spot_trades_window: float | None = None
    liquidity_min_perp_trades_window: float | None = None

    def __post_init__(self) -> None:
        for name in _PARITY_FLOAT_FIELDS:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, float(value))
        for name in _PARITY_INT_FIELDS:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, int(value))
        for name in _PARITY_INT_TUPLE_FIELDS:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _optional_int_tuple(value))
        for name in _PARITY_SLUG_FIELDS:
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, slug_token(value))

    @classmethod
    def field_names(cls) -> tuple[str, ...]:
        return tuple(cls.__dataclass_fields__.keys())

    @classmethod
    def field_aliases(cls) -> dict[str, str]:
        return dict(_PARITY_FIELD_ALIASES)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "BacktestParitySpec":
        if not isinstance(payload, Mapping):
            return cls()
        normalized = _normalize_parity_mapping(payload)
        kwargs: dict[str, Any] = {}
        for name in _PARITY_BOOL_FIELDS:
            kwargs[name] = _optional_bool(normalized.get(name))
        for name in _PARITY_FLOAT_FIELDS:
            kwargs[name] = _none_if_missing(normalized.get(name))
        for name in _PARITY_INT_FIELDS:
            kwargs[name] = _none_if_missing(normalized.get(name))
        for name in _PARITY_INT_TUPLE_FIELDS:
            kwargs[name] = _optional_int_tuple(normalized.get(name))
        for name in _PARITY_SLUG_FIELDS:
            kwargs[name] = _none_if_missing(normalized.get(name))
        return cls(**kwargs)

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value is not None}


@dataclass(frozen=True)
class ModelBundleSpec:
    profile: str
    target: str
    bundle_label: str
    offsets: tuple[int, ...]
    source_training_run: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile", slug_token(self.profile))
        object.__setattr__(self, "target", normalize_target(self.target))
        object.__setattr__(self, "bundle_label", slug_token(self.bundle_label, default="planned"))
        object.__setattr__(self, "offsets", tuple(int(offset) for offset in self.offsets))
        if self.source_training_run is not None:
            object.__setattr__(self, "source_training_run", slug_token(self.source_training_run))

    @property
    def object_type(self) -> str:
        return "model_bundle"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.profile}:{self.target}:{self.bundle_label}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}


@dataclass(frozen=True)
class BacktestRunSpec:
    profile: str
    spec_name: str
    run_label: str
    target: str = "direction"
    bundle_label: str | None = None
    secondary_target: str | None = None
    secondary_bundle_label: str | None = None
    fallback_reasons: tuple[str, ...] = ()
    variant_label: str = "default"
    variant_notes: str = ""
    stake_usd: float | None = None
    max_notional_usd: float | None = None
    parity: BacktestParitySpec = field(default_factory=BacktestParitySpec)

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile", slug_token(self.profile))
        object.__setattr__(self, "spec_name", slug_token(self.spec_name))
        object.__setattr__(self, "run_label", slug_token(self.run_label, default="planned"))
        object.__setattr__(self, "target", normalize_target(self.target))
        if self.bundle_label is not None:
            object.__setattr__(self, "bundle_label", slug_token(self.bundle_label))
        if self.secondary_target is not None:
            object.__setattr__(self, "secondary_target", normalize_target(self.secondary_target))
        if self.secondary_bundle_label is not None:
            object.__setattr__(self, "secondary_bundle_label", slug_token(self.secondary_bundle_label))
        object.__setattr__(self, "fallback_reasons", tuple(str(reason) for reason in self.fallback_reasons if str(reason)))
        object.__setattr__(self, "variant_label", slug_token(self.variant_label, default="default"))
        object.__setattr__(self, "variant_notes", str(self.variant_notes or ""))
        if self.stake_usd is not None:
            object.__setattr__(self, "stake_usd", float(self.stake_usd))
        if self.max_notional_usd is not None:
            object.__setattr__(self, "max_notional_usd", float(self.max_notional_usd))
        if not isinstance(self.parity, BacktestParitySpec):
            payload = self.parity if isinstance(self.parity, Mapping) else None
            object.__setattr__(self, "parity", BacktestParitySpec.from_mapping(payload))

    @property
    def object_type(self) -> str:
        return "backtest_run"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.profile}:{self.spec_name}:{self.run_label}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["parity"] = self.parity.to_dict()
        return payload | {"object_type": self.object_type, "object_id": self.object_id}


def _normalize_parity_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in payload.items():
        name = _PARITY_FIELD_ALIASES.get(str(key), str(key))
        if name in BacktestParitySpec.field_names():
            normalized[name] = value
    return normalized


def _none_if_missing(value: object) -> object | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value


def _optional_int_tuple(value: object) -> tuple[int, ...] | None:
    if value is None:
        return None
    items: list[object]
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
        if not items:
            return None
    else:
        try:
            items = list(value)
        except TypeError:
            items = [value]
    return tuple(dict.fromkeys(int(item) for item in items))


def _optional_bool(value: object) -> bool | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return bool(value)


@dataclass(frozen=True)
class ExperimentSuiteSpec:
    suite_name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "suite_name", slug_token(self.suite_name))

    @property
    def object_type(self) -> str:
        return "experiment_suite"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.suite_name}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}


@dataclass(frozen=True)
class ExperimentRunSpec:
    suite_name: str
    run_label: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "suite_name", slug_token(self.suite_name))
        object.__setattr__(self, "run_label", slug_token(self.run_label, default="planned"))

    @property
    def object_type(self) -> str:
        return "experiment_run"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.suite_name}:{self.run_label}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}


@dataclass(frozen=True)
class EvaluationRunSpec:
    category: str
    scope_label: str
    run_label: str
    backtest_spec: str = "baseline_truth"
    backtest_run_label: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "category", slug_token(self.category))
        object.__setattr__(self, "scope_label", slug_token(self.scope_label))
        object.__setattr__(self, "run_label", slug_token(self.run_label, default="planned"))
        object.__setattr__(self, "backtest_spec", slug_token(self.backtest_spec))
        if self.backtest_run_label is not None:
            object.__setattr__(self, "backtest_run_label", slug_token(self.backtest_run_label))

    @property
    def object_type(self) -> str:
        return "evaluation_run"

    @property
    def object_id(self) -> str:
        return f"{self.object_type}:{self.category}:{self.scope_label}:{self.run_label}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self) | {"object_type": self.object_type, "object_id": self.object_id}
