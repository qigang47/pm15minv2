from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from pm15min.research.contracts import DateWindow
from pm15min.research._contracts_runs import BacktestParitySpec
from pm15min.research.layout import slug_token


@dataclass(frozen=True)
class ExperimentComparePolicy:
    reference_variant_labels: tuple[str, ...] = ("default", "baseline", "control")

    def __post_init__(self) -> None:
        object.__setattr__(self, "reference_variant_labels", _normalize_compare_reference_labels(self.reference_variant_labels))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "ExperimentComparePolicy":
        if not isinstance(payload, Mapping):
            return cls()
        labels = payload.get("reference_variant_labels")
        if _is_missing_value(labels):
            labels = payload.get("baseline_variant_labels")
        if _is_missing_value(labels):
            labels = payload.get("compare_reference_variant_labels")
        return cls(reference_variant_labels=_parse_string_seq(labels) if not _is_missing_value(labels) else cls().reference_variant_labels)

    def to_dict(self) -> dict[str, Any]:
        return {"reference_variant_labels": list(self.reference_variant_labels)}


@dataclass(frozen=True)
class ExperimentRuntimePolicy:
    completed_cases: str = "resume"
    failed_cases: str = "rerun"
    parallel_case_workers: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "completed_cases",
            _normalize_runtime_policy_choice(
                self.completed_cases,
                default="resume",
                allowed=("resume", "rerun"),
                field_name="completed_cases",
            ),
        )
        object.__setattr__(
            self,
            "failed_cases",
            _normalize_runtime_policy_choice(
                self.failed_cases,
                default="rerun",
                allowed=("rerun", "skip"),
                field_name="failed_cases",
            ),
        )
        object.__setattr__(self, "parallel_case_workers", max(1, int(self.parallel_case_workers)))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "ExperimentRuntimePolicy":
        if not isinstance(payload, Mapping):
            return cls()

        completed_cases = _first_present_value(
            payload,
            "completed_cases",
            "completed_case_policy",
            "existing_completed_cases",
        )
        if _is_missing_value(completed_cases):
            resume_existing = _first_present_value(payload, "resume_existing_completed", "resume")
            if not _is_missing_value(resume_existing):
                completed_cases = "resume" if _coerce_bool(resume_existing) else "rerun"

        failed_cases = _first_present_value(
            payload,
            "failed_cases",
            "failed_case_policy",
            "existing_failed_cases",
        )
        if _is_missing_value(failed_cases):
            rerun_failed = _first_present_value(payload, "rerun_failed_cases")
            if not _is_missing_value(rerun_failed):
                failed_cases = "rerun" if _coerce_bool(rerun_failed) else "skip"
        parallel_case_workers = _first_present_value(
            payload,
            "parallel_case_workers",
            "parallel_workers",
            "max_parallel_cases",
        )

        defaults = cls()
        return cls(
            completed_cases=defaults.completed_cases if _is_missing_value(completed_cases) else str(completed_cases),
            failed_cases=defaults.failed_cases if _is_missing_value(failed_cases) else str(failed_cases),
            parallel_case_workers=defaults.parallel_case_workers
            if _is_missing_value(parallel_case_workers)
            else int(parallel_case_workers),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "completed_cases": self.completed_cases,
            "failed_cases": self.failed_cases,
            "parallel_case_workers": int(self.parallel_case_workers),
        }


@dataclass(frozen=True)
class ExperimentMarketSpec:
    market: str
    profile: str
    model_family: str
    feature_set: str
    label_set: str
    target: str
    offsets: tuple[int, ...]
    window: DateWindow
    backtest_spec: str = "baseline_truth"
    variant_label: str = "default"
    variant_notes: str = ""
    stake_usd: float | None = None
    max_notional_usd: float | None = None
    matrix_parent_run_name: str = ""
    matrix_stake_label: str = ""
    hybrid_secondary_target: str | None = None
    hybrid_secondary_offsets: tuple[int, ...] | None = None
    hybrid_fallback_reasons: tuple[str, ...] = ()
    parity: BacktestParitySpec = field(default_factory=BacktestParitySpec)
    group_name: str = ""
    run_name: str = ""
    tags: tuple[str, ...] = ()
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["window"] = self.window.to_dict()
        payload["offsets"] = list(self.offsets)
        if self.hybrid_secondary_offsets is not None:
            payload["hybrid_secondary_offsets"] = list(self.hybrid_secondary_offsets)
        payload["hybrid_fallback_reasons"] = list(self.hybrid_fallback_reasons)
        payload["parity"] = self.parity.to_dict()
        payload["tags"] = list(self.tags)
        if self.stake_usd is None:
            payload.pop("stake_usd", None)
        if self.max_notional_usd is None:
            payload.pop("max_notional_usd", None)
        if not self.matrix_parent_run_name:
            payload.pop("matrix_parent_run_name", None)
        if not self.matrix_stake_label:
            payload.pop("matrix_stake_label", None)
        return payload


@dataclass(frozen=True)
class ExperimentSuiteDefinition:
    suite_name: str
    cycle: str
    markets: tuple[ExperimentMarketSpec, ...]
    compare_policy: ExperimentComparePolicy = field(default_factory=ExperimentComparePolicy)
    runtime_policy: ExperimentRuntimePolicy = field(default_factory=ExperimentRuntimePolicy)

    def to_dict(self) -> dict[str, Any]:
        return {
            "suite_name": self.suite_name,
            "cycle": self.cycle,
            "markets": [market.to_dict() for market in self.markets],
            "compare_policy": self.compare_policy.to_dict(),
            "runtime_policy": self.runtime_policy.to_dict(),
        }


def load_suite_definition(path: Path) -> ExperimentSuiteDefinition:
    payload = json.loads(path.read_text(encoding="utf-8"))
    cycle = str(payload.get("cycle") or "15m")
    suite_name = str(payload.get("suite_name") or path.stem)
    defaults = {
        "suite_name": suite_name,
        "profile": str(payload.get("profile") or "deep_otm"),
        "model_family": str(payload.get("model_family") or "deep_otm"),
        "feature_set": str(payload.get("feature_set") or "deep_otm_v1"),
        "label_set": str(payload.get("label_set") or "truth"),
        "target": str(payload.get("target") or "direction"),
        "offsets": tuple(int(v) for v in (payload.get("offsets") or [7, 8, 9])),
        "window": _load_window(payload.get("window") or payload),
        "backtest_spec": str(payload.get("backtest_spec") or "baseline_truth"),
        "variant_label": str(payload.get("variant_label") or "default"),
        "variant_notes": str(payload.get("variant_notes") or ""),
        "stakes_usd": _parse_float_seq(_first_present_value(payload, "stakes_usd", "stakes")),
        "max_notional_usd": _coerce_optional_float(payload.get("max_notional_usd")),
        "matrix_parent_run_name": "",
        "matrix_stake_label": "",
        "hybrid_secondary_target": payload.get("hybrid_secondary_target"),
        "hybrid_secondary_offsets": payload.get("hybrid_secondary_offsets"),
        "hybrid_fallback_reasons": tuple(str(item) for item in (payload.get("hybrid_fallback_reasons") or [])),
        "parity": _load_parity_spec(payload, base=None),
        "group_name": "",
        "run_name": "",
        "tags": _parse_string_seq(payload.get("tags")),
        "notes": str(payload.get("notes") or ""),
        "backtest_variants": _parse_backtest_variants(payload.get("backtest_variants")),
        "feature_set_variants": _parse_feature_set_variants(
            _first_present_value(payload, "feature_set_variants", "factor_variants", "feature_variants")
        ),
    }

    raw_markets = payload.get("markets")
    if not raw_markets:
        raise ValueError(f"Suite spec {path} must define markets")

    markets = _expand_market_specs(raw_markets=raw_markets, defaults=defaults)
    return ExperimentSuiteDefinition(
        suite_name=suite_name,
        cycle=cycle,
        markets=tuple(markets),
        compare_policy=_load_compare_policy(payload),
        runtime_policy=_load_runtime_policy(payload),
    )


def _load_window(payload: dict[str, Any]) -> DateWindow:
    start = payload.get("start") or payload.get("window_start")
    end = payload.get("end") or payload.get("window_end")
    if start in {None, ""} or end in {None, ""}:
        raise ValueError("Experiment suite requires window.start/end or window_start/window_end")
    return DateWindow.from_bounds(str(start), str(end))


def _coerce_optional_offsets(raw: Any) -> tuple[int, ...] | None:
    if _is_missing_value(raw):
        return None
    return tuple(int(value) for value in raw)


def _coerce_optional_float(raw: Any) -> float | None:
    if _is_missing_value(raw):
        return None
    return float(raw)


def _parse_float_seq(raw: Any) -> tuple[float, ...]:
    if _is_missing_value(raw):
        return ()
    values: list[Any]
    if isinstance(raw, str):
        values = [token.strip() for token in raw.split(",") if token.strip()]
    elif isinstance(raw, (int, float)):
        values = [raw]
    elif isinstance(raw, Mapping):
        raise TypeError(f"Unsupported float sequence mapping: {raw!r}")
    else:
        values = list(raw)
    out: list[float] = []
    for value in values:
        numeric = float(value)
        if numeric not in out:
            out.append(numeric)
    return tuple(out)


def _parse_string_seq(raw: Any) -> tuple[str, ...]:
    if _is_missing_value(raw):
        return ()
    if isinstance(raw, str):
        return tuple(token.strip() for token in raw.split(",") if token.strip())
    return tuple(str(value).strip() for value in raw if str(value).strip())


def _is_missing_value(raw: Any) -> bool:
    return raw is None or raw == ""


def _load_parity_spec(payload: dict[str, Any], *, base: BacktestParitySpec | None) -> BacktestParitySpec:
    merged: dict[str, Any] = {}
    if base is not None:
        merged.update(base.to_dict())
    nested = payload.get("parity")
    if isinstance(nested, Mapping):
        merged.update(dict(nested))
    aliases = BacktestParitySpec.field_aliases()
    for key in BacktestParitySpec.field_names():
        if key in payload and not _is_missing_value(payload.get(key)):
            merged[key] = payload.get(key)
    for alias, field_name in aliases.items():
        if alias in payload and not _is_missing_value(payload.get(alias)):
            merged[field_name] = payload.get(alias)
    return BacktestParitySpec.from_mapping(merged)


def _load_compare_policy(payload: dict[str, Any]) -> ExperimentComparePolicy:
    nested = payload.get("compare_policy")
    if isinstance(nested, Mapping):
        return ExperimentComparePolicy.from_mapping(nested)
    return ExperimentComparePolicy.from_mapping(payload)


def _load_runtime_policy(payload: dict[str, Any]) -> ExperimentRuntimePolicy:
    nested = payload.get("runtime_policy")
    if isinstance(nested, Mapping):
        return ExperimentRuntimePolicy.from_mapping(nested)
    nested = payload.get("run_policy")
    if isinstance(nested, Mapping):
        return ExperimentRuntimePolicy.from_mapping(nested)
    return ExperimentRuntimePolicy.from_mapping(payload)


@dataclass(frozen=True)
class _BacktestVariantConfig:
    label: str
    notes: str = ""
    overrides: dict[str, Any] | None = None


@dataclass(frozen=True)
class _FeatureSetVariantConfig:
    label: str
    feature_set: str
    notes: str = ""
    overrides: dict[str, Any] | None = None


def _parse_backtest_variants(raw: Any) -> tuple[_BacktestVariantConfig, ...]:
    if _is_missing_value(raw):
        return ()
    if isinstance(raw, Mapping) or isinstance(raw, str):
        raise TypeError(f"Expected list of backtest variants, got: {raw!r}")
    variants: list[_BacktestVariantConfig] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise TypeError(f"Unsupported backtest variant item: {item!r}")
        label = str(item.get("label") or item.get("name") or "").strip()
        if not label:
            raise ValueError(f"Missing backtest variant label at index={idx}")
        notes = str(item.get("notes") or "")
        overrides = {str(key): value for key, value in item.items() if str(key) not in {"label", "name", "notes"}}
        variants.append(_BacktestVariantConfig(label=label, notes=notes, overrides=overrides))
    return tuple(variants)


def _parse_feature_set_variants(raw: Any) -> tuple[_FeatureSetVariantConfig, ...]:
    if _is_missing_value(raw):
        return ()
    if isinstance(raw, Mapping) or isinstance(raw, str):
        raise TypeError(f"Expected list of feature_set variants, got: {raw!r}")
    variants: list[_FeatureSetVariantConfig] = []
    for idx, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise TypeError(f"Unsupported feature_set variant item: {item!r}")
        label = str(item.get("label") or item.get("name") or "").strip()
        feature_set = str(item.get("feature_set") or item.get("feature_set_name") or "").strip()
        if not label:
            raise ValueError(f"Missing feature_set variant label at index={idx}")
        if not feature_set:
            raise ValueError(f"Missing feature_set in feature_set variant at index={idx}")
        notes = str(item.get("notes") or "")
        overrides = {
            str(key): value
            for key, value in item.items()
            if str(key) not in {"label", "name", "feature_set", "feature_set_name", "notes"}
        }
        variants.append(
            _FeatureSetVariantConfig(
                label=label,
                feature_set=feature_set,
                notes=notes,
                overrides=overrides,
            )
        )
    return tuple(variants)


def _expand_market_specs(
    *,
    raw_markets: Any,
    defaults: dict[str, Any],
) -> list[ExperimentMarketSpec]:
    items = _normalize_market_items(raw_markets)
    specs: list[ExperimentMarketSpec] = []
    for item in items:
        specs.extend(_expand_market_item(item=item, defaults=defaults))
    return specs


def _normalize_market_items(raw_markets: Any) -> list[dict[str, Any] | str]:
    if isinstance(raw_markets, Mapping):
        items: list[dict[str, Any] | str] = []
        for market_name, payload in raw_markets.items():
            if _is_missing_value(payload):
                items.append(str(market_name))
                continue
            if not isinstance(payload, Mapping):
                raise TypeError(f"Unsupported market spec for {market_name!r}: {payload!r}")
            items.append({"market": str(market_name), **dict(payload)})
        return items
    if isinstance(raw_markets, list):
        return list(raw_markets)
    raise TypeError(f"Suite markets must be a list or mapping, got: {raw_markets!r}")


def _expand_market_item(
    *,
    item: dict[str, Any] | str,
    defaults: dict[str, Any],
) -> list[ExperimentMarketSpec]:
    if isinstance(item, str):
        return _materialize_variants(_merge_context(defaults, {"market": item}))
    if not isinstance(item, Mapping):
        raise TypeError(f"Unsupported market spec: {item!r}")
    context = _merge_context(defaults, item)
    groups = item.get("groups")
    if isinstance(groups, Mapping) and groups:
        specs: list[ExperimentMarketSpec] = []
        for group_name, group_payload in groups.items():
            if _is_missing_value(group_payload):
                specs.extend(_materialize_variants(_merge_context(context, {}, group_name=str(group_name))))
                continue
            if not isinstance(group_payload, Mapping):
                raise TypeError(f"Unsupported group spec for {group_name!r}: {group_payload!r}")
            group_context = _merge_context(context, group_payload, group_name=str(group_name))
            specs.extend(_expand_runs(runs=group_payload.get("runs"), context=group_context))
        return specs
    return _expand_runs(runs=item.get("runs"), context=context)


def _expand_runs(
    *,
    runs: Any,
    context: dict[str, Any],
) -> list[ExperimentMarketSpec]:
    if _is_missing_value(runs):
        return _materialize_feature_set_variants(context)
    if isinstance(runs, Mapping):
        runs_payload = [runs]
    else:
        runs_payload = list(runs)
    specs: list[ExperimentMarketSpec] = []
    for item in runs_payload:
        if isinstance(item, str):
            specs.extend(_materialize_feature_set_variants(_merge_context(context, {}, run_name=item)))
            continue
        if not isinstance(item, Mapping):
            raise TypeError(f"Unsupported run spec: {item!r}")
        run_name = str(item.get("run_name") or item.get("name") or "").strip()
        if not run_name:
            raise ValueError("Run spec under groups/runs requires run_name or name")
        run_context = _merge_context(context, item, run_name=run_name)
        specs.extend(_materialize_feature_set_variants(run_context))
    return specs


def _materialize_feature_set_variants(context: dict[str, Any]) -> list[ExperimentMarketSpec]:
    variants = tuple(context.get("feature_set_variants") or ())
    if not variants:
        return _materialize_variants(context)
    specs: list[ExperimentMarketSpec] = []
    for variant in variants:
        feature_context = _merge_context(
            context,
            variant.overrides or {},
            group_name=str(context.get("group_name") or ""),
            run_name=_feature_set_run_name(str(context.get("run_name") or ""), str(variant.label or variant.feature_set)),
        )
        feature_context["feature_set"] = str(variant.feature_set)
        feature_context["notes"] = str(variant.notes or feature_context.get("notes") or "")
        feature_context["tags"] = _merge_tags(
            context.get("tags"),
            (f"feature_set:{slug_token(str(variant.feature_set))}",),
        )
        specs.extend(_materialize_variants(feature_context))
    return specs


def _materialize_variants(context: dict[str, Any]) -> list[ExperimentMarketSpec]:
    variants = tuple(context.get("backtest_variants") or ())
    if not variants:
        return _materialize_stake_cases(context)
    specs: list[ExperimentMarketSpec] = []
    for variant in variants:
        variant_context = _merge_context(
            context,
            variant.overrides or {},
            group_name=str(context.get("group_name") or ""),
            run_name=str(context.get("run_name") or ""),
        )
        variant_context["variant_label"] = str(variant.label or "default")
        variant_context["variant_notes"] = str(variant.notes or variant_context.get("variant_notes") or "")
        specs.extend(_materialize_stake_cases(variant_context))
    return specs


def _materialize_stake_cases(context: dict[str, Any]) -> list[ExperimentMarketSpec]:
    stakes = tuple(context.get("stakes_usd") or ())
    if not stakes:
        return [_context_to_market_spec(context)]
    base_run_name = str(context.get("run_name") or "")
    max_notional_usd = _coerce_optional_float(context.get("max_notional_usd"))
    specs: list[ExperimentMarketSpec] = []
    for stake_usd in stakes:
        stake_value = float(stake_usd)
        stake_label = _stake_matrix_label(stake_value, max_notional_usd=max_notional_usd)
        stake_context = dict(context)
        stake_context["stake_usd"] = stake_value
        stake_context["max_notional_usd"] = max_notional_usd
        stake_context["matrix_parent_run_name"] = base_run_name
        stake_context["matrix_stake_label"] = stake_label
        stake_context["run_name"] = _stake_matrix_run_name(base_run_name, stake_label)
        specs.append(_context_to_market_spec(stake_context))
    return specs


def _context_to_market_spec(context: dict[str, Any]) -> ExperimentMarketSpec:
    market = str(context.get("market") or context.get("asset") or "").strip().lower()
    if not market:
        raise ValueError("Experiment market spec requires market or asset")
    return ExperimentMarketSpec(
        market=market,
        profile=str(context["profile"]),
        model_family=str(context["model_family"]),
        feature_set=str(context["feature_set"]),
        label_set=str(context["label_set"]),
        target=str(context["target"]),
        offsets=tuple(int(value) for value in context["offsets"]),
        window=context["window"],
        backtest_spec=str(context["backtest_spec"]),
        variant_label=str(context["variant_label"] or "default"),
        variant_notes=str(context["variant_notes"] or ""),
        stake_usd=_coerce_optional_float(context.get("stake_usd")),
        max_notional_usd=_coerce_optional_float(context.get("max_notional_usd")),
        matrix_parent_run_name=str(context.get("matrix_parent_run_name") or ""),
        matrix_stake_label=str(context.get("matrix_stake_label") or ""),
        hybrid_secondary_target=(
            None if context.get("hybrid_secondary_target") in {None, ""} else str(context["hybrid_secondary_target"])
        ),
        hybrid_secondary_offsets=_coerce_optional_offsets(context.get("hybrid_secondary_offsets")),
        hybrid_fallback_reasons=tuple(str(reason) for reason in context.get("hybrid_fallback_reasons") or () if str(reason)),
        parity=context["parity"] if isinstance(context["parity"], BacktestParitySpec) else BacktestParitySpec.from_mapping(context["parity"]),
        group_name=str(context.get("group_name") or ""),
        run_name=str(context.get("run_name") or ""),
        tags=tuple(str(tag) for tag in context.get("tags") or () if str(tag)),
        notes=str(context.get("notes") or ""),
    )


def _merge_context(
    parent: dict[str, Any],
    payload: Mapping[str, Any],
    *,
    group_name: str | None = None,
    run_name: str | None = None,
) -> dict[str, Any]:
    merged = dict(parent)
    merged["market"] = str(payload.get("market") or payload.get("asset") or merged.get("market") or "").strip().lower()
    for key in ("profile", "model_family", "feature_set", "label_set", "target", "backtest_spec"):
        if not _is_missing_value(payload.get(key)):
            merged[key] = str(payload.get(key))
    if "stakes" in payload or "stakes_usd" in payload:
        merged["stakes_usd"] = _parse_float_seq(_first_present_value(payload, "stakes_usd", "stakes"))
    if "max_notional_usd" in payload:
        merged["max_notional_usd"] = _coerce_optional_float(payload.get("max_notional_usd"))
    if not _is_missing_value(payload.get("offsets")):
        merged["offsets"] = tuple(int(value) for value in payload.get("offsets") or ())
    if _has_window_override(payload):
        merged["window"] = _load_window(payload.get("window") if isinstance(payload.get("window"), Mapping) else payload)
    if "hybrid_secondary_target" in payload:
        merged["hybrid_secondary_target"] = None if _is_missing_value(payload.get("hybrid_secondary_target")) else str(payload.get("hybrid_secondary_target"))
    if "hybrid_secondary_offsets" in payload:
        merged["hybrid_secondary_offsets"] = payload.get("hybrid_secondary_offsets")
    if "hybrid_fallback_reasons" in payload and not _is_missing_value(payload.get("hybrid_fallback_reasons")):
        merged["hybrid_fallback_reasons"] = tuple(str(item) for item in payload.get("hybrid_fallback_reasons") or [] if str(item))
    if not _is_missing_value(payload.get("variant_label")):
        merged["variant_label"] = str(payload.get("variant_label"))
    if not _is_missing_value(payload.get("variant_notes")):
        merged["variant_notes"] = str(payload.get("variant_notes"))
    merged["parity"] = _load_parity_spec(dict(payload), base=merged.get("parity"))
    merged["tags"] = _merge_tags(parent.get("tags"), _parse_string_seq(payload.get("tags")))
    merged["notes"] = str(payload.get("notes") or merged.get("notes") or "") if not _is_missing_value(payload.get("notes")) else str(merged.get("notes") or "")
    if group_name is not None:
        merged["group_name"] = str(group_name or "")
    elif not _is_missing_value(payload.get("group_name")):
        merged["group_name"] = str(payload.get("group_name") or "")
    if run_name is not None:
        merged["run_name"] = str(run_name or "")
    elif not _is_missing_value(payload.get("run_name")):
        merged["run_name"] = str(payload.get("run_name") or "")
    feature_set_variants = _parse_feature_set_variants(
        _first_present_value(payload, "feature_set_variants", "factor_variants", "feature_variants")
    )
    merged["feature_set_variants"] = (
        feature_set_variants if feature_set_variants else tuple(merged.get("feature_set_variants") or ())
    )
    variants = _parse_backtest_variants(payload.get("backtest_variants"))
    merged["backtest_variants"] = variants if variants else tuple(merged.get("backtest_variants") or ())
    return merged


def _feature_set_run_name(base_run_name: str, label: str) -> str:
    token = slug_token(label, default="feature_set")
    if not base_run_name:
        return token
    return f"{base_run_name}__fs_{token}"


def _merge_tags(*parts: Any) -> tuple[str, ...]:
    out: list[str] = []
    for part in parts:
        for item in part or ():
            token = str(item).strip()
            if token and token not in out:
                out.append(token)
    return tuple(out)


def _normalize_compare_reference_labels(raw: Any) -> tuple[str, ...]:
    labels = _parse_string_seq(raw)
    out: list[str] = []
    for label in labels:
        token = str(label).strip().lower()
        if token and token not in out:
            out.append(token)
    return tuple(out) if out else ("default", "baseline", "control")


def _normalize_runtime_policy_choice(
    raw: Any,
    *,
    default: str,
    allowed: tuple[str, ...],
    field_name: str,
) -> str:
    token = str(raw or "").strip().lower()
    if not token:
        return default
    if token not in allowed:
        raise ValueError(f"Unsupported {field_name}: {raw!r}; expected one of {allowed}")
    return token


def _coerce_bool(raw: Any) -> bool:
    if isinstance(raw, bool):
        return raw
    token = str(raw or "").strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"Unsupported boolean value: {raw!r}")


def _first_present_value(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and not _is_missing_value(payload.get(key)):
            return payload.get(key)
    return None


def _has_window_override(payload: Mapping[str, Any]) -> bool:
    return not (
        _is_missing_value(payload.get("window"))
        and _is_missing_value(payload.get("start"))
        and _is_missing_value(payload.get("window_start"))
    )


def _stake_matrix_run_name(base_run_name: str, stake_label: str) -> str:
    prefix = str(base_run_name or "").strip()
    token = str(stake_label or "").strip()
    if prefix and token:
        return f"{prefix}__{token}"
    return token or prefix


def _stake_matrix_label(stake_usd: float, *, max_notional_usd: float | None) -> str:
    parts = [f"stake_{_float_label_token(stake_usd)}usd"]
    if max_notional_usd is not None:
        parts.append(f"max_{_float_label_token(max_notional_usd)}usd")
    return "__".join(parts)


def _float_label_token(value: float) -> str:
    token = f"{float(value):f}".rstrip("0").rstrip(".")
    return (token or "0").replace("-", "neg_").replace(".", "p")
