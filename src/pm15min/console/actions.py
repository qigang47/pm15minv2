from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import shlex
from typing import Any

from pm15min.core.assets import resolve_asset
from pm15min.data.layout import normalize_cycle as normalize_data_cycle
from pm15min.data.layout import normalize_surface
from pm15min.research.contracts import (
    BacktestRunSpec,
    DateWindow,
    ExperimentRunSpec,
    ModelBundleSpec,
    TrainingRunSpec,
)
from pm15min.research.labels.sources import normalize_label_set
from pm15min.research.layout import normalize_target, slug_token


DEFAULT_COMMAND_PREFIX = ("PYTHONPATH=v2/src", "python", "-m", "pm15min")
EXPERIMENT_SUITE_MODE_EXISTING = "existing"
EXPERIMENT_SUITE_MODE_INLINE = "inline"
_DEFAULT_COMPARE_REFERENCE_VARIANTS = ("default", "baseline", "control")
_INLINE_SUITE_SPEC_PREVIEW_ROOT = Path("v2") / "research" / "experiments" / "suite_specs"


@dataclass(frozen=True)
class ConsoleActionField:
    field_id: str
    label: str
    input_type: str = "text"
    required: bool = False
    default_value: str | None = None
    placeholder: str | None = None
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class ConsoleActionDescriptor:
    action_id: str
    title: str
    target_domain: str
    command_role: str
    required_args: tuple[str, ...]
    form_fields: tuple[ConsoleActionField, ...] = ()
    primary_section: str | None = None
    section_ids: tuple[str, ...] = ()
    shell_enabled: bool = False
    supports_async: bool = False
    preferred_execution_mode: str = "sync"
    read_only: bool = False
    dry_run_supported: bool = False
    notes: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self) | {
            "required_args": list(self.required_args),
            "form_fields": [field.to_dict() for field in self.form_fields],
            "section_ids": list(self.section_ids),
        }
        return payload


@dataclass(frozen=True)
class ConsoleActionPlan:
    action_id: str
    descriptor: ConsoleActionDescriptor
    normalized_request: dict[str, object]
    pm15min_args: tuple[str, ...]
    command_preview: str

    def to_dict(self) -> dict[str, object]:
        return {
            "action_id": self.action_id,
            "descriptor": self.descriptor.to_dict(),
            "normalized_request": dict(self.normalized_request),
            "pm15min_args": list(self.pm15min_args),
            "command_preview": self.command_preview,
            "target_domain": self.descriptor.target_domain,
            "command_role": self.descriptor.command_role,
            "read_only": self.descriptor.read_only,
            "dry_run_supported": self.descriptor.dry_run_supported,
        }


ActionBuilder = Callable[[Mapping[str, object]], ConsoleActionPlan]


def list_console_action_descriptors(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> list[dict[str, object]]:
    resolved_section = _optional_section(for_section)
    descriptors: list[dict[str, object]] = []
    for definition in _ACTION_DEFINITIONS:
        descriptor = definition.descriptor
        if resolved_section is not None and resolved_section not in descriptor.section_ids:
            continue
        if shell_enabled is not None and bool(descriptor.shell_enabled) is not bool(shell_enabled):
            continue
        descriptors.append(descriptor.to_dict())
    return descriptors


def load_console_action_catalog(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> dict[str, object]:
    resolved_section = _optional_section(for_section)
    descriptors = list_console_action_descriptors(
        for_section=resolved_section,
        shell_enabled=shell_enabled,
    )
    return {
        "domain": "console",
        "dataset": "console_action_catalog",
        "for_section": resolved_section,
        "shell_enabled": shell_enabled,
        "action_count": len(descriptors),
        "actions": descriptors,
    }


def build_console_action_request(
    action_id: str,
    request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload = {} if request is None else {str(key): value for key, value in request.items()}
    definition = _ACTION_BY_ID.get(str(action_id).strip())
    if definition is None:
        raise ValueError(
            f"不支持的 console action_id {action_id!r}，可选值: "
            f"{', '.join(item.action_id for item in _ACTION_DEFINITIONS)}"
        )
    return definition.builder(payload).to_dict()


@dataclass(frozen=True)
class _ActionDefinition:
    action_id: str
    descriptor: ConsoleActionDescriptor
    builder: ActionBuilder


def _build_data_refresh_summary(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    surface = normalize_surface(_string_value(request.get("surface")) or "backtest")
    write_state = _bool_value(request.get("write_state"), default=True)
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "surface": surface,
        "write_state": write_state,
    }
    args = [
        "data",
        "show-summary",
        "--market",
        market,
        "--cycle",
        cycle,
        "--surface",
        surface,
    ]
    if write_state:
        args.append("--write-state")
    return _build_plan("data_refresh_summary", normalized_request, args)


def _build_data_sync(request: Mapping[str, object]) -> ConsoleActionPlan:
    sync_command = _required_slug(request, "sync_command")
    if sync_command not in {
        "market-catalog",
        "binance-klines-1m",
        "direct-oracle-prices",
        "settlement-truth-rpc",
    }:
        raise ValueError(
            "不支持的 sync_command，可选值: "
            "market-catalog, binance-klines-1m, direct-oracle-prices, settlement-truth-rpc"
        )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    surface_default = "live" if sync_command in {"binance-klines-1m", "direct-oracle-prices"} else "backtest"
    surface = normalize_surface(_string_value(request.get("surface")) or surface_default)
    normalized_request: dict[str, object] = {
        "sync_command": sync_command,
        "market": market,
        "cycle": cycle,
        "surface": surface,
    }
    args = ["data", "sync", sync_command, "--market", market]
    if sync_command in {"market-catalog", "direct-oracle-prices"}:
        args.extend(["--cycle", cycle])
    args.extend(["--surface", surface])

    for key in (
        "start_date",
        "end_date",
        "lookback_days",
        "limit",
        "max_pages",
        "sleep_sec",
        "symbol",
        "lookback_minutes",
        "batch_limit",
        "timeout_sec",
        "count",
        "max_requests",
        "chunk_blocks",
    ):
        value = request.get(key)
        if value is None or value == "":
            continue
        normalized_request[key] = value
        args.extend([f"--{key.replace('_', '-')}", str(value)])

    for key in ("include_block_timestamp", "no_single_fallback"):
        if _bool_value(request.get(key), default=False):
            normalized_request[key] = True
            args.append(f"--{key.replace('_', '-')}")

    return _build_plan("data_sync", normalized_request, args)


def _build_data_build(request: Mapping[str, object]) -> ConsoleActionPlan:
    build_command = _required_slug(request, "build_command")
    if build_command not in {"oracle-prices-15m", "truth-15m", "orderbook-index"}:
        raise ValueError(
            "不支持的 build_command，可选值: oracle-prices-15m, truth-15m, orderbook-index"
        )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    surface_default = "live" if build_command == "orderbook-index" else "backtest"
    surface = normalize_surface(_string_value(request.get("surface")) or surface_default)
    normalized_request: dict[str, object] = {
        "build_command": build_command,
        "market": market,
        "cycle": cycle,
        "surface": surface,
    }
    args = ["data", "build", build_command, "--market", market, "--cycle", cycle, "--surface", surface]
    if build_command == "orderbook-index":
        date = _required_string(request, "date")
        normalized_request["date"] = date
        args.extend(["--date", date])
    return _build_plan("data_build", normalized_request, args)


def _build_research_train_run(request: Mapping[str, object]) -> ConsoleActionPlan:
    window = DateWindow.from_bounds(
        _required_string(request, "window_start"),
        _required_string(request, "window_end"),
    )
    spec = TrainingRunSpec(
        model_family=_string_value(request.get("model_family")) or "deep_otm",
        feature_set=_string_value(request.get("feature_set")) or "deep_otm_v1",
        label_set=_string_value(request.get("label_set")) or "truth",
        target=_string_value(request.get("target")) or "direction",
        window=window,
        run_label=_string_value(request.get("run_label")) or "planned",
        offsets=_normalize_offsets(request.get("offsets"), default=(7, 8, 9)),
        label_source=_string_value(request.get("label_source")),
        parallel_workers=_optional_int(request.get("parallel_workers")),
    )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm")
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        **spec.to_dict(),
        "window_start": window.start,
        "window_end": window.end,
    }
    args = [
        "research",
        "train",
        "run",
        "--market",
        market,
        "--cycle",
        cycle,
        "--profile",
        profile,
        "--model-family",
        spec.model_family,
        "--feature-set",
        spec.feature_set,
        "--label-set",
        spec.label_set,
        "--target",
        spec.target,
        "--offsets",
        _offsets_text(spec.offsets),
        "--window-start",
        window.start,
        "--window-end",
        window.end,
        "--run-label",
        spec.run_label,
    ]
    if spec.parallel_workers is not None:
        args.extend(["--parallel-workers", str(spec.parallel_workers)])
    return _build_plan("research_train_run", normalized_request, args)


def _build_research_bundle_build(request: Mapping[str, object]) -> ConsoleActionPlan:
    spec = ModelBundleSpec(
        profile=_string_value(request.get("profile")) or "deep_otm",
        target=_string_value(request.get("target")) or "direction",
        bundle_label=_string_value(request.get("bundle_label")) or "planned",
        offsets=_normalize_offsets(request.get("offsets"), default=(7, 8, 9)),
        source_training_run=_string_value(request.get("source_training_run")),
    )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    model_family = slug_token(_string_value(request.get("model_family")) or "deep_otm")
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "model_family": model_family,
        **spec.to_dict(),
    }
    args = [
        "research",
        "bundle",
        "build",
        "--market",
        market,
        "--cycle",
        cycle,
        "--profile",
        spec.profile,
        "--model-family",
        model_family,
        "--target",
        spec.target,
        "--offsets",
        _offsets_text(spec.offsets),
        "--bundle-label",
        spec.bundle_label,
    ]
    if spec.source_training_run is not None:
        args.extend(["--source-training-run", spec.source_training_run])
    return _build_plan("research_bundle_build", normalized_request, args)


def _build_research_activate_bundle(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm")
    target = normalize_target(_string_value(request.get("target")) or "direction")
    bundle_label = slug_token(_required_string(request, "bundle_label"))
    notes = _string_value(request.get("notes"))
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "target": target,
        "bundle_label": bundle_label,
        "notes": notes or "",
    }
    args = [
        "research",
        "activate-bundle",
        "--market",
        market,
        "--cycle",
        cycle,
        "--profile",
        profile,
        "--target",
        target,
        "--bundle-label",
        bundle_label,
    ]
    if notes:
        args.extend(["--notes", notes])
    return _build_plan("research_activate_bundle", normalized_request, args)


def _build_research_backtest_run(request: Mapping[str, object]) -> ConsoleActionPlan:
    spec = BacktestRunSpec(
        profile=_string_value(request.get("profile")) or "deep_otm",
        spec_name=_string_value(request.get("spec")) or "baseline_truth",
        run_label=_string_value(request.get("run_label")) or "planned",
        target=_string_value(request.get("target")) or "direction",
        bundle_label=_string_value(request.get("bundle_label")),
        secondary_bundle_label=_string_value(request.get("secondary_bundle_label")),
        fallback_reasons=_string_sequence(request.get("fallback_reasons")),
        stake_usd=_optional_float(request.get("stake_usd")),
        max_notional_usd=_optional_float(request.get("max_notional_usd")),
        parity=_json_mapping_value(request.get("parity_json")),
    )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "profile": spec.profile,
        "spec": spec.spec_name,
        "run_label": spec.run_label,
        "target": spec.target,
        "bundle_label": spec.bundle_label,
        "secondary_bundle_label": spec.secondary_bundle_label,
        "fallback_reasons": list(spec.fallback_reasons),
        "stake_usd": spec.stake_usd,
        "max_notional_usd": spec.max_notional_usd,
        "parity_json": spec.parity.to_dict(),
    }
    args = [
        "research",
        "backtest",
        "run",
        "--market",
        market,
        "--cycle",
        cycle,
        "--profile",
        spec.profile,
        "--target",
        spec.target,
        "--spec",
        spec.spec_name,
        "--run-label",
        spec.run_label,
    ]
    if spec.bundle_label is not None:
        args.extend(["--bundle-label", spec.bundle_label])
    if spec.secondary_bundle_label is not None:
        args.extend(["--secondary-bundle-label", spec.secondary_bundle_label])
    if spec.stake_usd is not None:
        args.extend(["--stake-usd", str(spec.stake_usd)])
    if spec.max_notional_usd is not None:
        args.extend(["--max-notional-usd", str(spec.max_notional_usd)])
    if spec.fallback_reasons:
        args.extend(["--fallback-reasons", ",".join(spec.fallback_reasons)])
    if spec.parity.to_dict():
        args.extend(["--parity-json", json.dumps(spec.parity.to_dict(), ensure_ascii=False, sort_keys=True)])
    return _build_plan("research_backtest_run", normalized_request, args)


def _build_research_experiment_run_suite(request: Mapping[str, object]) -> ConsoleActionPlan:
    spec = ExperimentRunSpec(
        suite_name=_required_string(request, "suite"),
        run_label=_string_value(request.get("run_label")) or "planned",
    )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "15m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm")
    suite_mode = _normalize_experiment_suite_mode(request.get("suite_mode"))
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "suite_mode": suite_mode,
        "suite": spec.suite_name,
        "run_label": spec.run_label,
    }
    suite_arg = spec.suite_name
    if suite_mode == EXPERIMENT_SUITE_MODE_INLINE:
        inline_payload, inline_summary = _build_inline_experiment_suite_payload(
            request,
            market=market,
            cycle=cycle,
            profile=profile,
            suite_name=spec.suite_name,
        )
        suite_spec_path = _inline_suite_spec_preview_path(spec.suite_name)
        normalized_request.update(inline_summary)
        normalized_request["suite_spec_path"] = suite_spec_path
        normalized_request["inline_suite_payload"] = inline_payload
        suite_arg = suite_spec_path
    args = [
        "research",
        "experiment",
        "run-suite",
        "--market",
        market,
        "--cycle",
        cycle,
        "--profile",
        profile,
        "--suite",
        suite_arg,
        "--run-label",
        spec.run_label,
    ]
    return _build_plan("research_experiment_run_suite", normalized_request, args)


def _build_plan(action_id: str, normalized_request: dict[str, object], pm15min_args: Sequence[str]) -> ConsoleActionPlan:
    descriptor = _ACTION_BY_ID[action_id].descriptor
    args = tuple(str(item) for item in pm15min_args)
    preview_parts = [DEFAULT_COMMAND_PREFIX[0], shlex.join(list(DEFAULT_COMMAND_PREFIX[1:]) + list(args))]
    return ConsoleActionPlan(
        action_id=action_id,
        descriptor=descriptor,
        normalized_request=normalized_request,
        pm15min_args=args,
        command_preview=" ".join(preview_parts),
    )


def _normalize_market(value: object, *, default: str) -> str:
    token = _string_value(value) or default
    return resolve_asset(token).slug


def _normalize_offsets(value: object, *, default: tuple[int, ...]) -> tuple[int, ...]:
    if value is None or value == "":
        return tuple(int(item) for item in default)
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(value)
    else:
        items = [value]
    if not items:
        return tuple(int(item) for item in default)
    return tuple(int(item) for item in items)


def _offsets_text(offsets: Sequence[int]) -> str:
    return ",".join(str(int(item)) for item in offsets)


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    text = _string_value(value)
    if text is None:
        return None
    return float(text)


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    text = _string_value(value)
    if text is None:
        return None
    return int(text)


def _float_sequence(value: object) -> tuple[float, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = list(value)
    else:
        items = [value]
    return tuple(float(item) for item in items)


def _string_sequence(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        token = _string_value(value)
        items = [] if token is None else [token]
    return tuple(items)


def _json_mapping_value(value: object) -> dict[str, object]:
    text = _string_value(value)
    if text is None:
        return {}
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("`parity_json` 必须解析为 JSON 对象。")
    return {str(key): item for key, item in payload.items()}


def _json_sequence_value(value: object, *, field_name: str) -> list[object]:
    text = _string_value(value)
    if text is None:
        return []
    payload = json.loads(text)
    if not isinstance(payload, list):
        raise ValueError(f"`{field_name}` 必须解析为 JSON 数组。")
    return list(payload)


def _required_string(request: Mapping[str, object], key: str) -> str:
    value = _string_value(request.get(key))
    if value is None:
        raise ValueError(f"缺少必填 action 参数: {key}")
    return value


def _required_slug(request: Mapping[str, object], key: str) -> str:
    return slug_token(_required_string(request, key))


def _normalize_experiment_suite_mode(value: object) -> str:
    token = slug_token(_string_value(value) or EXPERIMENT_SUITE_MODE_EXISTING)
    if token not in {EXPERIMENT_SUITE_MODE_EXISTING, EXPERIMENT_SUITE_MODE_INLINE}:
        raise ValueError("`suite_mode` 仅支持 existing 或 inline。")
    return token


def _inline_suite_spec_preview_path(suite_name: str) -> str:
    return str(_INLINE_SUITE_SPEC_PREVIEW_ROOT / f"{slug_token(suite_name)}.json")


def _normalize_markets(value: object, *, default: str) -> tuple[str, ...]:
    tokens = _string_sequence(value)
    if not tokens:
        return (_normalize_market(default, default=default),)
    return tuple(_normalize_market(token, default=default) for token in tokens)


def _parse_feature_set_variants(value: object) -> tuple[dict[str, object], ...]:
    text = _string_value(value)
    if text is None:
        return ()
    if text.startswith("["):
        raw_items = _json_sequence_value(text, field_name="feature_set_variants")
        rows: list[dict[str, object]] = []
        for idx, item in enumerate(raw_items):
            if not isinstance(item, Mapping):
                raise ValueError(f"`feature_set_variants` 第 {idx + 1} 项必须是 JSON 对象。")
            label = slug_token(str(item.get("label") or item.get("name") or ""))
            feature_set = slug_token(str(item.get("feature_set") or item.get("feature_set_name") or ""))
            if not label or not feature_set:
                raise ValueError("`feature_set_variants` 每一项都必须包含 label 和 feature_set。")
            row = {
                "label": label,
                "feature_set": feature_set,
            }
            notes = _string_value(item.get("notes"))
            if notes is not None:
                row["notes"] = notes
            rows.append(row)
        return tuple(rows)
    rows = []
    for raw_token in (part.strip() for part in text.split(",") if part.strip()):
        if ":" in raw_token:
            label_text, feature_set_text = raw_token.split(":", 1)
        elif "=" in raw_token:
            label_text, feature_set_text = raw_token.split("=", 1)
        else:
            label_text, feature_set_text = raw_token, raw_token
        label = slug_token(label_text)
        feature_set = slug_token(feature_set_text)
        if not label or not feature_set:
            raise ValueError("`feature_set_variants` 不能为空，格式示例: baseline:deep_otm_v1,wide:wide_otm_v1")
        rows.append({"label": label, "feature_set": feature_set})
    return tuple(rows)


def _normalize_runtime_policy_choice(
    value: object,
    *,
    default: str,
    allowed: tuple[str, ...],
    field_name: str,
) -> str:
    token = slug_token(_string_value(value) or default)
    if token not in allowed:
        allowed_text = ", ".join(allowed)
        raise ValueError(f"`{field_name}` 仅支持 {allowed_text}。")
    return token


def _build_inline_experiment_suite_payload(
    request: Mapping[str, object],
    *,
    market: str,
    cycle: str,
    profile: str,
    suite_name: str,
) -> tuple[dict[str, object], dict[str, object]]:
    window = DateWindow.from_bounds(
        _required_string(request, "window_start"),
        _required_string(request, "window_end"),
    )
    model_family = slug_token(_string_value(request.get("model_family")) or "deep_otm")
    feature_set = slug_token(_string_value(request.get("feature_set")) or "deep_otm_v1")
    label_set = normalize_label_set(_string_value(request.get("label_set")) or "truth")
    target = normalize_target(_string_value(request.get("target")) or "direction")
    offsets = _normalize_offsets(request.get("offsets"), default=(7, 8, 9))
    backtest_spec = slug_token(_string_value(request.get("backtest_spec")) or "baseline_truth")
    group_name = slug_token(_string_value(request.get("group_name")) or "core")
    run_name = slug_token(_string_value(request.get("run_name")) or "feature_set_matrix")
    variant_label = slug_token(_string_value(request.get("variant_label")) or "default")
    variant_notes = _string_value(request.get("variant_notes")) or ""
    markets = _normalize_markets(request.get("markets"), default=market)
    feature_set_variants = _parse_feature_set_variants(request.get("feature_set_variants"))
    stakes_usd = _float_sequence(request.get("stakes_usd"))
    max_notional_usd = _optional_float(request.get("max_notional_usd"))
    reference_variant_labels = _string_sequence(request.get("reference_variant_labels")) or _DEFAULT_COMPARE_REFERENCE_VARIANTS
    runtime_policy = {
        "completed_cases": _normalize_runtime_policy_choice(
            request.get("completed_cases"),
            default="resume",
            allowed=("resume", "rerun"),
            field_name="completed_cases",
        ),
        "failed_cases": _normalize_runtime_policy_choice(
            request.get("failed_cases"),
            default="rerun",
            allowed=("rerun", "skip"),
            field_name="failed_cases",
        ),
        "parallel_case_workers": _optional_int(request.get("parallel_case_workers")) or 1,
    }
    compare_policy = {
        "reference_variant_labels": list(reference_variant_labels),
    }
    markets_payload: list[dict[str, object]] = []
    for market_token in markets:
        row: dict[str, object] = {
            "market": market_token,
            "group_name": group_name,
            "run_name": run_name,
            "backtest_spec": backtest_spec,
            "variant_label": variant_label,
        }
        if variant_notes:
            row["variant_notes"] = variant_notes
        if feature_set_variants:
            row["feature_set_variants"] = list(feature_set_variants)
        if stakes_usd:
            row["stakes_usd"] = [float(item) for item in stakes_usd]
        if max_notional_usd is not None:
            row["max_notional_usd"] = max_notional_usd
        markets_payload.append(row)
    payload = {
        "suite_name": spec_name_from_suite(suite_name),
        "cycle": cycle,
        "profile": profile,
        "model_family": model_family,
        "feature_set": feature_set,
        "label_set": label_set,
        "target": target,
        "offsets": list(offsets),
        "window": {
            "start": window.start,
            "end": window.end,
        },
        "backtest_spec": backtest_spec,
        "compare_policy": compare_policy,
        "runtime_policy": runtime_policy,
        "markets": markets_payload,
    }
    summary = {
        "window_start": window.start,
        "window_end": window.end,
        "model_family": model_family,
        "feature_set": feature_set,
        "label_set": label_set,
        "target": target,
        "offsets": list(offsets),
        "markets": list(markets),
        "group_name": group_name,
        "run_name": run_name,
        "backtest_spec": backtest_spec,
        "variant_label": variant_label,
        "variant_notes": variant_notes,
        "feature_set_variants": list(feature_set_variants),
        "stakes_usd": [float(item) for item in stakes_usd],
        "max_notional_usd": max_notional_usd,
        "runtime_policy": runtime_policy,
        "compare_policy": compare_policy,
    }
    return payload, summary


def spec_name_from_suite(suite_name: str) -> str:
    return slug_token(suite_name)


def _bool_value(value: object, *, default: bool) -> bool:
    if value is None or value == "":
        return bool(default)
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return bool(value)


def _optional_section(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    return text.replace("-", "_")


_ACTION_DEFINITIONS: tuple[_ActionDefinition, ...] = (
    _ActionDefinition(
        action_id="data_refresh_summary",
        descriptor=ConsoleActionDescriptor(
            action_id="data_refresh_summary",
            title="刷新数据总览",
            target_domain="data",
            command_role="refresh_summary",
            required_args=("market",),
            primary_section="data_overview",
            section_ids=("data_overview",),
            shell_enabled=True,
            notes="写入最新的标准数据总览状态。",
        ),
        builder=_build_data_refresh_summary,
    ),
    _ActionDefinition(
        action_id="data_sync",
        descriptor=ConsoleActionDescriptor(
            action_id="data_sync",
            title="同步数据源",
            target_domain="data",
            command_role="sync_sources",
            required_args=("sync_command", "market"),
            form_fields=(
                ConsoleActionField(
                    field_id="sync_command",
                    label="同步命令",
                    required=True,
                    placeholder="market-catalog",
                    notes="标准同步入口标识。",
                ),
            ),
            primary_section="data_overview",
            section_ids=("data_overview",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="触发一个标准数据同步入口。",
        ),
        builder=_build_data_sync,
    ),
    _ActionDefinition(
        action_id="data_build",
        descriptor=ConsoleActionDescriptor(
            action_id="data_build",
            title="构建数据表",
            target_domain="data",
            command_role="build_tables",
            required_args=("build_command", "market"),
            form_fields=(
                ConsoleActionField(
                    field_id="build_command",
                    label="构建命令",
                    required=True,
                    placeholder="oracle-prices-15m",
                    notes="标准构建入口标识。",
                ),
                ConsoleActionField(
                    field_id="date",
                    label="日期",
                    input_type="date",
                    placeholder="2026-03-23",
                    notes="仅 `orderbook-index` 构建时必填。",
                ),
            ),
            primary_section="data_overview",
            section_ids=("data_overview",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="从源数据构建一个标准数据表。",
        ),
        builder=_build_data_build,
    ),
    _ActionDefinition(
        action_id="research_train_run",
        descriptor=ConsoleActionDescriptor(
            action_id="research_train_run",
            title="发起训练运行",
            target_domain="research",
            command_role="train_run",
            required_args=("market", "window_start", "window_end", "offsets"),
            form_fields=(
                ConsoleActionField(
                    field_id="window_start",
                    label="窗口开始",
                    input_type="date",
                    required=True,
                ),
                ConsoleActionField(
                    field_id="window_end",
                    label="窗口结束",
                    input_type="date",
                    required=True,
                ),
                ConsoleActionField(
                    field_id="offsets",
                    label="Offset 列表",
                    required=True,
                    default_value="7,8,9",
                    placeholder="7,8,9",
                ),
                ConsoleActionField(
                    field_id="feature_set",
                    label="特征集",
                    default_value="deep_otm_v1",
                ),
                ConsoleActionField(
                    field_id="label_set",
                    label="标签集",
                    default_value="truth",
                ),
                ConsoleActionField(
                    field_id="label_source",
                    label="标签来源",
                    placeholder="truth",
                    notes="可选，覆盖训练使用的标签来源。",
                ),
                ConsoleActionField(
                    field_id="run_label",
                    label="运行标签",
                    default_value="planned",
                ),
                ConsoleActionField(
                    field_id="parallel_workers",
                    label="并发进程数",
                    input_type="number",
                    placeholder="2",
                    notes="可选，offset 级并发进程数。",
                ),
            ),
            primary_section="training_runs",
            section_ids=("training_runs",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="触发一个标准训练运行。",
        ),
        builder=_build_research_train_run,
    ),
    _ActionDefinition(
        action_id="research_bundle_build",
        descriptor=ConsoleActionDescriptor(
            action_id="research_bundle_build",
            title="构建模型包",
            target_domain="research",
            command_role="build_bundle",
            required_args=("market", "profile", "target", "offsets", "bundle_label"),
            form_fields=(
                ConsoleActionField(
                    field_id="bundle_label",
                    label="模型包标签",
                    required=True,
                    default_value="planned",
                ),
                ConsoleActionField(
                    field_id="offsets",
                    label="Offset 列表",
                    required=True,
                    default_value="7,8,9",
                    placeholder="7,8,9",
                ),
                ConsoleActionField(
                    field_id="source_training_run",
                    label="来源训练运行",
                    placeholder="console-assets",
                    notes="可选，指定要打包的训练运行标签。",
                ),
            ),
            primary_section="bundles",
            section_ids=("bundles",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="构建一个可部署的标准模型包。",
        ),
        builder=_build_research_bundle_build,
    ),
    _ActionDefinition(
        action_id="research_activate_bundle",
        descriptor=ConsoleActionDescriptor(
            action_id="research_activate_bundle",
            title="激活模型包",
            target_domain="research",
            command_role="activate_bundle",
            required_args=("market", "profile", "target", "bundle_label"),
            form_fields=(
                ConsoleActionField(
                    field_id="bundle_label",
                    label="模型包标签",
                    required=True,
                ),
                ConsoleActionField(
                    field_id="notes",
                    label="备注",
                    placeholder="promote",
                ),
            ),
            primary_section="bundles",
            section_ids=("bundles",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="将一个模型包提升为当前激活选择。",
        ),
        builder=_build_research_activate_bundle,
    ),
    _ActionDefinition(
        action_id="research_backtest_run",
        descriptor=ConsoleActionDescriptor(
            action_id="research_backtest_run",
            title="发起回测",
            target_domain="research",
            command_role="run_backtest",
            required_args=("market", "profile", "target", "spec", "run_label"),
            form_fields=(
                ConsoleActionField(
                    field_id="spec",
                    label="回测规格",
                    required=True,
                    default_value="baseline_truth",
                ),
                ConsoleActionField(
                    field_id="run_label",
                    label="运行标签",
                    required=True,
                    default_value="planned",
                ),
                ConsoleActionField(
                    field_id="bundle_label",
                    label="模型包标签",
                    placeholder="console-bundle",
                    notes="可选，显式覆盖使用的模型包。",
                ),
                ConsoleActionField(
                    field_id="secondary_bundle_label",
                    label="备用模型包",
                    placeholder="shadow-bundle",
                    notes="可选，混合回退时使用的模型包标签。",
                ),
                ConsoleActionField(
                    field_id="stake_usd",
                    label="下注金额 USD",
                    input_type="number",
                    placeholder="5.0",
                    notes="可选，覆盖基础下注金额。",
                ),
                ConsoleActionField(
                    field_id="max_notional_usd",
                    label="最大名义金额 USD",
                    input_type="number",
                    placeholder="8.0",
                    notes="可选，覆盖最大金额上限。",
                ),
                ConsoleActionField(
                    field_id="fallback_reasons",
                    label="回退原因",
                    placeholder="direction_prob,policy_low_confidence",
                    notes="逗号分隔的混合回退原因。",
                ),
                ConsoleActionField(
                    field_id="parity_json",
                    label="一致性参数 JSON",
                    placeholder='{"regime_enabled": true}',
                    notes="可选，JSON 格式的一致性参数覆盖。",
                ),
            ),
            primary_section="backtests",
            section_ids=("backtests",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="运行一个标准回测。",
        ),
        builder=_build_research_backtest_run,
    ),
    _ActionDefinition(
        action_id="research_experiment_run_suite",
        descriptor=ConsoleActionDescriptor(
            action_id="research_experiment_run_suite",
            title="运行实验套件",
            target_domain="research",
            command_role="run_experiment_suite",
            required_args=("market", "profile", "suite", "run_label"),
            form_fields=(
                ConsoleActionField(
                    field_id="suite_mode",
                    label="suite 模式",
                    required=True,
                    default_value="existing",
                    placeholder="existing | inline",
                    notes="existing=直接跑现有 suite；inline=由 console 生成 canonical suite spec 再运行。",
                ),
                ConsoleActionField(
                    field_id="suite",
                    label="实验套件",
                    required=True,
                ),
                ConsoleActionField(
                    field_id="run_label",
                    label="运行标签",
                    required=True,
                    default_value="planned",
                ),
                ConsoleActionField(
                    field_id="window_start",
                    label="时间窗开始",
                    placeholder="2026-03-01",
                    notes="inline 模式必填。",
                ),
                ConsoleActionField(
                    field_id="window_end",
                    label="时间窗结束",
                    placeholder="2026-03-31",
                    notes="inline 模式必填。",
                ),
                ConsoleActionField(
                    field_id="markets",
                    label="市场列表",
                    placeholder="sol,btc",
                    notes="inline 模式可选；默认使用顶部 market。",
                ),
                ConsoleActionField(
                    field_id="run_name",
                    label="运行名",
                    default_value="feature_set_matrix",
                    notes="inline 模式写入 suite markets[].run_name。",
                ),
                ConsoleActionField(
                    field_id="group_name",
                    label="分组名",
                    default_value="core",
                    notes="inline 模式写入 suite markets[].group_name。",
                ),
                ConsoleActionField(
                    field_id="feature_set_variants",
                    label="Feature Set 变体",
                    placeholder="baseline:deep_otm_v1,wide:wide_otm_v2",
                    notes="inline 模式支持 `label:feature_set` 逗号分隔，或 JSON 数组。",
                ),
                ConsoleActionField(
                    field_id="stakes_usd",
                    label="金额矩阵 USD",
                    placeholder="1,5,10",
                    notes="inline 模式写入 stakes_usd，用于 stake sweep / matrix。",
                ),
                ConsoleActionField(
                    field_id="max_notional_usd",
                    label="最大名义金额 USD",
                    input_type="number",
                    placeholder="8",
                    notes="inline 模式统一写入 max_notional_usd。",
                ),
                ConsoleActionField(
                    field_id="parallel_case_workers",
                    label="实验并发数",
                    input_type="number",
                    placeholder="2",
                    notes="inline 模式写入 runtime_policy.parallel_case_workers。",
                ),
                ConsoleActionField(
                    field_id="reference_variant_labels",
                    label="对比基线变体",
                    placeholder="default,baseline,control",
                    notes="inline 模式写入 compare_policy.reference_variant_labels。",
                ),
                ConsoleActionField(
                    field_id="completed_cases",
                    label="已完成案例策略",
                    default_value="resume",
                    placeholder="resume | rerun",
                    notes="inline 模式写入 runtime_policy.completed_cases。",
                ),
                ConsoleActionField(
                    field_id="failed_cases",
                    label="失败案例策略",
                    default_value="rerun",
                    placeholder="rerun | skip",
                    notes="inline 模式写入 runtime_policy.failed_cases。",
                ),
                ConsoleActionField(
                    field_id="offsets",
                    label="Offsets",
                    default_value="7,8,9",
                    notes="inline 模式可覆盖 suite offsets。",
                ),
                ConsoleActionField(
                    field_id="feature_set",
                    label="基础 Feature Set",
                    default_value="deep_otm_v1",
                    notes="inline 模式的默认 feature_set。",
                ),
                ConsoleActionField(
                    field_id="label_set",
                    label="标签集",
                    default_value="truth",
                    notes="inline 模式默认 label_set。",
                ),
                ConsoleActionField(
                    field_id="target",
                    label="目标",
                    default_value="direction",
                    notes="inline 模式默认 target。",
                ),
                ConsoleActionField(
                    field_id="model_family",
                    label="模型族",
                    default_value="deep_otm",
                    notes="inline 模式默认 model_family。",
                ),
                ConsoleActionField(
                    field_id="backtest_spec",
                    label="回测规格",
                    default_value="baseline_truth",
                    notes="inline 模式写入 markets[].backtest_spec。",
                ),
                ConsoleActionField(
                    field_id="variant_label",
                    label="默认变体标签",
                    default_value="default",
                    notes="inline 模式默认 variant_label。",
                ),
                ConsoleActionField(
                    field_id="variant_notes",
                    label="默认变体备注",
                    placeholder="feature sweep baseline",
                    notes="inline 模式默认 variant_notes。",
                ),
            ),
            primary_section="experiments",
            section_ids=("experiments",),
            shell_enabled=True,
            supports_async=True,
            preferred_execution_mode="async",
            notes="运行一个标准实验套件。",
        ),
        builder=_build_research_experiment_run_suite,
    ),
)

_ACTION_BY_ID = {item.action_id: item for item in _ACTION_DEFINITIONS}


__all__ = [
    "build_console_action_request",
    "list_console_action_descriptors",
    "load_console_action_catalog",
]
