from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import shlex
from typing import Any

from pm5min.data.layout import normalize_cycle as normalize_data_cycle
from pm5min.data.layout import normalize_surface
from pm5min.research.layout_helpers import normalize_target, slug_token
from pmshared.assets import resolve_asset


DEFAULT_COMMAND_PREFIX = ("PYTHONPATH=src", "python", "-m", "pm5min")
EXPERIMENT_SUITE_MODE_EXISTING = "existing"
EXPERIMENT_SUITE_MODE_INLINE = "inline"
_INLINE_SUITE_SPEC_PREVIEW_ROOT = Path("research") / "experiments" / "suite_specs"


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
        payload = asdict(self)
        payload["required_args"] = list(self.required_args)
        payload["form_fields"] = [field.to_dict() for field in self.form_fields]
        payload["section_ids"] = list(self.section_ids)
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
            "normalized_request": _json_safe(self.normalized_request),
            "pm15min_args": list(self.pm15min_args),
            "command_preview": self.command_preview,
        }


ActionBuilder = Callable[[Mapping[str, object]], ConsoleActionPlan]


@dataclass(frozen=True)
class _ActionDefinition:
    action_id: str
    descriptor: ConsoleActionDescriptor
    builder: ActionBuilder


def _build_data_refresh_summary(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    surface = normalize_surface(_string_value(request.get("surface")) or "backtest")
    normalized_request = {
        "market": market,
        "cycle": cycle,
        "surface": surface,
    }
    return _build_plan(
        "data_refresh_summary",
        normalized_request,
        [
            "data",
            "show-summary",
            "--market",
            market,
            "--cycle",
            cycle,
            "--surface",
            surface,
        ],
    )


def _build_data_sync(request: Mapping[str, object]) -> ConsoleActionPlan:
    sync_command = _required_slug(request, "sync_command")
    supported = {
        "market-catalog",
        "binance-klines-1m",
        "direct-oracle-prices",
        "settlement-truth-rpc",
        "legacy-settlement-truth",
    }
    if sync_command not in supported:
        raise ValueError(
            "不支持的 sync_command，可选值: "
            "market-catalog, binance-klines-1m, direct-oracle-prices, settlement-truth-rpc, legacy-settlement-truth"
        )
    market = _normalize_market(request.get("market"), default="sol")
    cycle_commands = {
        "market-catalog",
        "direct-oracle-prices",
        "settlement-truth-rpc",
        "legacy-settlement-truth",
    }
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    surface_default = "live" if sync_command in {"binance-klines-1m", "direct-oracle-prices"} else "backtest"
    surface = normalize_surface(_string_value(request.get("surface")) or surface_default)
    normalized_request: dict[str, object] = {
        "sync_command": sync_command,
        "market": market,
        "surface": surface,
    }
    args = ["data", "sync", sync_command, "--market", market]
    if sync_command in cycle_commands:
        normalized_request["cycle"] = cycle
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
        "chunk_blocks",
        "source_path",
    ):
        value = request.get(key)
        if value in (None, ""):
            continue
        normalized_request[key] = _json_safe(value)
        args.extend([f"--{key.replace('_', '-')}", str(value)])
    for key in ("include_block_timestamp", "no_single_fallback"):
        if _bool_value(request.get(key), default=False):
            normalized_request[key] = True
            args.append(f"--{key.replace('_', '-')}")
    return _build_plan("data_sync", normalized_request, args)


def _build_data_build(request: Mapping[str, object]) -> ConsoleActionPlan:
    build_command = _required_slug(request, "build_command")
    supported = {"oracle-prices-15m", "truth-15m", "orderbook-index"}
    if build_command not in supported:
        raise ValueError(
            "不支持的 build_command，可选值: oracle-prices-15m, truth-15m, orderbook-index"
        )
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
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
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm_5m")
    model_family = slug_token(_string_value(request.get("model_family")) or "deep_otm")
    feature_set = slug_token(_string_value(request.get("feature_set")) or "deep_otm_v1")
    label_set = slug_token(_string_value(request.get("label_set")) or "truth")
    target = normalize_target(_string_value(request.get("target")) or "direction")
    run_label = slug_token(_string_value(request.get("run_label")) or "planned")
    offsets = _normalize_offsets(request.get("offsets"), default=(2, 3, 4))
    window_start = _required_string(request, "window_start")
    window_end = _required_string(request, "window_end")
    normalized_request: dict[str, object] = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "model_family": model_family,
        "feature_set": feature_set,
        "label_set": label_set,
        "target": target,
        "offsets": list(offsets),
        "window_start": window_start,
        "window_end": window_end,
        "run_label": run_label,
        "parallel_workers": _optional_int(request.get("parallel_workers")),
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
        model_family,
        "--feature-set",
        feature_set,
        "--label-set",
        label_set,
        "--target",
        target,
        "--offsets",
        _offsets_text(offsets),
        "--window-start",
        window_start,
        "--window-end",
        window_end,
        "--run-label",
        run_label,
    ]
    if normalized_request["parallel_workers"] is not None:
        args.extend(["--parallel-workers", str(normalized_request["parallel_workers"])])
    return _build_plan("research_train_run", normalized_request, args)


def _build_research_bundle_build(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm_5m")
    model_family = slug_token(_string_value(request.get("model_family")) or "deep_otm")
    target = normalize_target(_string_value(request.get("target")) or "direction")
    offsets = _normalize_offsets(request.get("offsets"), default=(2, 3, 4))
    bundle_label = slug_token(_string_value(request.get("bundle_label")) or "planned")
    source_training_run = _optional_slug(request.get("source_training_run"))
    normalized_request: dict[str, object] = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "model_family": model_family,
        "target": target,
        "offsets": list(offsets),
        "bundle_label": bundle_label,
        "source_training_run": source_training_run,
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
        profile,
        "--model-family",
        model_family,
        "--target",
        target,
        "--offsets",
        _offsets_text(offsets),
        "--bundle-label",
        bundle_label,
    ]
    if source_training_run is not None:
        args.extend(["--source-training-run", source_training_run])
    return _build_plan("research_bundle_build", normalized_request, args)


def _build_research_activate_bundle(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm_5m")
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
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm_5m")
    target = normalize_target(_string_value(request.get("target")) or "direction")
    spec = slug_token(_string_value(request.get("spec")) or "baseline_truth")
    run_label = slug_token(_string_value(request.get("run_label")) or "planned")
    bundle_label = _optional_slug(request.get("bundle_label"))
    secondary_bundle_label = _optional_slug(request.get("secondary_bundle_label"))
    fallback_reasons = _string_sequence(request.get("fallback_reasons"))
    parity_json = _json_mapping_value(request.get("parity_json"))
    normalized_request: dict[str, object] = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "target": target,
        "spec": spec,
        "run_label": run_label,
        "bundle_label": bundle_label,
        "secondary_bundle_label": secondary_bundle_label,
        "fallback_reasons": list(fallback_reasons),
        "stake_usd": _optional_float(request.get("stake_usd")),
        "max_notional_usd": _optional_float(request.get("max_notional_usd")),
        "parity_json": parity_json,
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
        profile,
        "--target",
        target,
        "--spec",
        spec,
        "--run-label",
        run_label,
    ]
    if bundle_label is not None:
        args.extend(["--bundle-label", bundle_label])
    if secondary_bundle_label is not None:
        args.extend(["--secondary-bundle-label", secondary_bundle_label])
    if normalized_request["stake_usd"] is not None:
        args.extend(["--stake-usd", str(normalized_request["stake_usd"])])
    if normalized_request["max_notional_usd"] is not None:
        args.extend(["--max-notional-usd", str(normalized_request["max_notional_usd"])])
    if fallback_reasons:
        args.extend(["--fallback-reasons", ",".join(fallback_reasons)])
    if parity_json:
        args.extend(["--parity-json", json.dumps(parity_json, ensure_ascii=False, sort_keys=True)])
    return _build_plan("research_backtest_run", normalized_request, args)


def _build_research_experiment_run_suite(request: Mapping[str, object]) -> ConsoleActionPlan:
    market = _normalize_market(request.get("market"), default="sol")
    cycle = normalize_data_cycle(_string_value(request.get("cycle")) or "5m")
    profile = slug_token(_string_value(request.get("profile")) or "deep_otm_5m")
    suite = _required_string(request, "suite")
    run_label = slug_token(_string_value(request.get("run_label")) or "planned")
    suite_mode = _normalize_suite_mode(request.get("suite_mode"))
    normalized_request: dict[str, object] = {
        "market": market,
        "cycle": cycle,
        "profile": profile,
        "suite": suite,
        "run_label": run_label,
        "suite_mode": suite_mode,
    }
    suite_arg = suite
    if suite_mode == EXPERIMENT_SUITE_MODE_INLINE:
        inline_suite_payload = _json_mapping_value(request.get("inline_suite_payload"))
        if not inline_suite_payload:
            raise ValueError("Inline experiment suite 缺少 inline_suite_payload。")
        suite_spec_path = str(_INLINE_SUITE_SPEC_PREVIEW_ROOT / f"{slug_token(suite)}.json")
        normalized_request["suite_spec_path"] = suite_spec_path
        normalized_request["inline_suite_payload"] = inline_suite_payload
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
        run_label,
    ]
    return _build_plan("research_experiment_run_suite", normalized_request, args)


_ACTION_DEFINITIONS: tuple[_ActionDefinition, ...] = (
    _ActionDefinition(
        action_id="data_refresh_summary",
        descriptor=ConsoleActionDescriptor(
            action_id="data_refresh_summary",
            title="刷新数据摘要",
            target_domain="data",
            command_role="read",
            required_args=(),
            primary_section="data_overview",
            section_ids=("data_overview",),
            read_only=True,
        ),
        builder=_build_data_refresh_summary,
    ),
    _ActionDefinition(
        action_id="data_sync",
        descriptor=ConsoleActionDescriptor(
            action_id="data_sync",
            title="同步数据源",
            target_domain="data",
            command_role="mutate",
            required_args=("sync_command",),
            primary_section="data_overview",
            section_ids=("data_overview",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_data_sync,
    ),
    _ActionDefinition(
        action_id="data_build",
        descriptor=ConsoleActionDescriptor(
            action_id="data_build",
            title="构建数据产物",
            target_domain="data",
            command_role="mutate",
            required_args=("build_command",),
            primary_section="data_overview",
            section_ids=("data_overview",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_data_build,
    ),
    _ActionDefinition(
        action_id="research_train_run",
        descriptor=ConsoleActionDescriptor(
            action_id="research_train_run",
            title="训练模型",
            target_domain="research",
            command_role="mutate",
            required_args=("window_start", "window_end"),
            primary_section="training_runs",
            section_ids=("training_runs",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_research_train_run,
    ),
    _ActionDefinition(
        action_id="research_bundle_build",
        descriptor=ConsoleActionDescriptor(
            action_id="research_bundle_build",
            title="构建模型包",
            target_domain="research",
            command_role="mutate",
            required_args=(),
            primary_section="bundles",
            section_ids=("bundles",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_research_bundle_build,
    ),
    _ActionDefinition(
        action_id="research_activate_bundle",
        descriptor=ConsoleActionDescriptor(
            action_id="research_activate_bundle",
            title="激活模型包",
            target_domain="research",
            command_role="mutate",
            required_args=("bundle_label",),
            primary_section="bundles",
            section_ids=("bundles",),
        ),
        builder=_build_research_activate_bundle,
    ),
    _ActionDefinition(
        action_id="research_backtest_run",
        descriptor=ConsoleActionDescriptor(
            action_id="research_backtest_run",
            title="运行回测",
            target_domain="research",
            command_role="mutate",
            required_args=(),
            primary_section="backtests",
            section_ids=("backtests",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_research_backtest_run,
    ),
    _ActionDefinition(
        action_id="research_experiment_run_suite",
        descriptor=ConsoleActionDescriptor(
            action_id="research_experiment_run_suite",
            title="运行实验套件",
            target_domain="research",
            command_role="mutate",
            required_args=("suite",),
            primary_section="experiments",
            section_ids=("experiments",),
            supports_async=True,
            preferred_execution_mode="async",
        ),
        builder=_build_research_experiment_run_suite,
    ),
)
_ACTION_BY_ID = {item.action_id: item for item in _ACTION_DEFINITIONS}


def list_console_action_descriptors(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> list[dict[str, object]]:
    resolved_section = _optional_text(for_section)
    rows: list[dict[str, object]] = []
    for definition in _ACTION_DEFINITIONS:
        descriptor = definition.descriptor
        if resolved_section is not None and resolved_section not in descriptor.section_ids:
            continue
        if shell_enabled is not None and descriptor.shell_enabled is not bool(shell_enabled):
            continue
        rows.append(descriptor.to_dict())
    return rows


def load_console_action_catalog(
    *,
    for_section: str | None = None,
    shell_enabled: bool | None = None,
) -> dict[str, object]:
    actions = list_console_action_descriptors(
        for_section=for_section,
        shell_enabled=shell_enabled,
    )
    return {
        "domain": "console",
        "dataset": "console_action_catalog",
        "for_section": _optional_text(for_section),
        "shell_enabled": shell_enabled,
        "action_count": len(actions),
        "actions": actions,
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


def _build_plan(action_id: str, normalized_request: dict[str, object], args: Sequence[str]) -> ConsoleActionPlan:
    descriptor = _ACTION_BY_ID[action_id].descriptor
    resolved_args = tuple(str(item) for item in args)
    preview_parts = [DEFAULT_COMMAND_PREFIX[0], shlex.join(list(DEFAULT_COMMAND_PREFIX[1:]) + list(resolved_args))]
    return ConsoleActionPlan(
        action_id=action_id,
        descriptor=descriptor,
        normalized_request=normalized_request,
        pm15min_args=resolved_args,
        command_preview=" ".join(preview_parts),
    )


def _normalize_market(value: object, *, default: str) -> str:
    token = _string_value(value) or default
    return resolve_asset(token).slug


def _normalize_offsets(value: object, *, default: Sequence[int]) -> tuple[int, ...]:
    if value in (None, ""):
        return tuple(int(item) for item in default)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(int(item) for item in value)
    text = str(value).strip()
    if not text:
        return tuple(int(item) for item in default)
    return tuple(int(item.strip()) for item in text.split(",") if item.strip())


def _offsets_text(offsets: Sequence[int]) -> str:
    return ",".join(str(int(item)) for item in offsets)


def _normalize_suite_mode(value: object) -> str:
    token = (_string_value(value) or EXPERIMENT_SUITE_MODE_EXISTING).strip().lower()
    if token not in {EXPERIMENT_SUITE_MODE_EXISTING, EXPERIMENT_SUITE_MODE_INLINE}:
        raise ValueError("Experiment suite mode only supports existing or inline.")
    return token


def _json_mapping_value(value: object) -> dict[str, object]:
    if value is None or value == "":
        return {}
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, str):
        payload = json.loads(value)
        if not isinstance(payload, Mapping):
            raise ValueError("JSON payload must be an object.")
        return {str(key): _json_safe(item) for key, item in payload.items()}
    raise ValueError("JSON payload must be a mapping or JSON object string.")


def _string_sequence(value: object) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return tuple(part.strip() for part in str(value).split(",") if part.strip())


def _string_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_text(value: object) -> str | None:
    return _string_value(value)


def _optional_slug(value: object) -> str | None:
    text = _string_value(value)
    if text is None:
        return None
    return slug_token(text)


def _required_string(request: Mapping[str, object], key: str) -> str:
    value = _string_value(request.get(key))
    if value is None:
        raise ValueError(f"缺少必填字段: {key}")
    return value


def _required_slug(request: Mapping[str, object], key: str) -> str:
    return slug_token(_required_string(request, key))


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _bool_value(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _json_safe(value: object) -> object:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_safe(item) for item in value]
    return str(value)


__all__ = [
    "ConsoleActionDescriptor",
    "ConsoleActionField",
    "ConsoleActionPlan",
    "DEFAULT_COMMAND_PREFIX",
    "build_console_action_request",
    "list_console_action_descriptors",
    "load_console_action_catalog",
]
