from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any

_LAZY_IMPORTS: dict[str, tuple[str, str]] = {
    "utc_snapshot_label": ("pm15min.data.layout", "utc_snapshot_label"),
    "build_depth_execution_plan": (".depth", "build_depth_execution_plan"),
    "build_execution_record": (".policy", "build_execution_record"),
    "build_policy_context": (".policy", "build_policy_context"),
    "load_policy_state": (".policy", "load_policy_state"),
    "resolve_dynamic_stake_base": (".policy", "resolve_dynamic_stake_base"),
    "resolve_execution_account_summary": (".policy", "resolve_execution_account_summary"),
    "resolve_regime_stake_multiplier": (".policy", "resolve_regime_stake_multiplier"),
    "_build_execution_snapshot_impl": (".service", "build_execution_snapshot"),
    "float_or_none": (".utils", "float_or_none"),
    "resolve_side_probability": (".utils", "resolve_side_probability"),
    "LiveStateLayout": ("..layout", "LiveStateLayout"),
    "write_live_payload_pair": ("..persistence", "write_live_payload_pair"),
    "resolve_live_profile_spec": ("..profiles", "resolve_live_profile_spec"),
}

_MISSING = object()


def _load_attr(name: str):
    existing = globals().get(name, _MISSING)
    if existing is not _MISSING:
        return existing
    module_name, attr_name = _LAZY_IMPORTS[name]
    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value


def __getattr__(name: str):
    if name in _LAZY_IMPORTS:
        return _load_attr(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def build_execution_snapshot(
    cfg,
    decision_payload: dict[str, Any],
    *,
    orderbook_provider=None,
    prefer_live_depth: bool = False,
) -> dict[str, Any]:
    return _load_attr("_build_execution_snapshot_impl")(
        cfg,
        decision_payload,
        orderbook_provider=orderbook_provider,
        prefer_live_depth=prefer_live_depth,
        resolve_live_profile_spec_fn=_load_attr("resolve_live_profile_spec"),
        utc_snapshot_label_fn=_load_attr("utc_snapshot_label"),
        load_policy_state_fn=_load_attr("load_policy_state"),
        build_policy_context_fn=_load_attr("build_policy_context"),
        build_execution_record_fn=_load_attr("build_execution_record"),
        resolve_regime_stake_multiplier_fn=_load_attr("resolve_regime_stake_multiplier"),
        resolve_execution_account_summary_fn=_load_attr("resolve_execution_account_summary"),
        resolve_dynamic_stake_base_fn=_load_attr("resolve_dynamic_stake_base"),
        resolve_side_probability_fn=_load_attr("resolve_side_probability"),
        float_or_none_fn=_load_attr("float_or_none"),
        build_depth_execution_plan_fn=_load_attr("build_depth_execution_plan"),
    )


def persist_execution_snapshot(*, rewrite_root: Path, payload: dict[str, Any]) -> dict[str, Path]:
    layout = _load_attr("LiveStateLayout").discover(root=rewrite_root)
    latest_path = layout.latest_execution_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
    )
    snapshot_path = layout.execution_snapshot_path(
        market=str(payload["market"]),
        cycle=str(payload["cycle"]),
        profile=str(payload["profile"]),
        target=str(payload["target"]),
        snapshot_ts=str(payload["snapshot_ts"]),
    )
    return _load_attr("write_live_payload_pair")(
        payload=payload,
        latest_path=latest_path,
        snapshot_path=snapshot_path,
        write_snapshot_history=False,
    )
