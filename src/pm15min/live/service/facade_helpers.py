from __future__ import annotations

import importlib.util
from collections.abc import Mapping
from pathlib import Path

import pandas as pd

from pm15min.core.config import LiveConfig
from .wiring import (
    decision_wiring as _decision_wiring_impl,
    execute_cancel_wiring as _execute_cancel_wiring_impl,
    execute_latest_wiring as _execute_latest_wiring_impl,
    execute_redeem_wiring as _execute_redeem_wiring_impl,
    gateway_check_wiring as _gateway_check_wiring_impl,
    quote_wiring as _quote_wiring_impl,
    score_live_latest_wiring as _score_live_latest_wiring_impl,
    signal_check_wiring as _signal_check_wiring_impl,
    simulate_execution_wiring as _simulate_execution_wiring_impl,
    sync_account_wiring as _sync_account_wiring_impl,
    sync_liquidity_wiring as _sync_liquidity_wiring_impl,
)
from ..signal.utils import (
    apply_live_blacklist as _apply_live_blacklist_impl,
    build_live_feature_frame as _build_live_feature_frame_impl,
    extract_feature_snapshot as _extract_feature_snapshot_impl,
    feature_coverage as _feature_coverage_impl,
    iso_or_none as _iso_or_none_impl,
    latest_nan_feature_columns as _latest_nan_feature_columns_impl,
    load_live_account_context as _load_live_account_context_impl,
    persist_live_signal_snapshot as _persist_live_signal_snapshot_impl,
    resolve_live_blacklist as _resolve_live_blacklist_impl,
    supports_feature_set as _supports_feature_set_impl,
)

_Namespace = Mapping[str, object]


def _resolve_wiring(namespace: _Namespace, bindings: dict[str, str]) -> dict[str, object]:
    return {wire_name: namespace[source_name] for wire_name, source_name in bindings.items()}


def build_gateway_check_wiring(namespace: _Namespace) -> dict[str, object]:
    return _gateway_check_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "load_env_configs_fn": "load_live_trading_env_configs",
                "build_gateway_from_env_fn": "build_live_trading_gateway_from_env",
                "describe_gateway_fn": "describe_live_trading_gateway",
                "normalize_adapter_fn": "normalize_live_trading_adapter",
                "module_available_fn": "_module_available",
            },
        )
    )


def build_score_live_latest_wiring(namespace: _Namespace) -> dict[str, object]:
    return _score_live_latest_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "resolve_live_profile_spec_fn": "resolve_live_profile_spec",
                "get_active_bundle_selection_fn": "get_active_bundle_selection",
                "resolve_model_bundle_dir_fn": "resolve_model_bundle_dir",
                "read_model_bundle_manifest_fn": "read_model_bundle_manifest",
                "read_bundle_config_fn": "read_bundle_config",
                "supports_feature_set_fn": "_supports_feature_set",
                "build_live_feature_frame_fn": "_build_live_feature_frame",
                "score_bundle_offset_fn": "score_bundle_offset",
                "resolve_live_blacklist_fn": "_resolve_live_blacklist",
                "apply_live_blacklist_fn": "_apply_live_blacklist",
                "latest_nan_feature_columns_fn": "_latest_nan_feature_columns",
                "feature_coverage_fn": "_feature_coverage",
                "extract_feature_snapshot_fn": "_extract_feature_snapshot",
                "iso_or_none_fn": "_iso_or_none",
                "persist_live_signal_snapshot_fn": "_persist_live_signal_snapshot",
                "utc_snapshot_label_fn": "utc_snapshot_label",
            },
        )
    )


def build_signal_check_wiring(namespace: _Namespace) -> dict[str, object]:
    return _signal_check_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "score_live_latest_fn": "score_live_latest",
                "supports_feature_set_fn": "_supports_feature_set",
            },
        )
    )


def build_decision_wiring(namespace: _Namespace) -> dict[str, object]:
    return _decision_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "score_live_latest_fn": "score_live_latest",
                "load_live_account_context_fn": "_load_live_account_context",
                "persist_live_signal_snapshot_fn": "_persist_live_signal_snapshot",
            },
        )
    )


def build_quote_wiring(namespace: _Namespace) -> dict[str, object]:
    return _quote_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "score_live_latest_fn": "score_live_latest",
            },
        )
    )


def build_sync_account_wiring(namespace: _Namespace) -> dict[str, object]:
    return _sync_account_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "build_live_trading_gateway_from_env_fn": "build_live_trading_gateway_from_env",
                "build_account_state_snapshot_fn": "build_account_state_snapshot",
            },
        )
    )


def build_sync_liquidity_wiring(namespace: _Namespace) -> dict[str, object]:
    return _sync_liquidity_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "build_liquidity_state_snapshot_fn": "build_liquidity_state_snapshot",
            },
        )
    )


def build_execute_cancel_wiring(namespace: _Namespace) -> dict[str, object]:
    return _execute_cancel_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "build_live_trading_gateway_from_env_fn": "build_live_trading_gateway_from_env",
                "apply_cancel_policy_fn": "apply_cancel_policy",
            },
        )
    )


def build_execute_redeem_wiring(namespace: _Namespace) -> dict[str, object]:
    return _execute_redeem_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "build_live_trading_gateway_from_env_fn": "build_live_trading_gateway_from_env",
                "apply_redeem_policy_fn": "apply_redeem_policy",
            },
        )
    )


def build_execute_latest_wiring(namespace: _Namespace) -> dict[str, object]:
    return _execute_latest_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "build_live_trading_gateway_from_env_fn": "build_live_trading_gateway_from_env",
                "simulate_live_execution_fn": "simulate_live_execution",
                "submit_execution_payload_fn": "submit_execution_payload",
            },
        )
    )


def build_simulate_execution_wiring(namespace: _Namespace) -> dict[str, object]:
    return _simulate_execution_wiring_impl(
        **_resolve_wiring(
            namespace,
            {
                "decide_live_latest_fn": "decide_live_latest",
                "build_execution_snapshot_fn": "build_execution_snapshot",
                "persist_execution_snapshot_fn": "persist_execution_snapshot",
            },
        )
    )


def module_available(name: str) -> bool:
    try:
        return importlib.util.find_spec(name) is not None
    except Exception:
        return False


def supports_feature_set(feature_set: str) -> bool:
    return _supports_feature_set_impl(feature_set)


def build_live_feature_frame(
    cfg: LiveConfig,
    *,
    feature_set: str,
    retain_offsets: tuple[int, ...] | None = None,
    allow_preview_open_bar: bool = False,
    required_feature_columns: set[str] | None = None,
) -> pd.DataFrame:
    kwargs = {
        "feature_set": feature_set,
        "retain_offsets": retain_offsets,
        "allow_preview_open_bar": allow_preview_open_bar,
    }
    if required_feature_columns is not None:
        kwargs["required_feature_columns"] = required_feature_columns
    return _build_live_feature_frame_impl(cfg, **kwargs)


def load_live_account_context(
    cfg: LiveConfig,
    *,
    utc_snapshot_label_fn: object,
) -> dict[str, object]:
    return _load_live_account_context_impl(cfg, utc_snapshot_label_fn=utc_snapshot_label_fn)


def feature_coverage(
    *,
    available_columns: set[str],
    required_columns: list[str],
    blacklisted_columns: list[str],
    not_allowed_blacklist_columns: list[str],
    nan_feature_columns: list[str],
) -> dict[str, object]:
    return _feature_coverage_impl(
        available_columns=available_columns,
        required_columns=required_columns,
        blacklisted_columns=blacklisted_columns,
        not_allowed_blacklist_columns=not_allowed_blacklist_columns,
        nan_feature_columns=nan_feature_columns,
    )


def resolve_live_blacklist(
    *,
    profile_blacklist: list[str],
    bundle_allowed_blacklist: list[str],
) -> tuple[list[str], list[str]]:
    return _resolve_live_blacklist_impl(
        profile_blacklist=profile_blacklist,
        bundle_allowed_blacklist=bundle_allowed_blacklist,
    )


def apply_live_blacklist(features: pd.DataFrame, *, blacklist_columns: list[str]) -> None:
    _apply_live_blacklist_impl(features, blacklist_columns=blacklist_columns)


def latest_nan_feature_columns(
    *,
    features: pd.DataFrame,
    offset: int,
    decision_ts,
    required_columns: list[str],
) -> list[str]:
    return _latest_nan_feature_columns_impl(
        features=features,
        offset=offset,
        decision_ts=decision_ts,
        required_columns=required_columns,
    )


def persist_live_signal_snapshot(
    cfg: LiveConfig,
    *,
    target: str,
    snapshot_ts: str,
    payload: dict[str, object],
) -> dict[str, Path]:
    return _persist_live_signal_snapshot_impl(
        cfg,
        target=target,
        snapshot_ts=snapshot_ts,
        payload=payload,
    )


def extract_feature_snapshot(features: pd.DataFrame, *, offset: int, decision_ts) -> dict[str, object]:
    return _extract_feature_snapshot_impl(features, offset=offset, decision_ts=decision_ts)


def iso_or_none(value) -> str | None:
    return _iso_or_none_impl(value)
