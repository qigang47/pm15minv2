from __future__ import annotations


def gateway_check_wiring(
    *,
    load_env_configs_fn: object,
    build_gateway_from_env_fn: object,
    describe_gateway_fn: object,
    normalize_adapter_fn: object,
    module_available_fn: object,
) -> dict[str, object]:
    return {
        "load_env_configs_fn": load_env_configs_fn,
        "build_gateway_from_env_fn": build_gateway_from_env_fn,
        "describe_gateway_fn": describe_gateway_fn,
        "normalize_adapter_fn": normalize_adapter_fn,
        "module_available_fn": module_available_fn,
    }


def score_live_latest_wiring(
    *,
    resolve_live_profile_spec_fn: object,
    get_active_bundle_selection_fn: object,
    resolve_model_bundle_dir_fn: object,
    read_model_bundle_manifest_fn: object,
    read_bundle_config_fn: object,
    supports_feature_set_fn: object,
    build_live_feature_frame_fn: object,
    score_bundle_offset_fn: object,
    resolve_live_blacklist_fn: object,
    apply_live_blacklist_fn: object,
    latest_nan_feature_columns_fn: object,
    feature_coverage_fn: object,
    extract_feature_snapshot_fn: object,
    iso_or_none_fn: object,
    persist_live_signal_snapshot_fn: object,
    utc_snapshot_label_fn: object,
) -> dict[str, object]:
    return {
        "resolve_live_profile_spec_fn": resolve_live_profile_spec_fn,
        "get_active_bundle_selection_fn": get_active_bundle_selection_fn,
        "resolve_model_bundle_dir_fn": resolve_model_bundle_dir_fn,
        "read_model_bundle_manifest_fn": read_model_bundle_manifest_fn,
        "read_bundle_config_fn": read_bundle_config_fn,
        "supports_feature_set_fn": supports_feature_set_fn,
        "build_live_feature_frame_fn": build_live_feature_frame_fn,
        "score_bundle_offset_fn": score_bundle_offset_fn,
        "resolve_live_blacklist_fn": resolve_live_blacklist_fn,
        "apply_live_blacklist_fn": apply_live_blacklist_fn,
        "latest_nan_feature_columns_fn": latest_nan_feature_columns_fn,
        "feature_coverage_fn": feature_coverage_fn,
        "extract_feature_snapshot_fn": extract_feature_snapshot_fn,
        "iso_or_none_fn": iso_or_none_fn,
        "persist_live_signal_snapshot_fn": persist_live_signal_snapshot_fn,
        "utc_snapshot_label_fn": utc_snapshot_label_fn,
    }


def signal_check_wiring(*, score_live_latest_fn: object, supports_feature_set_fn: object) -> dict[str, object]:
    return {
        "score_live_latest_fn": score_live_latest_fn,
        "supports_feature_set_fn": supports_feature_set_fn,
    }


def decision_wiring(
    *,
    score_live_latest_fn: object,
    load_live_account_context_fn: object,
    persist_live_signal_snapshot_fn: object,
) -> dict[str, object]:
    return {
        "score_live_latest_fn": score_live_latest_fn,
        "load_live_account_context_fn": load_live_account_context_fn,
        "persist_live_signal_snapshot_fn": persist_live_signal_snapshot_fn,
    }


def quote_wiring(*, score_live_latest_fn: object) -> dict[str, object]:
    return {
        "score_live_latest_fn": score_live_latest_fn,
    }


def sync_account_wiring(*, build_live_trading_gateway_from_env_fn: object, build_account_state_snapshot_fn: object) -> dict[str, object]:
    return {
        "build_live_trading_gateway_from_env_fn": build_live_trading_gateway_from_env_fn,
        "build_account_state_snapshot_fn": build_account_state_snapshot_fn,
    }


def sync_liquidity_wiring(*, build_liquidity_state_snapshot_fn: object) -> dict[str, object]:
    return {
        "build_liquidity_state_snapshot_fn": build_liquidity_state_snapshot_fn,
    }


def execute_cancel_wiring(*, build_live_trading_gateway_from_env_fn: object, apply_cancel_policy_fn: object) -> dict[str, object]:
    return {
        "build_live_trading_gateway_from_env_fn": build_live_trading_gateway_from_env_fn,
        "apply_cancel_policy_fn": apply_cancel_policy_fn,
    }


def execute_redeem_wiring(*, build_live_trading_gateway_from_env_fn: object, apply_redeem_policy_fn: object) -> dict[str, object]:
    return {
        "build_live_trading_gateway_from_env_fn": build_live_trading_gateway_from_env_fn,
        "apply_redeem_policy_fn": apply_redeem_policy_fn,
    }


def execute_latest_wiring(
    *,
    build_live_trading_gateway_from_env_fn: object,
    simulate_live_execution_fn: object,
    submit_execution_payload_fn: object,
) -> dict[str, object]:
    return {
        "build_live_trading_gateway_from_env_fn": build_live_trading_gateway_from_env_fn,
        "simulate_live_execution_fn": simulate_live_execution_fn,
        "submit_execution_payload_fn": submit_execution_payload_fn,
    }


def simulate_execution_wiring(
    *,
    decide_live_latest_fn: object,
    build_execution_snapshot_fn: object,
    persist_execution_snapshot_fn: object,
) -> dict[str, object]:
    return {
        "decide_live_latest_fn": decide_live_latest_fn,
        "build_execution_snapshot_fn": build_execution_snapshot_fn,
        "persist_execution_snapshot_fn": persist_execution_snapshot_fn,
    }
