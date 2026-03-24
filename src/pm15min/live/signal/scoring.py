from __future__ import annotations

from .scoring_bundle import BundleResolution, LiveFeatureContext
from .scoring_bundle import (
    prepare_live_features_and_states as _prepare_live_features_and_states,
)
from .scoring_bundle import resolve_bundle_resolution as _resolve_bundle_resolution
from .scoring_offsets import score_offset_signals as _score_offset_signals


def _optional_payload_field(payload: dict[str, object] | None, key: str) -> object | None:
    return None if payload is None else payload.get(key)


def _build_signal_payload(
    cfg,
    *,
    bundle: BundleResolution,
    feature_ctx: LiveFeatureContext,
    offset_signals: list[dict[str, object]],
    utc_snapshot_label_fn,
    iso_or_none_fn,
) -> dict[str, object]:
    snapshot_ts = utc_snapshot_label_fn()
    bundle_label = str(bundle.bundle_manifest.spec.get("bundle_label") or bundle.bundle_dir.name.split("=", 1)[-1])
    return {
        "domain": "live",
        "dataset": "live_signal_snapshot",
        "market": cfg.asset.slug,
        "profile": cfg.profile,
        "cycle": f"{int(cfg.cycle_minutes)}m",
        "target": bundle.selected_target,
        "builder_feature_set": bundle.builder_feature_set,
        "bundle_feature_set": bundle.manifest_feature_set,
        "profile_spec": bundle.profile_spec.to_dict(),
        "bundle_dir": str(bundle.bundle_dir),
        "bundle_label": bundle_label,
        "active_bundle_selection_path": bundle.active_payload["selection_path"],
        "active_bundle": bundle.selection,
        "snapshot_ts": snapshot_ts,
        "feature_rows": int(len(feature_ctx.base_features)),
        "latest_feature_decision_ts": iso_or_none_fn(feature_ctx.base_features["decision_ts"].max()),
        "liquidity_state_snapshot_ts": _optional_payload_field(feature_ctx.liquidity_payload, "snapshot_ts"),
        "latest_liquidity_path": _optional_payload_field(feature_ctx.liquidity_payload, "latest_liquidity_path"),
        "liquidity_snapshot_path": _optional_payload_field(feature_ctx.liquidity_payload, "liquidity_snapshot_path"),
        "liquidity_state": feature_ctx.liquidity_state,
        "regime_state_snapshot_ts": _optional_payload_field(feature_ctx.regime_payload, "snapshot_ts"),
        "latest_regime_path": _optional_payload_field(feature_ctx.regime_payload, "latest_regime_path"),
        "regime_snapshot_path": _optional_payload_field(feature_ctx.regime_payload, "regime_snapshot_path"),
        "regime_state": feature_ctx.regime_state,
        "offset_signals": offset_signals,
    }


def score_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    resolve_live_profile_spec_fn,
    get_active_bundle_selection_fn,
    resolve_model_bundle_dir_fn,
    read_model_bundle_manifest_fn,
    read_bundle_config_fn,
    supports_feature_set_fn,
    build_live_feature_frame_fn,
    score_bundle_offset_fn,
    resolve_live_blacklist_fn,
    apply_live_blacklist_fn,
    latest_nan_feature_columns_fn,
    feature_coverage_fn,
    extract_feature_snapshot_fn,
    iso_or_none_fn,
    persist_live_signal_snapshot_fn,
    utc_snapshot_label_fn,
) -> dict[str, object]:
    bundle = _resolve_bundle_resolution(
        cfg,
        target=target,
        feature_set=feature_set,
        resolve_live_profile_spec_fn=resolve_live_profile_spec_fn,
        get_active_bundle_selection_fn=get_active_bundle_selection_fn,
        resolve_model_bundle_dir_fn=resolve_model_bundle_dir_fn,
        read_model_bundle_manifest_fn=read_model_bundle_manifest_fn,
        supports_feature_set_fn=supports_feature_set_fn,
    )
    feature_ctx = _prepare_live_features_and_states(
        cfg,
        builder_feature_set=bundle.builder_feature_set,
        persist=persist,
        build_live_feature_frame_fn=build_live_feature_frame_fn,
    )
    offset_signals = _score_offset_signals(
        cfg,
        selected_target=bundle.selected_target,
        profile_spec=bundle.profile_spec,
        bundle_dir=bundle.bundle_dir,
        base_features=feature_ctx.base_features,
        read_bundle_config_fn=read_bundle_config_fn,
        resolve_live_blacklist_fn=resolve_live_blacklist_fn,
        apply_live_blacklist_fn=apply_live_blacklist_fn,
        score_bundle_offset_fn=score_bundle_offset_fn,
        feature_coverage_fn=feature_coverage_fn,
        latest_nan_feature_columns_fn=latest_nan_feature_columns_fn,
        extract_feature_snapshot_fn=extract_feature_snapshot_fn,
        iso_or_none_fn=iso_or_none_fn,
    )
    payload = _build_signal_payload(
        cfg,
        bundle=bundle,
        feature_ctx=feature_ctx,
        offset_signals=offset_signals,
        utc_snapshot_label_fn=utc_snapshot_label_fn,
        iso_or_none_fn=iso_or_none_fn,
    )
    if persist:
        paths = persist_live_signal_snapshot_fn(
            cfg,
            target=bundle.selected_target,
            snapshot_ts=payload["snapshot_ts"],
            payload=payload,
        )
        payload["latest_signal_path"] = str(paths["latest"])
        payload["snapshot_path"] = str(paths["snapshot"])
    return payload
