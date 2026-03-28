from __future__ import annotations

from pathlib import Path
import time

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
    timings_ms: dict[str, float],
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
        "timings_ms": timings_ms,
    }


def score_live_latest(
    cfg,
    *,
    target: str = "direction",
    feature_set: str | None = None,
    persist: bool = True,
    allow_preview_open_bar: bool = False,
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
    score_started = time.perf_counter()
    bundle_started = time.perf_counter()
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
    bundle_elapsed_ms = round(max(0.0, (time.perf_counter() - float(bundle_started)) * 1000.0), 3)
    feature_ctx_started = time.perf_counter()
    feature_ctx = _prepare_live_features_and_states(
        cfg,
        builder_feature_set=bundle.builder_feature_set,
        active_offsets=_bundle_offsets(bundle.bundle_dir),
        persist=persist,
        build_live_feature_frame_fn=build_live_feature_frame_fn,
        allow_preview_open_bar=allow_preview_open_bar,
    )
    feature_ctx_total_ms = round(max(0.0, (time.perf_counter() - float(feature_ctx_started)) * 1000.0), 3)
    offset_scoring_started = time.perf_counter()
    offset_signals, offset_scoring_timings = _score_offset_signals(
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
    offset_scoring_elapsed_ms = round(max(0.0, (time.perf_counter() - float(offset_scoring_started)) * 1000.0), 3)
    total_elapsed_ms = round(max(0.0, (time.perf_counter() - float(score_started)) * 1000.0), 3)
    payload = _build_signal_payload(
        cfg,
        bundle=bundle,
        feature_ctx=feature_ctx,
        offset_signals=offset_signals,
        timings_ms={
            "bundle_resolution_stage_ms": bundle_elapsed_ms,
            "feature_prepare_stage_ms": feature_ctx_total_ms,
            **{
                str(key): value
                for key, value in feature_ctx.timings_ms.items()
            },
            "offset_scoring_stage_ms": offset_scoring_elapsed_ms,
            **{
                str(key): value
                for key, value in offset_scoring_timings.items()
            },
            "signal_total_stage_ms": total_elapsed_ms,
        },
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


def _bundle_offsets(bundle_dir: Path) -> tuple[int, ...]:
    offsets: list[int] = []
    for path in sorted((bundle_dir / "offsets").glob("offset=*")):
        try:
            offsets.append(int(path.name.split("=", 1)[1]))
        except Exception:
            continue
    return tuple(sorted(set(offsets)))
