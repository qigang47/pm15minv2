from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..liquidity import load_latest_liquidity_state_snapshot, summarize_liquidity_state
from ..regime import build_regime_state_snapshot, summarize_regime_state
from pm15min.research.config import ResearchConfig


@dataclass(frozen=True)
class BundleResolution:
    selected_target: str
    profile_spec: Any
    research_cfg: ResearchConfig
    active_payload: dict[str, object]
    selection: dict[str, object]
    bundle_dir: Path
    bundle_manifest: Any
    manifest_feature_set: str
    builder_feature_set: str


@dataclass(frozen=True)
class LiveFeatureContext:
    base_features: Any
    liquidity_payload: dict[str, object] | None
    liquidity_state: dict[str, object]
    regime_payload: dict[str, object] | None
    regime_state: dict[str, object]


def normalize_feature_set(value: object) -> str:
    return str(value or "").strip().lower()


def resolve_builder_feature_set(
    *,
    requested_feature_set: str | None,
    manifest_feature_set: str,
    default_feature_set: str,
    supports_feature_set_fn,
) -> str:
    if requested_feature_set:
        return normalize_feature_set(requested_feature_set)
    if manifest_feature_set and supports_feature_set_fn(manifest_feature_set):
        return manifest_feature_set
    return default_feature_set


def resolve_bundle_resolution(
    cfg,
    *,
    target: str,
    feature_set: str | None,
    resolve_live_profile_spec_fn,
    get_active_bundle_selection_fn,
    resolve_model_bundle_dir_fn,
    read_model_bundle_manifest_fn,
    supports_feature_set_fn,
) -> BundleResolution:
    selected_target = str(target or "direction").strip().lower()
    profile_spec = resolve_live_profile_spec_fn(cfg.profile)
    research_cfg = ResearchConfig.build(
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
        source_surface="live",
        feature_set=feature_set or profile_spec.default_feature_set,
        target=selected_target,
        root=cfg.layout.rewrite.root,
    )
    active_payload = get_active_bundle_selection_fn(
        research_cfg,
        profile=cfg.profile,
        target=selected_target,
    )
    selection = active_payload.get("selection")
    if not selection:
        raise FileNotFoundError(
            f"No active bundle registered for market={cfg.asset.slug} profile={cfg.profile} target={selected_target}"
        )

    bundle_dir = resolve_model_bundle_dir_fn(
        research_cfg,
        profile=cfg.profile,
        target=selected_target,
        bundle_label=str(selection.get("bundle_label") or ""),
    )
    bundle_manifest = read_model_bundle_manifest_fn(bundle_dir)
    manifest_feature_set = normalize_feature_set(bundle_manifest.spec.get("feature_set"))
    builder_feature_set = resolve_builder_feature_set(
        requested_feature_set=feature_set,
        manifest_feature_set=manifest_feature_set,
        default_feature_set=profile_spec.default_feature_set,
        supports_feature_set_fn=supports_feature_set_fn,
    )

    return BundleResolution(
        selected_target=selected_target,
        profile_spec=profile_spec,
        research_cfg=research_cfg,
        active_payload=active_payload,
        selection=selection,
        bundle_dir=bundle_dir,
        bundle_manifest=bundle_manifest,
        manifest_feature_set=manifest_feature_set,
        builder_feature_set=builder_feature_set,
    )


def prepare_live_features_and_states(
    cfg,
    *,
    builder_feature_set: str,
    persist: bool,
    build_live_feature_frame_fn,
) -> LiveFeatureContext:
    base_features = build_live_feature_frame_fn(cfg, feature_set=builder_feature_set)
    if base_features.empty:
        raise ValueError(f"Live feature frame is empty for market={cfg.asset.slug}")

    liquidity_payload = load_latest_liquidity_state_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
    )
    liquidity_state = summarize_liquidity_state(liquidity_payload)
    regime_payload = build_regime_state_snapshot(
        cfg,
        features=base_features,
        liquidity_payload=liquidity_payload,
        persist=persist,
    )
    regime_state = summarize_regime_state(regime_payload)

    return LiveFeatureContext(
        base_features=base_features,
        liquidity_payload=liquidity_payload,
        liquidity_state=liquidity_state,
        regime_payload=regime_payload,
        regime_state=regime_state,
    )
