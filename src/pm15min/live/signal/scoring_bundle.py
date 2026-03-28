from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import threading
import time
from typing import Any

from ..liquidity import load_latest_liquidity_state_snapshot, summarize_liquidity_state
from ..regime import build_regime_state_snapshot, summarize_regime_state
from pm15min.research.config import ResearchConfig
from pm15min.research.features.builders import resolve_live_required_feature_columns


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
    timings_ms: dict[str, float]


_BUNDLE_RESOLUTION_CACHE: dict[tuple[str, str, int, str, str | None], tuple[float, BundleResolution]] = {}
_BUNDLE_RESOLUTION_CACHE_LOCK = threading.Lock()


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
    cache_ttl_seconds = _env_float("PM15MIN_LIVE_BUNDLE_CACHE_SEC", default=0.0)
    cache_key = (
        str(cfg.layout.rewrite.root),
        str(cfg.asset.slug),
        int(cfg.cycle_minutes),
        str(target or "direction").strip().lower(),
        None if feature_set is None else str(feature_set),
    )
    if cache_ttl_seconds > 0.0:
        cached = _load_cached_bundle_resolution(cache_key=cache_key)
        if cached is not None:
            return cached
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

    resolved = BundleResolution(
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
    if cache_ttl_seconds > 0.0:
        _store_cached_bundle_resolution(
            cache_key=cache_key,
            cache_ttl_seconds=cache_ttl_seconds,
            resolution=resolved,
        )
    return resolved


def prepare_live_features_and_states(
    cfg,
    *,
    builder_feature_set: str,
    active_offsets: tuple[int, ...] = (),
    persist: bool,
    build_live_feature_frame_fn,
    allow_preview_open_bar: bool = False,
) -> LiveFeatureContext:
    required_feature_columns = resolve_live_required_feature_columns(feature_set=builder_feature_set)
    feature_started = time.perf_counter()
    base_features = build_live_feature_frame_fn(
        cfg,
        feature_set=builder_feature_set,
        retain_offsets=active_offsets,
        allow_preview_open_bar=allow_preview_open_bar,
        required_feature_columns=required_feature_columns,
    )
    feature_elapsed_ms = _elapsed_ms(feature_started)
    if base_features.empty:
        raise ValueError(f"Live feature frame is empty for market={cfg.asset.slug}")

    liquidity_started = time.perf_counter()
    liquidity_payload = load_latest_liquidity_state_snapshot(
        rewrite_root=cfg.layout.rewrite.root,
        market=cfg.asset.slug,
        cycle=f"{int(cfg.cycle_minutes)}m",
        profile=cfg.profile,
    )
    liquidity_state = summarize_liquidity_state(liquidity_payload)
    liquidity_elapsed_ms = _elapsed_ms(liquidity_started)
    regime_started = time.perf_counter()
    regime_payload = build_regime_state_snapshot(
        cfg,
        features=base_features,
        liquidity_payload=liquidity_payload,
        persist=persist,
    )
    regime_state = summarize_regime_state(regime_payload)
    regime_elapsed_ms = _elapsed_ms(regime_started)

    return LiveFeatureContext(
        base_features=base_features,
        liquidity_payload=liquidity_payload,
        liquidity_state=liquidity_state,
        regime_payload=regime_payload,
        regime_state=regime_state,
        timings_ms={
            **{
                str(key): float(value)
                for key, value in dict(getattr(base_features, "attrs", {}).get("timings_ms") or {}).items()
                if value is not None
            },
            "feature_frame_stage_ms": feature_elapsed_ms,
            "liquidity_state_stage_ms": liquidity_elapsed_ms,
            "regime_state_stage_ms": regime_elapsed_ms,
        },
    )


def _load_cached_bundle_resolution(
    *,
    cache_key: tuple[str, str, int, str, str | None],
) -> BundleResolution | None:
    now_monotonic = time.monotonic()
    with _BUNDLE_RESOLUTION_CACHE_LOCK:
        cached = _BUNDLE_RESOLUTION_CACHE.get(cache_key)
        if cached is None:
            return None
        expires_at, resolution = cached
        if now_monotonic >= float(expires_at):
            _BUNDLE_RESOLUTION_CACHE.pop(cache_key, None)
            return None
        return resolution


def _store_cached_bundle_resolution(
    *,
    cache_key: tuple[str, str, int, str, str | None],
    cache_ttl_seconds: float,
    resolution: BundleResolution,
) -> None:
    with _BUNDLE_RESOLUTION_CACHE_LOCK:
        _BUNDLE_RESOLUTION_CACHE[cache_key] = (
            time.monotonic() + max(0.0, float(cache_ttl_seconds)),
            resolution,
        )


def _env_float(name: str, *, default: float) -> float:
    raw = os.getenv(name)
    if raw in (None, ""):
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _elapsed_ms(started_at: float) -> float:
    return round(max(0.0, (time.perf_counter() - float(started_at)) * 1000.0), 3)
