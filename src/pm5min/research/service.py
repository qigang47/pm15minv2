from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pm5min.research.bundles.active_registry import read_active_bundle_selection, write_active_bundle_selection
from pm5min.research.bundles.loader import read_model_bundle_manifest, resolve_model_bundle_dir
from pm5min.research.config import ResearchConfig


def describe_research_runtime(cfg) -> dict[str, object]:
    return {
        "domain": "research",
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": cfg.profile,
        "source_surface": cfg.source_surface,
        "feature_set": cfg.feature_set,
        "label_set": cfg.label_set,
        "target": cfg.target,
        "model_family": cfg.model_family,
        "run_prefix": cfg.run_prefix,
        "layout": cfg.layout.to_dict(),
        "research_root": str(cfg.layout.storage.research_root),
        "market_training_runs_root": str(cfg.layout.training_runs_root),
        "market_model_bundles_root": str(cfg.layout.model_bundles_root),
        "market_active_bundles_root": str(cfg.layout.active_bundles_root),
        "active_bundle_selection_path": str(
            cfg.layout.active_bundle_selection_path(profile=cfg.profile, target=cfg.target)
        ),
        "suite_specs_root": str(cfg.layout.storage.suite_specs_root),
        "evaluations_root": str(cfg.layout.storage.evaluations_root),
    }


def get_active_bundle_selection(
    cfg: ResearchConfig,
    *,
    profile: str | None = None,
    target: str | None = None,
) -> dict[str, object]:
    selected_profile = str(profile or cfg.profile)
    selected_target = str(target or cfg.target)
    payload = read_active_bundle_selection(cfg, profile=selected_profile, target=selected_target)
    return {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": selected_profile,
        "target": selected_target,
        "selection_path": str(
            cfg.layout.active_bundle_selection_path(profile=selected_profile, target=selected_target)
        ),
        "selection": payload,
    }


def list_training_runs(
    cfg: ResearchConfig,
    *,
    model_family: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
) -> list[dict[str, object]]:
    root = cfg.layout.training_runs_root
    if not root.exists():
        return []

    runs = [path for path in root.glob("model_family=*/target=*/run=*") if path.is_dir()]
    if model_family:
        runs = [path for path in runs if path.parent.parent.name == f"model_family={model_family}"]
    if target:
        runs = [path for path in runs if path.parent.name == f"target={target}"]
    if prefix:
        runs = [path for path in runs if path.name.startswith(f"run={prefix}")]
    runs = sorted(runs, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return [_dir_row(path, object_kind="training_run") for path in runs]


def list_model_bundles(
    cfg: ResearchConfig,
    *,
    profile: str | None = None,
    target: str | None = None,
    prefix: str | None = None,
) -> list[dict[str, object]]:
    root = cfg.layout.model_bundles_root
    if not root.exists():
        return []

    bundles = [path for path in root.glob("profile=*/target=*/bundle=*") if path.is_dir()]
    if profile:
        bundles = [path for path in bundles if path.parent.parent.name == f"profile={profile}"]
    if target:
        bundles = [path for path in bundles if path.parent.name == f"target={target}"]
    if prefix:
        bundles = [path for path in bundles if path.name.startswith(f"bundle={prefix}")]
    bundles = sorted(bundles, key=lambda path: (path.stat().st_mtime_ns, path.name))
    return [_dir_row(path, object_kind="model_bundle") for path in bundles]


def activate_model_bundle(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str | None = None,
    notes: str | None = None,
) -> dict[str, object]:
    bundle_dir = resolve_model_bundle_dir(
        cfg,
        profile=profile,
        target=target,
        bundle_label=bundle_label,
    )
    manifest = read_model_bundle_manifest(bundle_dir)
    selection_path = write_active_bundle_selection(
        cfg,
        profile=profile,
        target=target,
        bundle_label=str(manifest.spec.get("bundle_label") or bundle_dir.name.split("=", 1)[-1]),
        bundle_dir=bundle_dir,
        usage=str(manifest.spec.get("usage") or "manual_activation"),
        source_run_dir=str(manifest.spec.get("source_training_run") or ""),
        notes=notes or str(manifest.spec.get("notes") or ""),
        metadata={
            "offsets": list(manifest.spec.get("offsets") or []),
            "feature_set": manifest.spec.get("feature_set"),
            "label_set": manifest.spec.get("label_set"),
            "source_group": manifest.spec.get("source_group"),
            "allowed_blacklist_columns": list(manifest.metadata.get("allowed_blacklist_columns") or []),
            "activated_from_bundle_manifest": str(bundle_dir / "manifest.json"),
        },
    )
    return {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": profile,
        "target": target,
        "bundle_dir": str(bundle_dir),
        "selection_path": str(selection_path),
        "bundle_manifest_path": str(bundle_dir / "manifest.json"),
        "selection": read_active_bundle_selection(cfg, profile=profile, target=target),
    }


def _dir_row(path: Path, *, object_kind: str) -> dict[str, str]:
    updated = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    row = {
        "object_type": object_kind,
        "name": path.name,
        "path": str(path),
        "updated_at": updated,
    }
    for part in path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        row[key] = value
    return row
