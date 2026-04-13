from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pm5min.research.config import ResearchConfig
from pm5min.research.layout_helpers import normalize_target, slug_token
from pm5min.research.manifests import utc_manifest_timestamp


def active_bundle_selection_path(cfg: ResearchConfig, *, profile: str, target: str) -> Path:
    return cfg.layout.active_bundle_selection_path(profile=profile, target=target)


def read_active_bundle_selection(cfg: ResearchConfig, *, profile: str, target: str) -> dict[str, Any] | None:
    path = active_bundle_selection_path(cfg, profile=profile, target=target)
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_active_bundle_selection(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str,
    bundle_dir: str | Path,
    usage: str,
    source_run_dir: str | Path,
    notes: str | None = None,
    metadata: dict[str, Any] | None = None,
    activated_at: str | None = None,
) -> Path:
    path = active_bundle_selection_path(cfg, profile=profile, target=target)
    payload = {
        "market": cfg.asset.slug,
        "cycle": cfg.cycle,
        "profile": slug_token(profile),
        "target": normalize_target(target),
        "bundle_label": slug_token(bundle_label, default="planned"),
        "bundle_dir": str(Path(bundle_dir)),
        "source_run_dir": str(Path(source_run_dir)),
        "usage": str(usage),
        "activated_at": str(activated_at or utc_manifest_timestamp()),
        "notes": str(notes or ""),
        "metadata": dict(metadata or {}),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path


def resolve_active_bundle_dir(cfg: ResearchConfig, *, profile: str, target: str) -> Path | None:
    payload = read_active_bundle_selection(cfg, profile=profile, target=target)
    if not payload:
        return None

    bundle_dir_raw = payload.get("bundle_dir")
    if bundle_dir_raw:
        bundle_dir = Path(str(bundle_dir_raw))
        if bundle_dir.exists():
            return bundle_dir

    bundle_label = payload.get("bundle_label")
    if bundle_label:
        candidate = cfg.layout.model_bundle_dir(
            profile=str(payload.get("profile") or profile),
            target=str(payload.get("target") or target),
            bundle_label=str(bundle_label),
        )
        if candidate.exists():
            return candidate
    return None
