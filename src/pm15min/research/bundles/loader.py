from __future__ import annotations

import json
from pathlib import Path

from pm15min.research.bundles.active_registry import resolve_active_bundle_dir
from pm15min.research.config import ResearchConfig
from pm15min.research.manifests import read_manifest


def resolve_training_run_dir(
    cfg: ResearchConfig,
    *,
    target: str,
    source_training_run: str | None = None,
) -> Path:
    root = cfg.layout.training_runs_root / f"model_family={cfg.model_family}" / f"target={target}"
    if source_training_run:
        direct = root / f"run={source_training_run}"
        if direct.exists():
            return direct
        candidate = Path(source_training_run)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Training run not found: {source_training_run}")

    candidates = sorted(
        [path for path in root.glob("run=*") if path.is_dir()],
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )
    if not candidates:
        raise FileNotFoundError(f"No training runs available under {root}")
    return candidates[-1]


def resolve_model_bundle_dir(
    cfg: ResearchConfig,
    *,
    profile: str,
    target: str,
    bundle_label: str | None = None,
) -> Path:
    root = cfg.layout.model_bundles_root / f"profile={profile}" / f"target={target}"
    if bundle_label:
        direct = root / f"bundle={bundle_label}"
        if direct.exists():
            return direct
        active = resolve_active_bundle_dir(cfg, profile=profile, target=target)
        if active is not None:
            active_label = active.name.split("=", 1)[-1] if "=" in active.name else active.name
            if active_label == str(bundle_label):
                return active
        candidate = Path(bundle_label)
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"Model bundle not found: {bundle_label}")
    active = resolve_active_bundle_dir(cfg, profile=profile, target=target)
    if active is not None:
        return active
    candidates = sorted(
        [path for path in root.glob("bundle=*") if path.is_dir()],
        key=lambda path: (path.stat().st_mtime_ns, path.name),
    )
    if not candidates:
        raise FileNotFoundError(f"No model bundles available under {root}")
    return candidates[-1]


def read_model_bundle_manifest(bundle_dir: Path):
    return read_manifest(bundle_dir / "manifest.json")


def read_bundle_config(bundle_dir: Path, *, offset: int) -> dict[str, object]:
    path = bundle_dir / "offsets" / f"offset={int(offset)}" / "bundle_config.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing bundle config: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_bundle_summary(bundle_dir: Path) -> dict[str, object]:
    path = bundle_dir / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing bundle summary: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_training_run_summary(run_dir: Path) -> dict[str, object]:
    path = run_dir / "summary.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing training run summary: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def read_training_run_manifest(run_dir: Path):
    return read_manifest(run_dir / "manifest.json")
