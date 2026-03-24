from __future__ import annotations

from pathlib import Path

from pm15min.research.bundles.active_registry import write_active_bundle_selection
from pm15min.research.bundles.loader import resolve_model_bundle_dir
from pm15min.research.config import ResearchConfig
from pm15min.research.manifests import build_manifest, write_manifest
from pm15min.research.service import activate_model_bundle


def test_resolve_model_bundle_dir_prefers_active_selection(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )

    bundle_old = cfg.layout.bundle_dir(
        profile="deep_otm",
        target="direction",
        bundle_label_text="older",
    )
    bundle_new = cfg.layout.bundle_dir(
        profile="deep_otm",
        target="direction",
        bundle_label_text="newer",
    )
    bundle_old.mkdir(parents=True, exist_ok=True)
    bundle_new.mkdir(parents=True, exist_ok=True)

    write_active_bundle_selection(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="older",
        bundle_dir=bundle_old,
        usage="live_current",
        source_run_dir="/tmp/source-old",
    )

    resolved = resolve_model_bundle_dir(cfg, profile="deep_otm", target="direction", bundle_label=None)
    assert resolved == bundle_old


def test_resolve_model_bundle_dir_falls_back_to_latest_without_active_selection(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )
    bundle_old = cfg.layout.bundle_dir(
        profile="deep_otm",
        target="direction",
        bundle_label_text="older",
    )
    bundle_new = cfg.layout.bundle_dir(
        profile="deep_otm",
        target="direction",
        bundle_label_text="newer",
    )
    bundle_old.mkdir(parents=True, exist_ok=True)
    bundle_new.mkdir(parents=True, exist_ok=True)

    resolved = resolve_model_bundle_dir(cfg, profile="deep_otm", target="direction", bundle_label=None)
    assert resolved == bundle_new


def test_activate_model_bundle_writes_selection_from_manifest(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )

    bundle_dir = cfg.layout.bundle_dir(
        profile="deep_otm",
        target="direction",
        bundle_label_text="candidate",
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(
        object_type="model_bundle",
        object_id="model_bundle:deep_otm:direction:candidate",
        market="sol",
        cycle="15m",
        path=bundle_dir,
        spec={
            "bundle_label": "candidate",
            "source_training_run": "/tmp/source-run",
            "usage": "live_current",
            "offsets": [7, 8, 9],
            "notes": "candidate bundle",
        },
    )
    write_manifest(bundle_dir / "manifest.json", manifest)

    payload = activate_model_bundle(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="candidate",
        notes="promoted",
    )

    assert payload["bundle_dir"] == str(bundle_dir)
    assert payload["selection"]["bundle_label"] == "candidate"
    assert payload["selection"]["notes"] == "promoted"
    assert payload["selection"]["usage"] == "live_current"


def test_resolve_model_bundle_dir_uses_active_selection_dir_when_bundle_label_is_cross_profile(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )

    baseline_bundle = cfg.layout.bundle_dir(
        profile="deep_otm_baseline",
        target="direction",
        bundle_label_text="baseline_bundle",
    )
    baseline_bundle.mkdir(parents=True, exist_ok=True)

    write_active_bundle_selection(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="baseline_bundle",
        bundle_dir=baseline_bundle,
        usage="baseline_reference_on_deep_otm_guard_stack",
        source_run_dir="/tmp/source-baseline",
    )

    resolved = resolve_model_bundle_dir(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="baseline_bundle",
    )
    assert resolved == baseline_bundle
