from __future__ import annotations

import json
from pathlib import Path

import pytest

from pm15min.research.bundles.active_registry import read_active_bundle_selection, write_active_bundle_selection
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


def test_read_active_bundle_selection_returns_none_for_invalid_json(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )
    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm", target="direction")
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection_path.write_text("{", encoding="utf-8")

    assert read_active_bundle_selection(cfg, profile="deep_otm", target="direction") is None


def test_write_active_bundle_selection_preserves_existing_file_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        target="direction",
        root=root,
    )
    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm", target="direction")

    write_active_bundle_selection(
        cfg,
        profile="deep_otm",
        target="direction",
        bundle_label="stable",
        bundle_dir=root / "research" / "stable",
        usage="live_current",
        source_run_dir="/tmp/source-stable",
    )
    original_payload = json.loads(selection_path.read_text(encoding="utf-8"))
    original_write_text = Path.write_text

    def _failing_write_text(self: Path, data: str, *args, **kwargs):
        if self.parent == selection_path.parent and self.name.startswith(selection_path.name):
            original_write_text(self, "{", *args, **kwargs)
            raise RuntimeError("simulated selection write failure")
        return original_write_text(self, data, *args, **kwargs)

    monkeypatch.setattr(Path, "write_text", _failing_write_text)

    with pytest.raises(RuntimeError, match="simulated selection write failure"):
        write_active_bundle_selection(
            cfg,
            profile="deep_otm",
            target="direction",
            bundle_label="candidate",
            bundle_dir=root / "research" / "candidate",
            usage="live_candidate",
            source_run_dir="/tmp/source-candidate",
        )

    assert read_active_bundle_selection(cfg, profile="deep_otm", target="direction") == original_payload
