from __future__ import annotations

import json
from pathlib import Path

from pm5min.research.config import ResearchConfig
from pm5min.research.service import (
    activate_model_bundle,
    get_active_bundle_selection,
    list_model_bundles,
    list_training_runs,
)


def _build_cfg(tmp_path: Path) -> ResearchConfig:
    return ResearchConfig.build(
        market="sol",
        cycle="5m",
        profile="deep_otm_5m",
        target="direction",
        model_family="deep_otm",
        root=tmp_path / "v2",
    )


def _forbid_pm15min_research_service(monkeypatch, attribute: str) -> None:
    def _raise_if_called(*args, **kwargs):
        raise AssertionError(f"pm5min research service delegated to pm15min.research.service.{attribute}")

    monkeypatch.setattr(f"pm15min.research.service.{attribute}", _raise_if_called)


def test_research_service_list_training_runs_reads_local_5m_registry(monkeypatch, tmp_path: Path) -> None:
    _forbid_pm15min_research_service(monkeypatch, "list_training_runs")
    cfg = _build_cfg(tmp_path)

    run_dir = cfg.layout.training_run_dir(
        model_family="deep_otm",
        target="direction",
        run_label="demo-local-run",
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    wrong_cycle = (
        cfg.layout.storage.training_runs_root
        / "cycle=15m"
        / "asset=sol"
        / "model_family=deep_otm"
        / "target=direction"
        / "run=wrong-cycle-run"
    )
    wrong_cycle.mkdir(parents=True, exist_ok=True)

    rows = list_training_runs(cfg)

    assert rows == [
        {
            "object_type": "training_run",
            "name": "run=demo-local-run",
            "path": str(run_dir),
            "updated_at": rows[0]["updated_at"],
            "cycle": "5m",
            "asset": "sol",
            "model_family": "deep_otm",
            "target": "direction",
            "run": "demo-local-run",
        }
    ]


def test_research_service_list_model_bundles_reads_local_5m_registry(monkeypatch, tmp_path: Path) -> None:
    _forbid_pm15min_research_service(monkeypatch, "list_model_bundles")
    cfg = _build_cfg(tmp_path)

    bundle_dir = cfg.layout.model_bundle_dir(
        profile="deep_otm_5m",
        target="direction",
        bundle_label="candidate-local-bundle",
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)

    wrong_cycle = (
        cfg.layout.storage.model_bundles_root
        / "cycle=15m"
        / "asset=sol"
        / "profile=deep_otm_5m"
        / "target=direction"
        / "bundle=wrong-cycle-bundle"
    )
    wrong_cycle.mkdir(parents=True, exist_ok=True)

    rows = list_model_bundles(cfg)

    assert rows == [
        {
            "object_type": "model_bundle",
            "name": "bundle=candidate-local-bundle",
            "path": str(bundle_dir),
            "updated_at": rows[0]["updated_at"],
            "cycle": "5m",
            "asset": "sol",
            "profile": "deep_otm_5m",
            "target": "direction",
            "bundle": "candidate-local-bundle",
        }
    ]


def test_research_service_get_active_bundle_selection_reads_local_selection(monkeypatch, tmp_path: Path) -> None:
    _forbid_pm15min_research_service(monkeypatch, "get_active_bundle_selection")
    cfg = _build_cfg(tmp_path)

    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm_5m", target="direction")
    selection_path.parent.mkdir(parents=True, exist_ok=True)
    selection = {
        "market": "sol",
        "cycle": "5m",
        "profile": "deep_otm_5m",
        "target": "direction",
        "bundle_label": "candidate-local-bundle",
        "bundle_dir": str(
            cfg.layout.model_bundle_dir(
                profile="deep_otm_5m",
                target="direction",
                bundle_label="candidate-local-bundle",
            )
        ),
        "source_run_dir": str(
            cfg.layout.training_run_dir(
                model_family="deep_otm",
                target="direction",
                run_label="demo-local-run",
            )
        ),
        "usage": "live_current",
        "activated_at": "2026-04-13T00:00:00Z",
        "notes": "promoted locally",
        "metadata": {"offsets": [7, 8, 9]},
    }
    selection_path.write_text(json.dumps(selection), encoding="utf-8")

    payload = get_active_bundle_selection(cfg)

    assert payload == {
        "market": "sol",
        "cycle": "5m",
        "profile": "deep_otm_5m",
        "target": "direction",
        "selection_path": str(selection_path),
        "selection": selection,
    }


def test_research_service_activate_model_bundle_writes_local_selection(monkeypatch, tmp_path: Path) -> None:
    _forbid_pm15min_research_service(monkeypatch, "activate_model_bundle")
    cfg = _build_cfg(tmp_path)

    bundle_dir = cfg.layout.model_bundle_dir(
        profile="deep_otm_5m",
        target="direction",
        bundle_label="candidate-local-bundle",
    )
    bundle_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = bundle_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "object_type": "model_bundle",
                "object_id": "model_bundle:deep_otm_5m:direction:candidate-local-bundle",
                "market": "sol",
                "cycle": "5m",
                "path": str(bundle_dir),
                "created_at": "2026-04-13T00:00:00Z",
                "spec": {
                    "bundle_label": "candidate-local-bundle",
                    "source_training_run": str(
                        cfg.layout.training_run_dir(
                            model_family="deep_otm",
                            target="direction",
                            run_label="demo-local-run",
                        )
                    ),
                    "usage": "live_current",
                    "offsets": [7, 8, 9],
                    "feature_set": "deep_otm_v1",
                    "label_set": "truth",
                    "source_group": "pm5min_local_registry",
                    "notes": "candidate bundle notes",
                },
                "inputs": [],
                "outputs": [],
                "metadata": {
                    "allowed_blacklist_columns": ["funding_rate"],
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    payload = activate_model_bundle(
        cfg,
        profile="deep_otm_5m",
        target="direction",
        bundle_label="candidate-local-bundle",
        notes="promoted locally",
    )

    selection_path = cfg.layout.active_bundle_selection_path(profile="deep_otm_5m", target="direction")
    selection = json.loads(selection_path.read_text(encoding="utf-8"))

    assert payload == {
        "market": "sol",
        "cycle": "5m",
        "profile": "deep_otm_5m",
        "target": "direction",
        "bundle_dir": str(bundle_dir),
        "selection_path": str(selection_path),
        "bundle_manifest_path": str(manifest_path),
        "selection": selection,
    }
    assert selection["bundle_label"] == "candidate-local-bundle"
    assert selection["bundle_dir"] == str(bundle_dir)
    assert selection["usage"] == "live_current"
    assert selection["notes"] == "promoted locally"
    assert selection["metadata"]["offsets"] == [7, 8, 9]
    assert selection["metadata"]["feature_set"] == "deep_otm_v1"
    assert selection["metadata"]["label_set"] == "truth"
    assert selection["metadata"]["source_group"] == "pm5min_local_registry"
    assert selection["metadata"]["allowed_blacklist_columns"] == ["funding_rate"]
    assert selection["metadata"]["activated_from_bundle_manifest"] == str(manifest_path)
