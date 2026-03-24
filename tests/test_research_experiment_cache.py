from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research.experiments.cache import ExperimentSharedCache
from pm15min.research.layout import ResearchLayout


def _load_cache(root: Path) -> ExperimentSharedCache:
    layout = ResearchLayout.discover(root=root)
    layout.ensure_base_dirs()
    return ExperimentSharedCache.load_for_storage(layout)


def test_experiment_shared_cache_persists_prepared_training_and_bundle_metadata(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache = _load_cache(root)

    training_run_dir = root / "research" / "training_runs" / "sol-train-a"
    bundle_dir = root / "research" / "model_bundles" / "sol-bundle-a"
    cache.remember_prepared_dataset(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        rewrite_root=str(root),
        source_suite_name="suite-a",
        source_run_label="run-a",
        source_run_dir=str(training_run_dir),
        prepared_at="2026-03-22T00:00:00Z",
    )
    cache.remember_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
        run_dir=str(training_run_dir),
        source_suite_name="suite-a",
        source_run_label="run-a",
        updated_at="2026-03-22T00:00:01Z",
    )
    cache.remember_bundle(
        market="sol",
        profile="deep_otm",
        target="direction",
        offsets=(7, 8),
        training_run_label="sol-train-a",
        bundle_dir=str(bundle_dir),
        source_suite_name="suite-a",
        source_run_label="run-a",
        updated_at="2026-03-22T00:00:02Z",
    )

    outputs = cache.save()
    reloaded = _load_cache(root)

    assert outputs["root"] == str(root / "research" / "experiments" / "cache")
    assert reloaded.has_prepared_dataset(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        rewrite_root=str(root),
    )
    assert reloaded.get_prepared_dataset(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        rewrite_root=str(root),
    ) == {
        "market": "sol",
        "cycle": "15m",
        "profile": "deep_otm",
        "feature_set": "deep_otm_v1",
        "label_set": "truth",
        "rewrite_root": str(root),
        "source_suite_name": "suite-a",
        "source_run_label": "run-a",
        "source_run_dir": str(training_run_dir),
        "prepared_at": "2026-03-22T00:00:00Z",
    }
    assert reloaded.get_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
    ) == {
        "cache_key": reloaded.get_training(
            market="sol",
            profile="deep_otm",
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="direction",
            window_label="2026-03-01_2026-03-01",
            offsets=(7, 8),
        )["cache_key"],
        "market": "sol",
        "profile": "deep_otm",
        "model_family": "deep_otm",
        "feature_set": "deep_otm_v1",
        "label_set": "truth",
        "target": "direction",
        "window": "2026-03-01_2026-03-01",
        "offsets": (7, 8),
        "run_label": "sol-train-a",
        "run_dir": str(training_run_dir),
        "source_suite_name": "suite-a",
        "source_run_label": "run-a",
        "updated_at": "2026-03-22T00:00:01Z",
    }
    assert reloaded.get_bundle(
        market="sol",
        profile="deep_otm",
        target="direction",
        offsets=(7, 8),
        training_run_label="sol-train-a",
    ) == {
        "cache_key": reloaded.get_bundle(
            market="sol",
            profile="deep_otm",
            target="direction",
            offsets=(7, 8),
            training_run_label="sol-train-a",
        )["cache_key"],
        "market": "sol",
        "profile": "deep_otm",
        "target": "direction",
        "offsets": (7, 8),
        "training_run_label": "sol-train-a",
        "bundle_label": "sol-bundle-a",
        "bundle_dir": str(bundle_dir),
        "source_suite_name": "suite-a",
        "source_run_label": "run-a",
        "updated_at": "2026-03-22T00:00:02Z",
    }
    manifest = json.loads((root / "research" / "experiments" / "cache" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "pm15min.research.experiments.cache.v1"
    assert manifest["counts"] == {"prepared_datasets": 1, "training_reuse": 1, "bundle_reuse": 1}


def test_experiment_shared_cache_save_merges_stale_writers(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache_a = _load_cache(root)
    cache_b = _load_cache(root)

    cache_a.remember_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
        run_dir=str(root / "research" / "training_runs" / "sol-train-a"),
    )
    cache_a.save()

    cache_b.remember_training(
        market="xrp",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="reversal",
        window_label="2026-03-02_2026-03-02",
        offsets=(9,),
        run_dir=str(root / "research" / "training_runs" / "xrp-train-a"),
    )
    cache_b.remember_bundle(
        market="xrp",
        profile="deep_otm",
        target="reversal",
        offsets=(9,),
        training_run_label="xrp-train-a",
        bundle_dir=str(root / "research" / "model_bundles" / "xrp-bundle-a"),
    )
    cache_b.save()

    reloaded = _load_cache(root)

    assert reloaded.counts() == {"prepared_datasets": 0, "training_reuse": 2, "bundle_reuse": 1}
    assert reloaded.get_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
    )["run_label"] == "sol-train-a"
    assert reloaded.get_training(
        market="xrp",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="reversal",
        window_label="2026-03-02_2026-03-02",
        offsets=(9,),
    )["run_label"] == "xrp-train-a"
    assert reloaded.get_bundle(
        market="xrp",
        profile="deep_otm",
        target="reversal",
        offsets=(9,),
        training_run_label="xrp-train-a",
    )["bundle_label"] == "xrp-bundle-a"


def test_experiment_shared_cache_ingests_training_runs_metadata(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache = _load_cache(root)
    training_runs = pd.DataFrame(
        [
            {
                "case_key": "baseline",
                "market": "sol",
                "profile": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "model_family": "deep_otm",
                "target": "direction",
                "window": "2026-03-01_2026-03-01",
                "offsets": [7, 8],
                "training_run_dir": str(root / "research" / "training_runs" / "sol-train-a"),
                "bundle_dir": str(root / "research" / "model_bundles" / "sol-bundle-a"),
            },
            {
                "case_key": "alt",
                "market": "sol",
                "profile": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "model_family": "deep_otm",
                "target": "direction",
                "window": "2026-03-01_2026-03-01",
                "offsets": [7, 8],
                "training_run_dir": str(root / "research" / "training_runs" / "sol-train-b"),
                "bundle_dir": str(root / "research" / "model_bundles" / "sol-bundle-b"),
            },
            {
                "case_key": "resumed-only",
                "market": "xrp",
                "profile": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "model_family": "deep_otm",
                "target": "reversal",
                "window": "2026-03-02_2026-03-02",
                "offsets": [9],
                "training_run_dir": None,
                "bundle_dir": str(root / "research" / "model_bundles" / "xrp-bundle-a"),
            },
        ]
    )

    cache.ingest_training_runs(
        training_runs,
        cycle="15m",
        rewrite_root=str(root),
        source_suite_name="suite-a",
        source_run_label="run-a",
    )
    cache.save()
    reloaded = _load_cache(root)

    assert reloaded.counts() == {"prepared_datasets": 2, "training_reuse": 1, "bundle_reuse": 1}
    assert reloaded.has_prepared_dataset(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        rewrite_root=str(root),
    )
    assert reloaded.has_prepared_dataset(
        market="xrp",
        cycle="15m",
        profile="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        rewrite_root=str(root),
    )
    assert reloaded.get_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
    )["run_label"] == "sol-train-b"
    assert reloaded.get_bundle(
        market="sol",
        profile="deep_otm",
        target="direction",
        offsets=(7, 8),
        training_run_label="sol-train-b",
    )["bundle_label"] == "sol-bundle-b"
    assert (
        reloaded.get_training(
            market="xrp",
            profile="deep_otm",
            model_family="deep_otm",
            feature_set="deep_otm_v1",
            label_set="truth",
            target="reversal",
            window_label="2026-03-02_2026-03-02",
            offsets=(9,),
        )
        is None
    )
