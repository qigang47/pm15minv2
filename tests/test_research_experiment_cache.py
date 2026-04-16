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
        "weight_variant_label": "default",
        "balance_classes": None,
        "weight_by_vol": None,
        "inverse_vol": None,
        "contrarian_weight": None,
        "contrarian_quantile": None,
        "contrarian_return_col": None,
        "winner_in_band_weight": None,
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


def test_experiment_shared_cache_normalizes_partitioned_run_and_bundle_dir_names(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache = _load_cache(root)

    training_run_dir = root / "research" / "training_runs" / "cycle=15m" / "asset=sol" / "model_family=deep_otm" / "target=direction" / "run=sol-train-a"
    bundle_dir = root / "research" / "model_bundles" / "cycle=15m" / "asset=sol" / "profile=deep_otm" / "target=direction" / "bundle=sol-bundle-a"
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
    )
    cache.remember_bundle(
        market="sol",
        profile="deep_otm",
        target="direction",
        offsets=(7, 8),
        training_run_label="sol-train-a",
        bundle_dir=str(bundle_dir),
    )
    cache.save()

    reloaded = _load_cache(root)

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
    assert reloaded.get_bundle(
        market="sol",
        profile="deep_otm",
        target="direction",
        offsets=(7, 8),
        training_run_label="sol-train-a",
    )["bundle_label"] == "sol-bundle-a"


def test_experiment_shared_cache_separates_training_reuse_for_weight_variants(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache = _load_cache(root)

    default_run_dir = root / "research" / "training_runs" / "cycle=15m" / "asset=sol" / "model_family=deep_otm" / "target=direction" / "run=sol-train-default"
    novol_run_dir = root / "research" / "training_runs" / "cycle=15m" / "asset=sol" / "model_family=deep_otm" / "target=direction" / "run=sol-train-novol"
    cache.remember_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
        run_dir=str(default_run_dir),
        weight_variant_label="current_default",
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
        run_dir=str(novol_run_dir),
        weight_variant_label="no_vol_weight",
        weight_by_vol=False,
    )
    cache.save()

    reloaded = _load_cache(root)

    assert reloaded.counts()["training_reuse"] == 2
    assert reloaded.get_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
        weight_variant_label="current_default",
    )["run_label"] == "sol-train-default"
    assert reloaded.get_training(
        market="sol",
        profile="deep_otm",
        model_family="deep_otm",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        window_label="2026-03-01_2026-03-01",
        offsets=(7, 8),
        weight_variant_label="no_vol_weight",
        weight_by_vol=False,
    )["run_label"] == "sol-train-novol"


def test_experiment_shared_cache_separates_training_reuse_for_offset_weight_overrides(tmp_path: Path) -> None:
    root = tmp_path / "v2"
    cache = _load_cache(root)

    mild_run_dir = root / "research" / "training_runs" / "cycle=15m" / "asset=btc" / "model_family=deep_otm" / "target=direction" / "run=btc-train-offset-mild"
    strong_run_dir = root / "research" / "training_runs" / "cycle=15m" / "asset=btc" / "model_family=deep_otm" / "target=direction" / "run=btc-train-offset-strong"
    mild_overrides = {
        7: {"contrarian_weight": 1.25, "contrarian_quantile": 0.8},
        8: {"contrarian_weight": 1.5, "contrarian_quantile": 0.75},
        9: {"contrarian_weight": 1.75, "contrarian_quantile": 0.7},
    }
    strong_overrides = {
        7: {"contrarian_weight": 1.5, "contrarian_quantile": 0.8},
        8: {"contrarian_weight": 2.0, "contrarian_quantile": 0.75},
        9: {"contrarian_weight": 2.5, "contrarian_quantile": 0.7},
    }
    cache.remember_training(
        market="btc",
        profile="deep_otm_baseline",
        model_family="deep_otm",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        window_label="2025-10-27_2026-03-27",
        offsets=(7, 8, 9),
        run_dir=str(mild_run_dir),
        weight_variant_label="direction_offset_reversal_mild",
        offset_weight_overrides=mild_overrides,
    )
    cache.remember_training(
        market="btc",
        profile="deep_otm_baseline",
        model_family="deep_otm",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        window_label="2025-10-27_2026-03-27",
        offsets=(7, 8, 9),
        run_dir=str(strong_run_dir),
        weight_variant_label="direction_offset_reversal_strong",
        offset_weight_overrides=strong_overrides,
    )
    cache.save()

    reloaded = _load_cache(root)

    assert reloaded.counts()["training_reuse"] == 2
    assert reloaded.get_training(
        market="btc",
        profile="deep_otm_baseline",
        model_family="deep_otm",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        window_label="2025-10-27_2026-03-27",
        offsets=(7, 8, 9),
        weight_variant_label="direction_offset_reversal_mild",
        offset_weight_overrides=mild_overrides,
    )["run_label"] == "btc-train-offset-mild"
    assert reloaded.get_training(
        market="btc",
        profile="deep_otm_baseline",
        model_family="deep_otm",
        feature_set="bs_q_replace_direction",
        label_set="truth",
        target="direction",
        window_label="2025-10-27_2026-03-27",
        offsets=(7, 8, 9),
        weight_variant_label="direction_offset_reversal_strong",
        offset_weight_overrides=strong_overrides,
    )["run_label"] == "btc-train-offset-strong"
