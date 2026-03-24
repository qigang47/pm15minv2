from __future__ import annotations

import json
from pathlib import Path

import joblib

from pm15min.research.bundles.builder import build_model_bundle
from pm15min.research.config import ResearchConfig
from pm15min.research.contracts import ModelBundleSpec
from pm15min.research.manifests import build_manifest, write_manifest


def test_build_model_bundle_copies_optional_training_artifacts(tmp_path) -> None:
    root = tmp_path / "v2"
    cfg = ResearchConfig.build(
        market="sol",
        cycle="15m",
        profile="deep_otm",
        source_surface="backtest",
        feature_set="deep_otm_v1",
        label_set="truth",
        target="direction",
        model_family="deep_otm",
        root=root,
    )
    training_run_dir = cfg.layout.training_run_dir(model_family="deep_otm", target="direction", run_label_text="source")
    offset_dir = training_run_dir / "offsets" / "offset=7"
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)
    (offset_dir / "reports").mkdir(parents=True, exist_ok=True)

    (offset_dir / "feature_schema.json").write_text('[{"name": "ret_1m", "dtype": "float64"}]', encoding="utf-8")
    joblib.dump(["ret_1m"], offset_dir / "feature_cols.joblib")
    joblib.dump({"kind": "dummy"}, offset_dir / "models" / "lgbm_sigmoid.joblib")
    joblib.dump({"kind": "dummy"}, offset_dir / "models" / "logreg_sigmoid.joblib")
    (offset_dir / "calibration" / "blend_weights.json").write_text('{"w_lgb": 0.5, "w_lr": 0.5}', encoding="utf-8")
    (offset_dir / "calibration" / "reliability_bins_blend.json").write_text("[]", encoding="utf-8")
    (offset_dir / "metrics.json").write_text(
        '{"offset": 7, "rows": 16, "positive_rate": 0.5, "metrics": {"blend": {"brier": 0.2, "auc": 0.7}}}',
        encoding="utf-8",
    )
    (offset_dir / "feature_pruning.json").write_text('{"dropped_columns": ["ma_gap_15"]}', encoding="utf-8")
    (offset_dir / "logreg_coefficients.json").write_text('{"rows": [{"feature": "ret_1m", "coefficient": 0.4}]}', encoding="utf-8")
    (offset_dir / "lgb_feature_importance.json").write_text(
        '{"rows": [{"feature": "ret_1m", "gain_importance": 12.0, "split_importance": 3}]}',
        encoding="utf-8",
    )
    (offset_dir / "factor_direction_summary.json").write_text(
        '{"rows": [{"feature": "ret_1m", "direction": "positive"}]}',
        encoding="utf-8",
    )
    (offset_dir / "factor_correlations.parquet").write_text("placeholder", encoding="utf-8")
    (offset_dir / "probe.json").write_text('{"probe_rows": 16}', encoding="utf-8")
    (offset_dir / "reports" / "offset_report.md").write_text("# Offset Report", encoding="utf-8")

    write_manifest(
        training_run_dir / "manifest.json",
        build_manifest(
            object_type="training_run",
            object_id="training_run:deep_otm:direction:source",
            market="sol",
            cycle="15m",
            path=training_run_dir,
            spec={
                "model_family": "deep_otm",
                "feature_set": "deep_otm_v1",
                "label_set": "truth",
                "target": "direction",
                "window": {"start": "2026-03-01", "end": "2026-03-01", "label": "2026-03-01_2026-03-01"},
            },
        ),
    )
    (training_run_dir / "summary.json").write_text(
        json.dumps(
            {
                "offsets": [7],
                "offset_summaries": [
                    {
                        "offset": 7,
                        "rows": 16,
                        "positive_rate": 0.5,
                        "brier_blend": 0.2,
                        "auc_blend": 0.7,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    summary = build_model_bundle(
        cfg,
        ModelBundleSpec(
            profile="deep_otm",
            target="direction",
            bundle_label="smoke",
            offsets=(7,),
            source_training_run="source",
        ),
    )
    bundle_dir = Path(summary["bundle_dir"])
    bundle_config = json.loads((bundle_dir / "offsets" / "offset=7" / "bundle_config.json").read_text(encoding="utf-8"))

    assert (bundle_dir / "summary.json").exists()
    assert (bundle_dir / "report.md").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "diagnostics" / "feature_pruning.json").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "diagnostics" / "logreg_coefficients.json").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "diagnostics" / "lgb_feature_importance.json").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "diagnostics" / "factor_direction_summary.json").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "diagnostics" / "factor_correlations.parquet").exists()
    assert (bundle_dir / "offsets" / "offset=7" / "reports" / "offset_report.md").exists()
    assert bundle_config["allowed_blacklist_columns"] == ["ma_gap_15"]
