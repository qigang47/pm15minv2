from __future__ import annotations

import json
import os
import time

import joblib
import numpy as np
import pandas as pd
import pytest

import pm15min.research.inference.scorer as scorer_module
from pm15min.research.inference.scorer import load_offset_model_context, score_bundle_offset


class FixedProbabilityModel:
    def __init__(self, probability: float) -> None:
        self.probability = float(probability)

    def predict_proba(self, X):
        return np.asarray([[1.0 - self.probability, self.probability] for _ in range(len(X))], dtype=float)


class FakeLogregClassifier:
    coef_ = np.asarray([[0.4, -0.3]], dtype=float)
    intercept_ = np.asarray([0.1], dtype=float)


class FakeLogregPipeline:
    named_steps = {"clf": FakeLogregClassifier()}


class FakeBooster:
    def feature_importance(self, importance_type="gain"):
        if importance_type == "gain":
            return np.asarray([10.0, 5.0], dtype=float)
        return np.asarray([4, 2], dtype=float)


class FakeLgbModel:
    booster_ = FakeBooster()


class FakeCalibratedMember:
    def __init__(self, estimator) -> None:
        self.estimator = estimator


class FakeCalibratedModel:
    def __init__(self, estimator) -> None:
        self.calibrated_classifiers_ = [FakeCalibratedMember(estimator)]


def test_score_bundle_offset_uses_conservative_reliability_probabilities(tmp_path) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)

    (offset_dir / "bundle_config.json").write_text(
        json.dumps(
            {
                "feature_columns": ["ret_1m"],
                "signal_target": "direction",
                "allowed_blacklist_columns": [],
                "missing_feature_fill_value": 0.0,
            }
        ),
        encoding="utf-8",
    )
    joblib.dump(FixedProbabilityModel(0.90), offset_dir / "models" / "lgbm_sigmoid.joblib")
    joblib.dump(FixedProbabilityModel(0.70), offset_dir / "models" / "logreg_sigmoid.joblib")
    (offset_dir / "calibration" / "blend_weights.json").write_text(
        json.dumps({"w_lgb": 0.5, "w_lr": 0.5}),
        encoding="utf-8",
    )
    (offset_dir / "calibration" / "reliability_bins_blend.json").write_text(
        json.dumps(
            [
                {
                    "left": 0.75,
                    "right": 0.85,
                    "n": 100,
                    "predicted_mean": 0.80,
                    "observed_rate": 0.74,
                    "gap": -0.06,
                    "alpha": 0.05,
                    "positives": 74,
                    "lower": 0.65,
                    "upper": 0.81,
                }
            ]
        ),
        encoding="utf-8",
    )

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "cycle_start_ts": "2026-03-20T00:00:00+00:00",
                "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                "offset": 7,
                "ret_1m": 0.01,
            }
        ]
    )

    out = score_bundle_offset(bundle_dir, features, offset=7)

    assert len(out) == 1
    assert out.iloc[0]["p_signal"] == pytest.approx(0.8)
    assert out.iloc[0]["p_lgb"] == pytest.approx(0.9)
    assert out.iloc[0]["p_lr"] == pytest.approx(0.7)
    assert out.iloc[0]["w_lgb"] == pytest.approx(0.5)
    assert out.iloc[0]["w_lr"] == pytest.approx(0.5)
    assert out.iloc[0]["p_up_raw"] == pytest.approx(0.8)
    assert out.iloc[0]["p_down_raw"] == pytest.approx(0.2)
    assert out.iloc[0]["p_eff_up"] == pytest.approx(0.65)
    assert out.iloc[0]["p_eff_down"] == pytest.approx(0.19)
    assert out.iloc[0]["p_up"] == pytest.approx(0.65)
    assert out.iloc[0]["p_down"] == pytest.approx(0.19)
    assert out.iloc[0]["probability_mode"] == "conservative_reliability_bin"


def test_load_offset_model_context_falls_back_to_models_when_diagnostics_missing(tmp_path) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)

    (offset_dir / "bundle_config.json").write_text(
        json.dumps(
            {
                "feature_columns": ["move_z", "ret_5m"],
                "signal_target": "direction",
                "allowed_blacklist_columns": [],
                "missing_feature_fill_value": 0.0,
            }
        ),
        encoding="utf-8",
    )
    joblib.dump(FakeCalibratedModel(FakeLgbModel()), offset_dir / "models" / "lgbm_sigmoid.joblib")
    joblib.dump(FakeCalibratedModel(FakeLogregPipeline()), offset_dir / "models" / "logreg_sigmoid.joblib")

    context = load_offset_model_context(offset_dir)

    assert context is not None
    assert context["top_logreg_coefficients"][0]["feature"] == "move_z"
    assert context["top_lgb_feature_importance"][0]["feature"] == "move_z"
    assert context["top_positive_factors"][0]["feature"] == "move_z"
    assert context["top_negative_factors"][0]["feature"] == "ret_5m"


def test_score_bundle_offset_reuses_cached_runtime_for_repeated_calls(tmp_path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)
    (offset_dir / "diagnostics").mkdir(parents=True, exist_ok=True)

    (offset_dir / "bundle_config.json").write_text(
        json.dumps(
            {
                "feature_columns": ["ret_1m"],
                "signal_target": "direction",
                "allowed_blacklist_columns": [],
                "missing_feature_fill_value": 0.0,
            }
        ),
        encoding="utf-8",
    )
    joblib.dump(FixedProbabilityModel(0.90), offset_dir / "models" / "lgbm_sigmoid.joblib")
    joblib.dump(FixedProbabilityModel(0.70), offset_dir / "models" / "logreg_sigmoid.joblib")
    (offset_dir / "calibration" / "blend_weights.json").write_text(
        json.dumps({"w_lgb": 0.5, "w_lr": 0.5}),
        encoding="utf-8",
    )
    (offset_dir / "diagnostics" / "logreg_coefficients.json").write_text(
        json.dumps({"rows": [{"feature": "ret_1m", "coefficient": 0.2, "abs_coefficient": 0.2, "direction": "positive", "rank": 1}]}),
        encoding="utf-8",
    )
    (offset_dir / "diagnostics" / "lgb_feature_importance.json").write_text(
        json.dumps({"rows": [{"feature": "ret_1m", "gain_importance": 1.0, "gain_share": 1.0, "split_importance": 1, "split_share": 1.0, "rank": 1}]}),
        encoding="utf-8",
    )
    (offset_dir / "diagnostics" / "factor_direction_summary.json").write_text(
        json.dumps(
            {
                "top_positive_factors": [
                    {
                        "feature": "ret_1m",
                        "direction_score": 0.2,
                        "direction": "positive",
                        "logreg_coefficient": 0.2,
                        "lgb_gain_importance": 1.0,
                        "rank": 1,
                    }
                ],
                "top_negative_factors": [],
            }
        ),
        encoding="utf-8",
    )

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "cycle_start_ts": "2026-03-20T00:00:00+00:00",
                "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                "offset": 7,
                "ret_1m": 0.01,
            }
        ]
    )

    load_count = {"value": 0}
    original_joblib_load = scorer_module.joblib.load

    def _counting_joblib_load(path):
        load_count["value"] += 1
        return original_joblib_load(path)

    monkeypatch.setattr(scorer_module.joblib, "load", _counting_joblib_load)

    first = score_bundle_offset(bundle_dir, features, offset=7)
    second = score_bundle_offset(bundle_dir, features, offset=7)

    assert load_count["value"] == 2
    assert float(first.iloc[0]["p_signal"]) == pytest.approx(0.8)
    assert float(second.iloc[0]["p_signal"]) == pytest.approx(0.8)


def test_score_bundle_offset_skips_model_context_loading_by_default(tmp_path, monkeypatch) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    (offset_dir / "models").mkdir(parents=True, exist_ok=True)
    (offset_dir / "calibration").mkdir(parents=True, exist_ok=True)

    (offset_dir / "bundle_config.json").write_text(
        json.dumps(
            {
                "feature_columns": ["ret_1m"],
                "signal_target": "direction",
                "allowed_blacklist_columns": [],
                "missing_feature_fill_value": 0.0,
            }
        ),
        encoding="utf-8",
    )
    joblib.dump(FixedProbabilityModel(0.90), offset_dir / "models" / "lgbm_sigmoid.joblib")
    joblib.dump(FixedProbabilityModel(0.70), offset_dir / "models" / "logreg_sigmoid.joblib")
    (offset_dir / "calibration" / "blend_weights.json").write_text(
        json.dumps({"w_lgb": 0.5, "w_lr": 0.5}),
        encoding="utf-8",
    )

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "cycle_start_ts": "2026-03-20T00:00:00+00:00",
                "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                "offset": 7,
                "ret_1m": 0.01,
            }
        ]
    )

    def _boom(*args, **kwargs):
        raise AssertionError("model_context should not be loaded for backtest scoring by default")

    monkeypatch.setattr(scorer_module, "load_offset_model_context", _boom)

    out = score_bundle_offset(bundle_dir, features, offset=7)

    assert len(out) == 1
    assert out.iloc[0]["model_context"] is None


def test_load_offset_model_context_refreshes_when_diagnostics_change(tmp_path) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    diagnostics_dir = offset_dir / "diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    diagnostics_path = diagnostics_dir / "logreg_coefficients.json"
    diagnostics_path.write_text(
        json.dumps(
            {
                "rows": [
                    {"feature": "move_z", "coefficient": 0.4, "abs_coefficient": 0.4, "direction": "positive", "rank": 1},
                    {"feature": "ret_5m", "coefficient": -0.3, "abs_coefficient": 0.3, "direction": "negative", "rank": 2},
                ]
            }
        ),
        encoding="utf-8",
    )

    first = load_offset_model_context(offset_dir)
    assert first is not None
    assert first["top_logreg_coefficients"][0]["feature"] == "move_z"

    diagnostics_path.write_text(
        json.dumps(
            {
                "rows": [
                    {"feature": "ret_5m", "coefficient": 0.9, "abs_coefficient": 0.9, "direction": "positive", "rank": 1},
                    {"feature": "move_z", "coefficient": -0.1, "abs_coefficient": 0.1, "direction": "negative", "rank": 2},
                ]
            }
        ),
        encoding="utf-8",
    )
    future_ns = time.time_ns() + 1_000_000
    os.utime(diagnostics_path, ns=(future_ns, future_ns))

    second = load_offset_model_context(offset_dir)
    assert second is not None
    assert second["top_logreg_coefficients"][0]["feature"] == "ret_5m"


def test_score_bundle_offset_refreshes_cached_models_when_model_file_changes(tmp_path) -> None:
    bundle_dir = tmp_path / "bundle=test"
    offset_dir = bundle_dir / "offsets" / "offset=7"
    model_dir = offset_dir / "models"
    calibration_dir = offset_dir / "calibration"
    diagnostics_dir = offset_dir / "diagnostics"
    model_dir.mkdir(parents=True, exist_ok=True)
    calibration_dir.mkdir(parents=True, exist_ok=True)
    diagnostics_dir.mkdir(parents=True, exist_ok=True)

    (offset_dir / "bundle_config.json").write_text(
        json.dumps(
            {
                "feature_columns": ["ret_1m"],
                "signal_target": "direction",
                "allowed_blacklist_columns": [],
                "missing_feature_fill_value": 0.0,
            }
        ),
        encoding="utf-8",
    )
    lgb_path = model_dir / "lgbm_sigmoid.joblib"
    lr_path = model_dir / "logreg_sigmoid.joblib"
    joblib.dump(FixedProbabilityModel(0.90), lgb_path)
    joblib.dump(FixedProbabilityModel(0.70), lr_path)
    (calibration_dir / "blend_weights.json").write_text(
        json.dumps({"w_lgb": 0.5, "w_lr": 0.5}),
        encoding="utf-8",
    )
    (diagnostics_dir / "logreg_coefficients.json").write_text(
        json.dumps({"rows": [{"feature": "ret_1m", "coefficient": 0.2, "abs_coefficient": 0.2, "direction": "positive", "rank": 1}]}),
        encoding="utf-8",
    )

    features = pd.DataFrame(
        [
            {
                "decision_ts": "2026-03-20T00:08:00+00:00",
                "cycle_start_ts": "2026-03-20T00:00:00+00:00",
                "cycle_end_ts": "2026-03-20T00:15:00+00:00",
                "offset": 7,
                "ret_1m": 0.01,
            }
        ]
    )

    first = score_bundle_offset(bundle_dir, features, offset=7)
    assert float(first.iloc[0]["p_lr"]) == pytest.approx(0.7)
    assert float(first.iloc[0]["p_signal"]) == pytest.approx(0.8)

    joblib.dump(FixedProbabilityModel(0.10), lr_path)
    future_ns = time.time_ns() + 1_000_000
    os.utime(lr_path, ns=(future_ns, future_ns))

    second = score_bundle_offset(bundle_dir, features, offset=7)
    assert float(second.iloc[0]["p_lr"]) == pytest.approx(0.1)
    assert float(second.iloc[0]["p_signal"]) == pytest.approx(0.5)
