from __future__ import annotations

import json

import joblib
import numpy as np
import pandas as pd
import pytest

from pm15min.research.inference.scorer import score_bundle_offset


class FixedProbabilityModel:
    def __init__(self, probability: float) -> None:
        self.probability = float(probability)

    def predict_proba(self, X):
        return np.asarray([[1.0 - self.probability, self.probability] for _ in range(len(X))], dtype=float)


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
    assert out.iloc[0]["p_up_raw"] == pytest.approx(0.8)
    assert out.iloc[0]["p_down_raw"] == pytest.approx(0.2)
    assert out.iloc[0]["p_eff_up"] == pytest.approx(0.65)
    assert out.iloc[0]["p_eff_down"] == pytest.approx(0.19)
    assert out.iloc[0]["p_up"] == pytest.approx(0.65)
    assert out.iloc[0]["p_down"] == pytest.approx(0.19)
    assert out.iloc[0]["probability_mode"] == "conservative_reliability_bin"
