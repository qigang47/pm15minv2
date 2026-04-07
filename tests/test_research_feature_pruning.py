from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pm15min.research.features.pruning import build_feature_pruning_plan
from pm15min.research.training.trainers import TrainerConfig, prepare_training_matrix


def test_build_feature_pruning_plan_applies_shared_and_feature_set_blacklists() -> None:
    plan = build_feature_pruning_plan(
        [
            "ret_1m",
            "ma_gap_15",
            "ema_gap_12",
            "vwap_gap_20",
            "has_cl_strike",
            "extra_feature",
        ],
        feature_set="deep_otm_v1",
        market="btc",
        extra_drop_columns=("extra_feature", "missing_col"),
        apply_shared_blacklist=True,
    )
    assert plan.dropped_columns == ("extra_feature", "ma_gap_15", "ema_gap_12", "vwap_gap_20", "has_cl_strike")
    assert "missing_col" in plan.missing_columns
    assert plan.kept_columns == ("ret_1m",)


def test_prepare_training_matrix_uses_custom_feature_set_root(tmp_path: Path) -> None:
    experiments_root = tmp_path / "research" / "experiments"
    experiments_root.mkdir(parents=True, exist_ok=True)
    (experiments_root / "custom_feature_sets.json").write_text(
        json.dumps(
            {
                "focus_btc_2_v1": {
                    "columns": ["ret_1m", "bb_pos_20"],
                }
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    frame = pd.DataFrame(
        {
            "ret_1m": [0.1, 0.2],
            "bb_pos_20": [0.4, 0.5],
            "extra_feature": [9.0, 8.0],
            "y": [1, 0],
        }
    )

    X, y, plan = prepare_training_matrix(
        frame,
        feature_set="focus_btc_2_v1",
        market="btc",
        cfg=TrainerConfig(feature_set_root=tmp_path),
    )

    assert list(X.columns) == ["ret_1m", "bb_pos_20"]
    assert y.tolist() == [1, 0]
    assert plan.kept_columns == ("ret_1m", "bb_pos_20")
