from __future__ import annotations

from pm15min.research.features.pruning import build_feature_pruning_plan


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
