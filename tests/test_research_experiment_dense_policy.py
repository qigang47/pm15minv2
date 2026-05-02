from __future__ import annotations

from pm15min.research.automation import classify_dense_gate as exported_classify_dense_gate
from pm15min.research.automation import prefer_dense_screen_candidate as exported_prefer_dense_screen_candidate
from pm15min.research.automation import prefer_dense_candidate as exported_prefer_dense_candidate
from pm15min.research.automation import classify_dense_history_route as exported_classify_dense_history_route
from pm15min.research.automation import classify_density_bottleneck as exported_classify_density_bottleneck
from pm15min.research.automation import choose_density_research_route as exported_choose_density_research_route
from pm15min.research.automation.dense_policy import (
    choose_density_research_route,
    classify_density_bottleneck,
    classify_dense_gate,
    classify_dense_history_route,
    prefer_dense_candidate,
    prefer_dense_screen_candidate,
)


def test_classify_dense_gate_marks_sparse_subtarget_and_on_target() -> None:
    assert classify_dense_gate(total_trades=40, window_days=14) == "reject_sparse"
    assert classify_dense_gate(total_trades=56, window_days=14) == "subtarget"
    assert classify_dense_gate(total_trades=139, window_days=14) == "subtarget"
    assert classify_dense_gate(total_trades=140, window_days=14) == "on_target"


def test_prefer_dense_candidate_prefers_better_dense_gate_before_roi_pct() -> None:
    sparse = {"roi_pct": 220.0, "trades": 3, "dense_gate": "reject_sparse"}
    thick = {"roi_pct": 90.0, "trades": 160, "dense_gate": "on_target"}

    assert prefer_dense_candidate(thick, sparse) is thick


def test_prefer_dense_candidate_prefers_more_trades_when_dense_gate_matches() -> None:
    left = {"roi_pct": 12.0, "trades": 120, "dense_gate": "subtarget"}
    right = {"roi_pct": 40.0, "trades": 80, "dense_gate": "subtarget"}

    assert prefer_dense_candidate(left, right) is left


def test_prefer_dense_candidate_prefers_higher_roi_pct_when_gate_and_trades_tie() -> None:
    left = {"roi_pct": 12.0, "trades": 140, "dense_gate": "on_target"}
    right = {"roi_pct": 18.0, "trades": 140, "dense_gate": "on_target"}

    assert prefer_dense_candidate(left, right) is right


def test_prefer_dense_screen_candidate_prefers_coverage_before_trade_count() -> None:
    lower_coverage = {
        "profitable_pool_coverage_ratio": 0.58,
        "profitable_pool_capture_rows": 58,
        "profitable_pool_correct_side_rows": 70,
        "trade_rows": 90,
    }
    higher_coverage = {
        "profitable_pool_coverage_ratio": 0.71,
        "profitable_pool_capture_rows": 57,
        "profitable_pool_correct_side_rows": 66,
        "trade_rows": 72,
    }

    assert prefer_dense_screen_candidate(higher_coverage, lower_coverage) is higher_coverage


def test_classify_dense_history_route_prefers_weight_search_when_correct_side_exists() -> None:
    route = classify_dense_history_route(
        no_capture_streak=3,
        best_quick_trade_rows=8,
        best_quick_correct_side_rows=10,
        best_quick_capture_rows=7,
    )

    assert route == "weight_search_first"


def test_classify_dense_history_route_prefers_factor_rework_when_no_correct_side_exists() -> None:
    route = classify_dense_history_route(
        no_capture_streak=3,
        best_quick_trade_rows=0,
        best_quick_correct_side_rows=0,
        best_quick_capture_rows=0,
    )

    assert route == "factor_rework_first"


def test_classify_dense_history_route_keeps_incremental_mode_before_streak_threshold() -> None:
    route = classify_dense_history_route(
        no_capture_streak=2,
        best_quick_trade_rows=8,
        best_quick_correct_side_rows=10,
        best_quick_capture_rows=7,
    )

    assert route == "continue_incremental"


def test_classify_density_bottleneck_identifies_probability_gate() -> None:
    bottleneck = classify_density_bottleneck(
        total_rows=6564,
        trade_rows=8,
        profitable_pool_rows=373,
        profitable_pool_capture_rows=2,
        profitable_pool_correct_side_rows=8,
        reject_reason_counts={"direction_prob": 4248, "entry_price_max": 2299},
        quote_missing_rows=1416,
    )

    assert bottleneck["primary_bottleneck"] == "probability_gate"
    assert bottleneck["recommended_route"] == "model_or_calibration_rework"
    assert bottleneck["sparse_density"] is True


def test_classify_density_bottleneck_identifies_entry_price_gate() -> None:
    bottleneck = classify_density_bottleneck(
        total_rows=6564,
        trade_rows=12,
        profitable_pool_rows=373,
        profitable_pool_capture_rows=3,
        profitable_pool_correct_side_rows=39,
        reject_reason_counts={"direction_prob": 900, "entry_price_max": 3100},
        quote_missing_rows=100,
    )

    assert bottleneck["primary_bottleneck"] == "entry_price_gate"
    assert bottleneck["recommended_route"] == "entry_band_relaxation"


def test_choose_density_research_route_forces_width_or_model_after_stall() -> None:
    route = choose_density_research_route(
        completed_sparse_streak=4,
        same_width_streak=3,
        same_model_family_streak=4,
        latest_width=56,
        density_bottleneck={
            "primary_bottleneck": "probability_gate",
            "recommended_route": "model_or_calibration_rework",
            "sparse_density": True,
        },
    )

    assert route == "model_or_ensemble_required"


def test_choose_density_research_route_prefers_width_change_before_same_width_retry() -> None:
    route = choose_density_research_route(
        completed_sparse_streak=3,
        same_width_streak=3,
        same_model_family_streak=1,
        latest_width=40,
        density_bottleneck={
            "primary_bottleneck": "low_trade_density",
            "recommended_route": "feature_width_or_family_rework",
            "sparse_density": True,
        },
    )

    assert route == "feature_width_change_required"


def test_dense_policy_helpers_are_exported_from_package() -> None:
    assert exported_classify_dense_gate is classify_dense_gate
    assert exported_classify_dense_history_route is classify_dense_history_route
    assert exported_classify_density_bottleneck is classify_density_bottleneck
    assert exported_choose_density_research_route is choose_density_research_route
    assert exported_prefer_dense_candidate is prefer_dense_candidate
    assert exported_prefer_dense_screen_candidate is prefer_dense_screen_candidate
