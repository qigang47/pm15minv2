from __future__ import annotations

from pm15min.research.automation import classify_dense_gate as exported_classify_dense_gate
from pm15min.research.automation import prefer_dense_screen_candidate as exported_prefer_dense_screen_candidate
from pm15min.research.automation import prefer_dense_candidate as exported_prefer_dense_candidate
from pm15min.research.automation.dense_policy import (
    classify_dense_gate,
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


def test_dense_policy_helpers_are_exported_from_package() -> None:
    assert exported_classify_dense_gate is classify_dense_gate
    assert exported_prefer_dense_candidate is prefer_dense_candidate
    assert exported_prefer_dense_screen_candidate is prefer_dense_screen_candidate
