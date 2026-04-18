from __future__ import annotations

from typing import Any, Mapping

_DENSE_GATE_RANK = {
    "reject_sparse": 0,
    "subtarget": 1,
    "on_target": 2,
}


def classify_dense_gate(*, total_trades: int, window_days: int) -> str:
    _ = window_days
    if int(total_trades) < 56:
        return "reject_sparse"
    if int(total_trades) < 140:
        return "subtarget"
    return "on_target"


def prefer_dense_candidate(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> Mapping[str, Any]:
    left_rank = _DENSE_GATE_RANK.get(str(left.get("dense_gate") or "").strip(), -1)
    right_rank = _DENSE_GATE_RANK.get(str(right.get("dense_gate") or "").strip(), -1)
    if left_rank != right_rank:
        return left if left_rank > right_rank else right

    left_trades = int(left.get("trades") or 0)
    right_trades = int(right.get("trades") or 0)
    if left_trades != right_trades:
        return left if left_trades > right_trades else right

    left_roi = float(left.get("roi_pct") or 0.0)
    right_roi = float(right.get("roi_pct") or 0.0)
    return left if left_roi >= right_roi else right


def prefer_dense_screen_candidate(
    left: Mapping[str, Any],
    right: Mapping[str, Any],
) -> Mapping[str, Any]:
    left_coverage = float(left.get("profitable_pool_coverage_ratio") or 0.0)
    right_coverage = float(right.get("profitable_pool_coverage_ratio") or 0.0)
    if left_coverage != right_coverage:
        return left if left_coverage > right_coverage else right

    left_capture = int(left.get("profitable_pool_capture_rows") or 0)
    right_capture = int(right.get("profitable_pool_capture_rows") or 0)
    if left_capture != right_capture:
        return left if left_capture > right_capture else right

    left_correct_side = int(left.get("profitable_pool_correct_side_rows") or 0)
    right_correct_side = int(right.get("profitable_pool_correct_side_rows") or 0)
    if left_correct_side != right_correct_side:
        return left if left_correct_side > right_correct_side else right

    left_trades = int(left.get("trade_rows") or 0)
    right_trades = int(right.get("trade_rows") or 0)
    return left if left_trades >= right_trades else right
