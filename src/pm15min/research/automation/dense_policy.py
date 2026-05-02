from __future__ import annotations

from typing import Any, Mapping

_DENSE_GATE_RANK = {
    "reject_sparse": 0,
    "subtarget": 1,
    "on_target": 2,
}

_DENSE_HISTORY_REWORK_STREAK = 3
_DENSE_SPARSE_TRADE_FLOOR = 56


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


def classify_density_bottleneck(
    *,
    total_rows: int,
    trade_rows: int,
    profitable_pool_rows: int,
    profitable_pool_capture_rows: int,
    profitable_pool_correct_side_rows: int,
    reject_reason_counts: Mapping[str, Any] | None = None,
    quote_missing_rows: int = 0,
) -> dict[str, Any]:
    rejects = {
        str(key): int(value or 0)
        for key, value in dict(reject_reason_counts or {}).items()
        if int(value or 0) > 0
    }
    reject_rows = int(sum(rejects.values()))
    dominant_reject_reason = ""
    dominant_reject_rows = 0
    if rejects:
        dominant_reject_reason, dominant_reject_rows = max(
            rejects.items(),
            key=lambda item: (int(item[1]), str(item[0])),
        )

    total = max(0, int(total_rows or 0))
    trades = max(0, int(trade_rows or 0))
    pool_rows = max(0, int(profitable_pool_rows or 0))
    capture_rows = max(0, int(profitable_pool_capture_rows or 0))
    correct_side_rows = max(0, int(profitable_pool_correct_side_rows or 0))
    quote_missing = max(0, int(quote_missing_rows or 0))
    sparse_density = trades < _DENSE_SPARSE_TRADE_FLOOR

    probability_rejects = int(rejects.get("direction_prob", 0))
    entry_price_rejects = int(rejects.get("entry_price_max", 0)) + int(rejects.get("entry_price_min", 0))
    quote_missing_heavy = total > 0 and quote_missing / float(total) >= 0.35

    if quote_missing_heavy and quote_missing >= max(probability_rejects, entry_price_rejects):
        primary = "quote_coverage_gap"
        route = "data_or_orderbook_repair"
    elif entry_price_rejects > probability_rejects and entry_price_rejects >= max(1, reject_rows // 3):
        primary = "entry_price_gate"
        route = "entry_band_relaxation"
    elif probability_rejects >= entry_price_rejects and probability_rejects >= max(1, reject_rows // 3):
        primary = "probability_gate"
        route = "model_or_calibration_rework"
    elif correct_side_rows > capture_rows:
        primary = "conversion_gap"
        route = "weight_or_threshold_release"
    elif sparse_density:
        primary = "low_trade_density"
        route = "feature_width_or_family_rework"
    else:
        primary = "balanced_or_mixed"
        route = "inspect_frontier"

    return {
        "primary_bottleneck": primary,
        "recommended_route": route,
        "sparse_density": bool(sparse_density),
        "dominant_reject_reason": dominant_reject_reason,
        "dominant_reject_rows": int(dominant_reject_rows),
        "reject_rows": int(reject_rows),
        "quote_missing_rows": int(quote_missing),
        "trade_rows": int(trades),
        "profitable_pool_rows": int(pool_rows),
        "profitable_pool_capture_rows": int(capture_rows),
        "profitable_pool_correct_side_rows": int(correct_side_rows),
    }


def choose_density_research_route(
    *,
    completed_sparse_streak: int,
    same_width_streak: int,
    same_model_family_streak: int,
    latest_width: int | None,
    density_bottleneck: Mapping[str, Any] | None = None,
) -> str:
    bottleneck = dict(density_bottleneck or {})
    sparse = bool(bottleneck.get("sparse_density"))
    sparse_streak = max(0, int(completed_sparse_streak or 0))
    width_streak = max(0, int(same_width_streak or 0))
    model_streak = max(0, int(same_model_family_streak or 0))
    width = int(latest_width or 0)
    recommended = str(bottleneck.get("recommended_route") or "").strip()

    if sparse_streak < _DENSE_HISTORY_REWORK_STREAK and not sparse:
        return recommended or "continue_incremental"

    if (
        recommended == "model_or_calibration_rework"
        and model_streak >= _DENSE_HISTORY_REWORK_STREAK
    ):
        return "model_or_ensemble_required"

    if width_streak >= _DENSE_HISTORY_REWORK_STREAK and width < 56:
        return "feature_width_change_required"

    if recommended in {
        "entry_band_relaxation",
        "weight_or_threshold_release",
        "model_or_calibration_rework",
        "feature_width_or_family_rework",
        "data_or_orderbook_repair",
    }:
        return recommended

    if sparse_streak >= _DENSE_HISTORY_REWORK_STREAK:
        return "feature_width_or_family_rework"
    return "continue_incremental"


def classify_dense_history_route(
    *,
    no_capture_streak: int,
    best_quick_trade_rows: int | None,
    best_quick_correct_side_rows: int | None,
    best_quick_capture_rows: int | None,
) -> str:
    if int(no_capture_streak) < _DENSE_HISTORY_REWORK_STREAK:
        return "continue_incremental"

    correct_side_rows = max(0, int(best_quick_correct_side_rows or 0))
    capture_rows = max(0, int(best_quick_capture_rows or 0))
    trade_rows = max(0, int(best_quick_trade_rows or 0))

    if correct_side_rows <= 0:
        return "factor_rework_first"
    if capture_rows <= 0 and trade_rows <= 0:
        return "factor_rework_first"
    if capture_rows < correct_side_rows:
        return "weight_search_first"
    return "continue_incremental"
