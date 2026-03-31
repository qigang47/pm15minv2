from __future__ import annotations

import os
from typing import Any

from .features import (
    directional_return_guard_reasons,
    tail_space_guard_reasons,
)
from .account import (
    cash_balance_guard_reasons,
    max_open_markets_guard_reasons,
)
from .quote import quote_guard_reasons
from .regime import (
    build_account_context,
    liquidity_guard_reasons,
    regime_guard_reasons,
    trade_count_cap_reasons,
)
from ..execution.utils import float_or_none, resolve_probability_interval_view
from ..profiles import LiveProfileSpec
from ..session_state import normalize_trade_side


def evaluate_signal_guard_reasons(
    *,
    market: str,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
    quote_row: dict[str, Any] | None = None,
    liquidity_state: dict[str, Any] | None = None,
    regime_state: dict[str, Any] | None = None,
    account_state: dict[str, Any] | None = None,
    session_state: dict[str, Any] | None = None,
    quote_metrics_override: dict[str, Any] | None = None,
) -> tuple[list[str], dict[str, Any], dict[str, Any]]:
    reasons: list[str] = []
    offset = int(signal_row["offset"])
    if str(market).lower() not in set(profile_spec.active_markets):
        reasons.append("market_not_active_for_profile")
    if offset not in set(profile_spec.offsets):
        reasons.append("offset_not_enabled_for_profile")

    coverage = signal_row.get("coverage") or {}
    if signal_row.get("status"):
        reasons.append(str(signal_row["status"]))
    if not bool(signal_row.get("score_valid", False)):
        reasons.append(str(signal_row.get("score_reason") or "score_invalid"))
    if int(coverage.get("effective_missing_feature_count") or 0) > 0:
        reasons.append("effective_missing_features")
    threshold = float(profile_spec.threshold_for(market=market, offset=offset))
    confidence = float_or_none(signal_row.get("confidence")) or 0.0
    reasons.extend(
        probability_guard_reasons(
            market=market,
            profile_spec=profile_spec,
            signal_row=signal_row,
        )
    )
    reasons.extend(
        configured_trade_side_guard_reasons(
            signal_row=signal_row,
        )
    )
    reasons.extend(
        liquidity_guard_reasons(
            profile_spec=profile_spec,
            liquidity_state=liquidity_state,
        )
    )
    reasons.extend(
        regime_guard_reasons(
            profile_spec=profile_spec,
            signal_row=signal_row,
            regime_state=regime_state,
            base_threshold=threshold,
            chosen_prob=confidence,
        )
    )
    account_context = build_account_context(
        market=market,
        profile_spec=profile_spec,
        signal_row=signal_row,
        quote_row=quote_row,
        regime_state=regime_state,
        account_state=account_state,
        session_state=session_state,
    )
    reasons.extend(
        trade_count_cap_reasons(
            profile_spec=profile_spec,
            regime_state=regime_state,
            account_context=account_context,
        )
    )
    reasons.extend(
        cash_balance_guard_reasons(
            profile_spec=profile_spec,
            account_context=account_context,
        )
    )
    reasons.extend(
        max_open_markets_guard_reasons(
            profile_spec=profile_spec,
            account_context=account_context,
        )
    )

    feature_snapshot = signal_row.get("feature_snapshot") or {}
    reasons.extend(
        directional_return_guard_reasons(
            market=market,
            profile_spec=profile_spec,
            signal_row=signal_row,
            feature_snapshot=feature_snapshot,
        )
    )
    reasons.extend(
        tail_space_guard_reasons(
            profile_spec=profile_spec,
            signal_row=signal_row,
            feature_snapshot=feature_snapshot,
        )
    )
    quote_reasons, quote_metrics = quote_guard_reasons(
        profile_spec=profile_spec,
        signal_row=signal_row,
        quote_row=quote_row,
        metrics_override=quote_metrics_override,
    )
    reasons.extend(quote_reasons)
    return reasons, quote_metrics, account_context


def probability_guard_reasons(
    *,
    market: str,
    profile_spec: LiveProfileSpec,
    signal_row: dict[str, Any],
) -> list[str]:
    offset = int(signal_row["offset"])
    threshold = float(profile_spec.threshold_for(market=market, offset=offset))
    upper_threshold = max(0.0, 1.0 - float(threshold))
    selected_side = normalize_trade_side(signal_row.get("recommended_side"))
    view = resolve_probability_interval_view(selected_row=signal_row)
    if view is None:
        confidence = float_or_none(signal_row.get("confidence"))
        if confidence is None:
            return ["confidence_missing"]
        return ["confidence_below_threshold"] if float(confidence) <= float(threshold) else []

    p_up_raw = float(view["p_up_raw"])
    p_up_lcb = float(view["p_up_lcb"])
    p_up_ucb = float(view["p_up_ucb"])
    if selected_side == "UP":
        reasons: list[str] = []
        if p_up_raw <= 0.5:
            reasons.append("up_raw_not_above_midpoint")
        if p_up_lcb <= float(threshold):
            reasons.append("up_lcb_below_threshold")
        return reasons
    if selected_side == "DOWN":
        reasons = []
        if p_up_raw >= 0.5:
            reasons.append("down_raw_not_below_midpoint")
        if p_up_ucb >= float(upper_threshold):
            reasons.append("up_ucb_above_threshold")
        return reasons
    return ["recommended_side_missing"]


def configured_trade_side_guard_reasons(*, signal_row: dict[str, Any]) -> list[str]:
    raw = os.getenv("PM15MIN_ALLOWED_TRADE_SIDES")
    if raw in (None, ""):
        return []
    allowed = {
        side
        for token in str(raw).split(",")
        for side in [normalize_trade_side(token)]
        if side is not None
    }
    if not allowed:
        return []
    selected_side = normalize_trade_side(signal_row.get("recommended_side"))
    if selected_side is None or selected_side in allowed:
        return []
    return ["trade_side_blocked"]
