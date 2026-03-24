from __future__ import annotations

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
from ..profiles import LiveProfileSpec


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
    if int(coverage.get("not_allowed_blacklist_count") or 0) > 0:
        reasons.append("blacklist_not_allowed_by_bundle")

    confidence = float(signal_row.get("confidence") or 0.0)
    threshold = float(profile_spec.threshold_for(market=market, offset=offset))
    if confidence < threshold:
        reasons.append("confidence_below_threshold")
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
