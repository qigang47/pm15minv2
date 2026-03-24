from __future__ import annotations

from typing import Any

import pandas as pd

from .policy_raw import evaluate_liquidity_raw as _evaluate_liquidity_raw_impl
from .policy_temporal import (
    apply_temporal_filter as _apply_temporal_filter_impl,
    can_reuse_previous as _can_reuse_previous_impl,
)
from .policy_thresholds import liquidity_thresholds as _liquidity_thresholds_impl


def can_reuse_previous(*, previous_payload: dict[str, Any] | None, now: pd.Timestamp, refresh_seconds: float) -> bool:
    return _can_reuse_previous_impl(previous_payload=previous_payload, now=now, refresh_seconds=refresh_seconds)


def liquidity_thresholds(*, spec, market: str) -> dict[str, float]:
    return _liquidity_thresholds_impl(spec=spec, market=market)


def evaluate_liquidity_raw(
    *,
    symbol: str,
    now: pd.Timestamp,
    thresholds: dict[str, float],
    lookback_minutes: int,
    baseline_minutes: int,
    soft_fail_min_count: int,
    hard_spread_multiplier: float,
    hard_basis_multiplier: float,
    spot_base_url: str,
    perp_base_url: str,
    fetch_klines_fn,
    fetch_book_ticker_fn,
    fetch_open_interest_fn,
    compute_ratio_fn,
    window_sum_fn,
    spread_bps_fn,
) -> dict[str, Any]:
    return _evaluate_liquidity_raw_impl(
        symbol=symbol,
        now=now,
        thresholds=thresholds,
        lookback_minutes=lookback_minutes,
        baseline_minutes=baseline_minutes,
        soft_fail_min_count=soft_fail_min_count,
        hard_spread_multiplier=hard_spread_multiplier,
        hard_basis_multiplier=hard_basis_multiplier,
        spot_base_url=spot_base_url,
        perp_base_url=perp_base_url,
        fetch_klines_fn=fetch_klines_fn,
        fetch_book_ticker_fn=fetch_book_ticker_fn,
        fetch_open_interest_fn=fetch_open_interest_fn,
        compute_ratio_fn=compute_ratio_fn,
        window_sum_fn=window_sum_fn,
        spread_bps_fn=spread_bps_fn,
    )


def apply_temporal_filter(
    *,
    raw_result: dict[str, Any],
    previous_payload: dict[str, Any] | None,
    min_failed_checks: int,
    min_recovered_checks: int,
    block_on_degrade: bool,
) -> dict[str, Any]:
    return _apply_temporal_filter_impl(
        raw_result=raw_result,
        previous_payload=previous_payload,
        min_failed_checks=min_failed_checks,
        min_recovered_checks=min_recovered_checks,
        block_on_degrade=block_on_degrade,
    )
